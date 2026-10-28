"""Unit proof for the shared ``assert_restored`` gate and the Codex append hook.

Requirement under test: the coordinator may only accept a Codex store whose
journal kept the archive bytes as an exact prefix and whose suffix is a bounded
JSONL appendix consisting solely of ``event_msg``/``thread_settings_applied``
resume events. Identity stays exact, non-journal records stay byte-identical,
and any other history, metadata drift, malformed or truncated/oversized suffix
must refuse. An adapter without the read-only hook still compares records
exactly. No process is spawned and no real store is touched.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from untether.session_migration.codex_rollout_adapter import (
    _MAX_APPENDIX_BYTES,
    CodexRolloutPersistenceAdapter,
    _journal_appendix_ok,
)
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    assert_restored,
)
from untether.session_migration.native_archive import NativeArchive
from untether.session_migration.reclaim import _preoverwrite_matches

_ENGINE = "codex"
_SID = "01a17ff7-ee74-7ef1-b6fd-db188d65de55"
_REL = "2026/10/27/rollout-append.jsonl"
_LINEAGE = "codex-rollout-v1:" + _REL


def _rollout_bytes(extra: list[dict] | None = None) -> bytes:
    lines = [
        {
            "ordinal": 0,
            "type": "session_meta",
            "payload": {
                "session_id": _SID,
                "id": _SID,
                "cwd": "/work/demo",
                "model_provider": "openai",
                "base_instructions": {"provenance": {"model": "gpt-5.1-codex"}},
            },
        },
        {"ordinal": 1, "type": "event_msg", "payload": {"type": "task_started"}},
    ]
    lines.extend(extra or [])
    return "".join(json.dumps(line) + "\n" for line in lines).encode()


def _settings_line(ordinal: int) -> bytes:
    return (
        json.dumps(
            {
                "ordinal": ordinal,
                "type": "event_msg",
                "payload": {"type": "thread_settings_applied", "thread_settings": {}},
            }
        ).encode()
        + b"\n"
    )


def _archive(journal: bytes, generation: int = 0) -> NativeArchive:
    return NativeArchive(
        engine=_ENGINE,
        logical_id=_SID,
        native_id=_SID,
        generation=generation,
        lineage=_LINEAGE,
        records={
            "session.jsonl": journal,
            "untether.json": json.dumps(
                {
                    "logical_id": _SID,
                    "native_id": _SID,
                    "generation": generation,
                    "lineage": _LINEAGE,
                }
            ).encode(),
        },
        digest="0" * 64,
    )


def _stored(archive: NativeArchive, journal: bytes) -> NativeArchive:
    return replace(archive, records={**archive.records, "session.jsonl": journal})


def _store(tmp_path: Path, name: str = "store") -> Path:
    root = tmp_path / name
    root.mkdir()
    return Path(os.path.realpath(root))


def _adapter(root: Path) -> CodexRolloutPersistenceAdapter:
    return CodexRolloutPersistenceAdapter(
        engine=_ENGINE,
        sessions_root=root,
        app_server_executable=Path("C:/fake/codex.exe"),
        rpc_resume=lambda native_id, journal_path, timeout_s: None,
    )


class _HooklessAdapter:
    """No read-only append hook: the shared gate must compare records exactly."""

    engine = _ENGINE


# -- the shared gate and the hook agree on append-only resumes --------------


def test_multiple_settings_appends_are_allowed(tmp_path):
    archive = _archive(_rollout_bytes())
    journal = (
        _rollout_bytes() + _settings_line(9) + _settings_line(10) + _settings_line(11)
    )
    adapter = _adapter(_store(tmp_path))
    assert CodexRolloutPersistenceAdapter.verify_restored_store(
        archive, _stored(archive, journal)
    )
    assert_restored(adapter, archive, _stored(archive, journal))
    assert _journal_appendix_ok(journal, _rollout_bytes())


def test_no_appendix_is_still_allowed(tmp_path):
    archive = _archive(_rollout_bytes())
    adapter = _adapter(_store(tmp_path))
    assert_restored(adapter, archive, _stored(archive, _rollout_bytes()))


def test_default_adapter_without_hook_stays_exact():
    archive = _archive(_rollout_bytes())
    grown = _stored(archive, _rollout_bytes() + _settings_line(9))
    with pytest.raises(CapabilityUnavailable, match="RESTORE_IDENTITY_MISMATCH"):
        assert_restored(_HooklessAdapter(), archive, grown)
    assert_restored(_HooklessAdapter(), archive, _stored(archive, _rollout_bytes()))


# -- refusal cases ----------------------------------------------------------


@pytest.mark.parametrize(
    "suffix",
    [
        pytest.param(
            b'{"type":"response_item","payload":{"type":"message"}}\n',
            id="response_item",
        ),
        pytest.param(
            b'{"type":"session_meta","payload":{"session_id":"x"}}\n',
            id="new_session_meta",
        ),
        pytest.param(
            b'{"type":"event_msg","payload":{"type":"other_event"}}\n', id="other_event"
        ),
        pytest.param(b"not-json\n", id="malformed"),
        pytest.param(b"\n", id="blank_line"),
        pytest.param(b'{"type":"event_msg"', id="truncated"),
        pytest.param(
            b'{"type":"event_msg","payload":{"type":"thread_settings_applied"}}',
            id="no_trailing_newline",
        ),
    ],
)
def test_appendix_refuses_any_other_history(tmp_path, suffix):
    archive = _archive(_rollout_bytes())
    journal = _rollout_bytes() + suffix
    adapter = _adapter(_store(tmp_path))
    assert not CodexRolloutPersistenceAdapter.verify_restored_store(
        archive, _stored(archive, journal)
    )
    with pytest.raises(CapabilityUnavailable, match="RESTORE_IDENTITY_MISMATCH"):
        assert_restored(adapter, archive, _stored(archive, journal))
    assert not _journal_appendix_ok(journal, _rollout_bytes())


def test_appendix_refuses_oversize_suffix(tmp_path):
    archive = _archive(_rollout_bytes())
    line = _settings_line(9)
    journal = _rollout_bytes() + line * (_MAX_APPENDIX_BYTES // len(line) + 1)
    adapter = _adapter(_store(tmp_path))
    assert not CodexRolloutPersistenceAdapter.verify_restored_store(
        archive, _stored(archive, journal)
    )
    with pytest.raises(CapabilityUnavailable, match="RESTORE_IDENTITY_MISMATCH"):
        assert_restored(adapter, archive, _stored(archive, journal))


def test_hook_refuses_mutated_prefix_and_metadata_drift(tmp_path):
    archive = _archive(_rollout_bytes())
    adapter = _adapter(_store(tmp_path))
    mutated = _stored(archive, b"TAMPERED-HEAD\n" + _rollout_bytes())
    with pytest.raises(CapabilityUnavailable, match="RESTORE_IDENTITY_MISMATCH"):
        assert_restored(adapter, archive, mutated)
    metadata_drift = replace(
        archive, records={**archive.records, "untether.json": b'{"generation":9}'}
    )
    assert not CodexRolloutPersistenceAdapter.verify_restored_store(
        archive, _stored(metadata_drift, _rollout_bytes())
    )


def test_identity_drift_refuses_even_with_hook(tmp_path):
    archive = _archive(_rollout_bytes())
    adapter = _adapter(_store(tmp_path))
    for field in ("generation", "lineage", "native_id"):
        drifted = replace(
            _stored(archive, _rollout_bytes()),
            **{field: archive.generation + 1 if field == "generation" else "other"},
        )
        with pytest.raises(CapabilityUnavailable, match="RESTORE_IDENTITY_MISMATCH"):
            assert_restored(adapter, archive, drifted)


# -- import_overwrite uses the same validator -------------------------------


def _overwrite_records(journal: bytes) -> dict[str, bytes]:
    return {
        "session.jsonl": journal,
        "untether.json": json.dumps(
            {
                "logical_id": _SID,
                "native_id": _SID,
                "generation": 1,
                "lineage": _LINEAGE,
            }
        ).encode(),
    }


def test_import_overwrite_accepts_settings_appendix_and_refuses_other(tmp_path):
    root = _store(tmp_path)
    prior = _rollout_bytes()
    target = _rollout_bytes([{"ordinal": 2, "type": "event_msg", "payload": {}}])
    path = root / _REL
    path.parent.mkdir(parents=True)
    path.write_bytes(prior)
    adapter = _adapter(root)
    records = _overwrite_records(target)
    # An idempotent retry whose stored bytes are the target plus a probe append.
    path.write_bytes(target + _settings_line(9))
    adapter.import_overwrite(
        engine=_ENGINE,
        native_id=_SID,
        records=records,
        expected_prior_records={"session.jsonl": prior},
        generation=1,
        lineage=_LINEAGE,
    )
    assert path.read_bytes() == target
    # Other history in the suffix is refused, never overwritten.
    path.write_bytes(target + b'{"type":"response_item","payload":{}}\n')
    with pytest.raises(CapabilityUnavailable, match="OVERWRITE_PRIOR_MISMATCH"):
        adapter.import_overwrite(
            engine=_ENGINE,
            native_id=_SID,
            records=records,
            expected_prior_records={"session.jsonl": prior},
            generation=1,
            lineage=_LINEAGE,
        )
    assert path.read_bytes() == target + b'{"type":"response_item","payload":{}}\n'


# -- reclaim transitional overwrite (journal landed, sidecar still prior) ----


def _evolved_journal() -> bytes:
    return _rollout_bytes(
        [{"ordinal": 5, "type": "event_msg", "payload": {"type": "user_message"}}]
    )


def _crashed_current(prior: NativeArchive, journal: bytes) -> NativeArchive:
    """State after the journal replace but before the metadata upsert."""
    return replace(
        prior,
        records={
            "session.jsonl": journal,
            "untether.json": prior.records["untether.json"],
        },
    )


def test_preoverwrite_accepts_crashed_journal_before_metadata(tmp_path):
    prior = _archive(_rollout_bytes(), generation=0)
    incoming = _archive(_evolved_journal(), generation=1)
    adapter = _adapter(_store(tmp_path))
    # Incoming journal already stored (plus the engine's own resume append)
    # while the identity sidecar is still the prior generation.
    current = _crashed_current(
        prior, incoming.records["session.jsonl"] + _settings_line(9)
    )
    assert current.generation == prior.generation
    assert _preoverwrite_matches(adapter, incoming, prior, current)


def test_preoverwrite_refuses_arbitrary_metadata_in_transition(tmp_path):
    prior = _archive(_rollout_bytes(), generation=0)
    incoming = _archive(_evolved_journal(), generation=1)
    adapter = _adapter(_store(tmp_path))
    forged = (
        b'{"logical_id":"other","native_id":"other","generation":0,'
        b'"lineage":"codex-rollout-v1:1999/01/01/evil.jsonl"}'
    )
    current = replace(
        _crashed_current(prior, incoming.records["session.jsonl"]),
        records={
            "session.jsonl": incoming.records["session.jsonl"],
            "untether.json": forged,
        },
    )
    assert not _preoverwrite_matches(adapter, incoming, prior, current)


def test_preoverwrite_refuses_response_append_in_transition(tmp_path):
    prior = _archive(_rollout_bytes(), generation=0)
    incoming = _archive(_evolved_journal(), generation=1)
    adapter = _adapter(_store(tmp_path))
    current = _crashed_current(
        prior,
        incoming.records["session.jsonl"]
        + b'{"type":"response_item","payload":{"type":"message"}}\n',
    )
    assert not _preoverwrite_matches(adapter, incoming, prior, current)


def test_preoverwrite_refuses_foreign_prior_engine(tmp_path):
    prior = _archive(_rollout_bytes(), generation=0)
    incoming = _archive(_evolved_journal(), generation=1)
    adapter = _adapter(_store(tmp_path))
    current = _crashed_current(prior, incoming.records["session.jsonl"])
    assert not _preoverwrite_matches(
        adapter, incoming, replace(prior, engine="other"), current
    )
