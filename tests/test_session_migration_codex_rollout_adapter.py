"""Disposable-root tests for the Codex rollout persistence adapter: header
scan strictness, project cwd authorization, gated export, byte-exact
roundtrip with the metadata record, forged-identity refusal, failed-probe
rollback, probe prefix enforcement, collision and bounded reclaim overwrite.

All fakes are injected via ``rpc_resume``; no test spawns a real process.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from untether.session_migration.codex_rollout_adapter import (
    CodexRolloutPersistenceAdapter,
    _assert_read_history,
)
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
)
from untether.session_migration.native_archive import (
    NativeArchive,
    decode_native,
    encode_native,
)
from untether.session_migration.session_gate import GateRefused, migration_lock

_ENGINE = "codex"


def _session_id(name: str) -> str:
    return "01a17ff7-ee74-7ef1-b6fd-db188d65de" + name


def _rollout_bytes(
    sid: str,
    cwd: str,
    *,
    model: str = "gpt-5.1-codex",
    provider: str = "openai",
    extra_line: dict | None = None,
    wrong_type: str | None = None,
    payload_overrides: dict | None = None,
) -> bytes:
    """Synthetic rollout: line 1 session_meta, optional message entry after."""
    payload = {
        "session_id": sid,
        "id": sid,
        "timestamp": "2026-10-27T10:35:22.000Z",
        "cwd": cwd,
        "originator": "codex-cli",
        "cli_version": "0.55.0",
        "model_provider": provider,
        "base_instructions": {"provenance": {"model": model}},
    }
    if payload_overrides:
        payload.update(payload_overrides)
    entry_type = wrong_type or "session_meta"
    lines = [{"ordinal": 0, "type": entry_type, "payload": payload}]
    if extra_line is not None:
        lines.append({"ordinal": 1, "type": "event_msg", "payload": extra_line})
    return "".join(json.dumps(line) + "\n" for line in lines).encode()


def _store(tmp_path: Path, name: str) -> Path:
    """sessions_root as a real absolute path (resolved against 8.3 short
    names and /tmp symlink setups alike)."""
    root = tmp_path / name
    root.mkdir()
    return Path(os.path.realpath(root))


def _adapter(root: Path, **kwargs) -> CodexRolloutPersistenceAdapter:
    return CodexRolloutPersistenceAdapter(
        engine=_ENGINE,
        sessions_root=root,
        app_server_executable=Path("C:/fake/codex.exe"),
        **kwargs,
    )


def _seed(root: Path, rel: str, data: bytes) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _append_probe():
    """rpc_resume fake mimicking a successful resume: appends the
    thread_settings_applied event the real engine appends."""
    appended = (
        json.dumps(
            {
                "ordinal": 9,
                "type": "event_msg",
                "payload": {
                    "type": "thread_settings_applied",
                    "thread_settings": {},
                },
            }
        ).encode()
        + b"\n"
    )

    def probe(native_id, journal_path, timeout_s):
        with journal_path.open("ab") as stream:
            stream.write(appended)

    return probe


def _records(
    sid: str,
    data: bytes,
    *,
    generation: int = 0,
    lineage: str,
    metadata: dict | None = None,
    include_metadata: bool = True,
) -> dict[str, bytes]:
    records = {"session.jsonl": data}
    if include_metadata:
        body = dict(
            metadata
            or {
                "logical_id": sid,
                "native_id": sid,
                "generation": generation,
                "lineage": lineage,
            }
        )
        records["untether.json"] = json.dumps(body).encode()
    return records


def _archive(
    sid: str,
    data: bytes,
    *,
    generation: int = 0,
    lineage: str,
    records: dict[str, bytes] | None = None,
) -> NativeArchive:
    return NativeArchive(
        engine=_ENGINE,
        logical_id=sid,
        native_id=sid,
        generation=generation,
        lineage=lineage,
        records=(
            records
            if records is not None
            else _records(sid, data, generation=generation, lineage=lineage)
        ),
        digest="0" * 64,
    )


@pytest.fixture(autouse=True)
def gate_root(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "shared-gates"))
    return tmp_path / "shared-gates"


# -- store construction ---------------------------------------------------


def test_adapter_refuses_missing_store(tmp_path):
    with pytest.raises(CapabilityUnavailable, match="PERSISTENCE_STORE_NOT_READY"):
        _adapter(tmp_path / "absent")


def test_adapter_refuses_symlinked_store(tmp_path):
    target = tmp_path / "real"
    target.mkdir()
    root = tmp_path / "link"
    try:
        root.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink privilege unavailable on this host")
    with pytest.raises(CapabilityUnavailable, match="PERSISTENCE_STORE_SYMLINK"):
        _adapter(root)


def test_adapter_refuses_non_positive_timeout(tmp_path):
    root = _store(tmp_path, "store")
    with pytest.raises(CapabilityUnavailable, match="PERSISTENCE_LIMIT_INVALID"):
        _adapter(root, resume_timeout_s=0)


# -- bounded header scan --------------------------------------------------


def test_lookup_refuses_wrong_header_type(tmp_path):
    root = _store(tmp_path, "store")
    sid = _session_id("a0")
    # Line 1 is a message entry, not the meta header: refused by identity.
    _seed(
        root,
        "2026/10/27/x.jsonl",
        _rollout_bytes(sid, "/work", wrong_type="event_msg"),
    )
    engine = _adapter(root)
    with pytest.raises(CapabilityUnavailable, match="SESSION_NOT_FOUND"):
        engine.lookup_exact(sid)


def test_lookup_strict_header_session_id_mismatch(tmp_path):
    root = _store(tmp_path, "store")
    sid = _session_id("a1")
    # Meta-shaped line 1 whose payload.session_id is foreign: not a match.
    _seed(
        root,
        "2026/10/27/x.jsonl",
        _rollout_bytes(sid, "/work", payload_overrides={"session_id": "other"}),
    )
    engine = _adapter(root)
    with pytest.raises(CapabilityUnavailable, match="SESSION_NOT_FOUND"):
        engine.lookup_exact(sid)


def test_lookup_ambiguity_refused(tmp_path):
    root = _store(tmp_path, "store")
    sid = _session_id("a2")
    _seed(root, "2026/10/27/a.jsonl", _rollout_bytes(sid, "/work"))
    _seed(root, "2026/10/28/b.jsonl", _rollout_bytes(sid, "/elsewhere"))
    engine = _adapter(root)
    with pytest.raises(CapabilityUnavailable, match="SESSION_LOOKUP_AMBIGUOUS"):
        engine.lookup_exact(sid)


def test_lookup_scans_only_digit_date_tree(tmp_path):
    root = _store(tmp_path, "store")
    sid = _session_id("a3")
    # Real rollout in the date tree plus an identical-id file outside it
    # (junk dir, flat root, wrong extension): the bounded scan must not
    # count the outside copies as matches.
    _seed(root, "2026/10/27/ok.jsonl", _rollout_bytes(sid, "/work"))
    _seed(root, "junk/2026/10/27/x.jsonl", _rollout_bytes(sid, "/work"))
    _seed(root, "flat.jsonl", _rollout_bytes(sid, "/work"))
    _seed(root, "2026/10/27/note.txt", _rollout_bytes(sid, "/work"))
    engine = _adapter(root)
    assert engine.lookup_exact(sid) == sid


def test_lookup_refuses_invalid_native_id(tmp_path):
    root = _store(tmp_path, "store")
    engine = _adapter(root)
    for bad in ("", "../escape", "a/b", "a\\b", "x" * 129):
        with pytest.raises(CapabilityUnavailable, match="NATIVE_ID_INVALID"):
            engine.lookup_exact(bad)


# -- project authorization ------------------------------------------------


def test_lookup_for_project_authorizes_exact_header_cwd(tmp_path):
    root = _store(tmp_path, "store")
    mine = _session_id("b0")
    _seed(root, "2026/10/27/mine.jsonl", _rollout_bytes(mine, "/work/demo"))
    _seed(root, "2026/10/27/other.jsonl", _rollout_bytes(_session_id("b1"), "/else"))
    engine = _adapter(root)
    assert engine.lookup_for_project(mine, PurePosixPath("/work/demo")) == mine
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        engine.lookup_for_project(_session_id("b1"), PurePosixPath("/work/demo"))
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        engine.lookup_for_project(mine, PurePosixPath("/work/other"))


def test_lookup_for_project_windows_cwd_case_insensitive_drive(tmp_path):
    root = _store(tmp_path, "store")
    sid = _session_id("b2")
    _seed(root, "2026/10/27/win.jsonl", _rollout_bytes(sid, "D:\\Work\\Demo"))
    engine = _adapter(root)
    assert engine.lookup_for_project(sid, PureWindowsPath("d:/Work/Demo")) == sid


# -- gated export ---------------------------------------------------------


def test_export_requires_gate_root(tmp_path, monkeypatch):
    root = _store(tmp_path, "store")
    sid = _session_id("c0")
    _seed(root, "2026/10/27/a.jsonl", _rollout_bytes(sid, "/work"))
    monkeypatch.delenv("UNTETHER_SESSION_GATE_ROOT")
    with pytest.raises(CapabilityUnavailable, match="SOURCE_GATE_UNCONFIGURED"):
        _adapter(root).export_exact(sid)


def test_export_locked_requires_owned_migration_lock(tmp_path):
    root = _store(tmp_path, "store")
    sid = _session_id("c1")
    _seed(root, "2026/10/27/a.jsonl", _rollout_bytes(sid, "/work"))
    engine = _adapter(root)
    gate = Path(os.environ["UNTETHER_SESSION_GATE_ROOT"])
    with pytest.raises(GateRefused, match="MIGRATION_LOCK_NOT_OWNED"):
        engine.export_locked(sid)
    with migration_lock(root=gate, engine_id=_ENGINE, native_id=sid):
        assert engine.export_locked(sid).native_id == sid


def test_export_carries_metadata_record_and_exact_bytes(tmp_path):
    root = _store(tmp_path, "store")
    sid = _session_id("c2")
    extra = {"type": "user_message", "text": "hello"}
    data = _rollout_bytes(sid, "/work/demo", extra_line=extra)
    _seed(root, "2026/10/27/rollout-x.jsonl", data)
    engine = _adapter(root, rpc_resume=_append_probe())
    archive = engine.export_exact(sid)
    assert archive.engine == _ENGINE
    assert archive.native_id == sid
    assert archive.lineage == "codex-rollout-v1:2026/10/27/rollout-x.jsonl"
    assert archive.generation == 0
    assert archive.records["session.jsonl"] == data
    assert json.loads(archive.records["untether.json"]) == {
        "logical_id": sid,
        "native_id": sid,
        "generation": 0,
        "lineage": "codex-rollout-v1:2026/10/27/rollout-x.jsonl",
    }


def test_export_unknown_session_refused(tmp_path):
    root = _store(tmp_path, "store")
    _seed(root, "2026/10/27/a.jsonl", _rollout_bytes(_session_id("c3"), "/work"))
    engine = _adapter(root)
    with pytest.raises(CapabilityUnavailable, match="SESSION_NOT_FOUND"):
        engine.export_exact("missing")


# -- codec roundtrip ------------------------------------------------------


def test_archive_codec_roundtrip(tmp_path):
    sid = _session_id("c4")
    data = _rollout_bytes(sid, "/work")
    lineage = "codex-rollout-v1:2026/10/27/rollout-y.jsonl"
    archive = _archive(sid, data, lineage=lineage)
    encoded = encode_native(
        engine=archive.engine,
        logical_id=archive.logical_id,
        native_id=archive.native_id,
        generation=archive.generation,
        lineage=archive.lineage,
        records=archive.records,
        max_bytes=1 << 20,
    )
    decoded = decode_native(encoded, max_archive=1 << 20, max_expanded=1 << 20)
    assert decoded.records == archive.records
    assert (decoded.engine, decoded.native_id, decoded.lineage) == (
        archive.engine,
        archive.native_id,
        archive.lineage,
    )


# -- import_exact ---------------------------------------------------------


def test_import_roundtrip_preserves_identity_and_bytes(tmp_path):
    src = _store(tmp_path, "src")
    dst = _store(tmp_path, "dst")
    sid = _session_id("d0")
    rel = "2026/10/27/rollout-r.jsonl"
    data = _rollout_bytes(sid, "/work/demo", extra_line={"text": "hi"})
    _seed(src, rel, data)
    archive = _adapter(src, rpc_resume=_append_probe()).export_exact(sid)
    proof = _adapter(dst, rpc_resume=_append_probe()).import_exact(archive)
    assert proof.native_id == sid
    assert proof.restarted and proof.resumed
    on_disk = (dst / rel).read_bytes()
    assert on_disk.startswith(data)  # byte-exact prefix at the pinned path
    assert (
        json.loads(on_disk.splitlines()[-1])["payload"]["type"]
        == "thread_settings_applied"
    )  # the probe's own append
    sidecar = dst / ".untether-migration" / (sid + ".json")
    assert json.loads(sidecar.read_bytes())["generation"] == 0
    # Re-export from the target preserves lineage/identity, then re-import
    # collides: the restore is create-only, never overwrite.
    reexported = _adapter(dst, rpc_resume=_append_probe()).export_exact(sid)
    assert reexported.lineage == archive.lineage
    with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
        _adapter(dst, rpc_resume=_append_probe()).import_exact(archive)


def test_import_refuses_unmaterialised_asset_record(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d4")
    archive = _archive(
        sid,
        _rollout_bytes(sid, "/work"),
        lineage="codex-rollout-v1:2026/10/27/x.jsonl",
    )
    archive = replace(
        archive, records={**archive.records, "assets/image.png": b"original asset"}
    )
    with pytest.raises(CapabilityUnavailable, match="RECORD_LAYOUT_REFUSED"):
        _adapter(dst, rpc_resume=_append_probe()).import_exact(archive)
    assert not list(dst.rglob("*.jsonl"))


def test_import_refuses_wrong_engine(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d1")
    lineage = "codex-rollout-v1:2026/10/27/x.jsonl"
    archive = replace(
        _archive(sid, _rollout_bytes(sid, "/work"), lineage=lineage), engine="other"
    )
    with pytest.raises(CapabilityUnavailable, match="ENGINE_MISMATCH"):
        _adapter(dst, rpc_resume=_append_probe()).import_exact(archive)


def test_import_refuses_forged_metadata(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d2")
    lineage = "codex-rollout-v1:2026/10/27/x.jsonl"
    data = _rollout_bytes(sid, "/work")
    records = _records(sid, data, generation=3, lineage=lineage)
    records["untether.json"] = json.dumps(
        {
            "logical_id": sid,
            "native_id": sid,
            "generation": 9,  # forged: disagrees with archive.generation=3
            "lineage": lineage,
        }
    ).encode()
    with pytest.raises(CapabilityUnavailable, match="ARCHIVE_IDENTITY_INVALID"):
        _adapter(dst, rpc_resume=_append_probe()).import_exact(
            _archive(sid, data, generation=3, lineage=lineage, records=records)
        )


def test_import_refuses_traversal_lineage(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d3")
    data = _rollout_bytes(sid, "/work")
    for bad in (
        "codex-rollout-v1:../../escape.jsonl",
        "codex-rollout-v1:C:/abs.jsonl",
        "codex-rollout-v1:2026/10/27/..\\x.jsonl",
        "codex-rollout-v1:/abs/x.jsonl",
        "omp-v1:2026/10/27/x.jsonl",
        "codex-rollout-v1:",
    ):
        with pytest.raises(CapabilityUnavailable, match="ARCHIVE_IDENTITY_INVALID"):
            _adapter(dst, rpc_resume=_append_probe()).import_exact(
                _archive(sid, data, lineage=bad)
            )
    assert not (dst / "escape.jsonl").exists()


def test_import_refuses_header_identity_disagreement(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d4")
    lineage = "codex-rollout-v1:2026/10/27/x.jsonl"
    # Rollout header claims a foreign session_id: identity must come from the
    # engine's own header, never just from the archive envelope.
    data = _rollout_bytes(sid, "/work", payload_overrides={"session_id": "other"})
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_FORMAT_REFUSED"):
        _adapter(dst, rpc_resume=_append_probe()).import_exact(
            _archive(sid, data, lineage=lineage)
        )
    assert not (dst / "2026" / "10" / "27" / "x.jsonl").exists()


def test_import_failed_probe_rolls_back_journal_and_sidecar(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d5")
    rel = "2026/10/27/x.jsonl"
    lineage = "codex-rollout-v1:" + rel
    archive = _archive(sid, _rollout_bytes(sid, "/work"), lineage=lineage)

    def failing_probe(native_id, journal_path, timeout_s):
        raise CapabilityUnavailable("RESTART_PROBE_FAILED")

    with pytest.raises(CapabilityUnavailable, match="RESTART_PROBE_FAILED"):
        _adapter(dst, rpc_resume=failing_probe).import_exact(archive)
    assert not (dst / rel).exists()
    assert not (dst / ".untether-migration" / (sid + ".json")).exists()


def test_probe_prefix_check_refuses_non_append_rewrite(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d6")
    rel = "2026/10/27/x.jsonl"
    lineage = "codex-rollout-v1:" + rel
    data = _rollout_bytes(sid, "/work")
    archive = _archive(sid, data, lineage=lineage)

    def mutating_probe(native_id, journal_path, timeout_s):
        # Not an append: rewrites the file with divergent head bytes — the
        # exact-prefix invariant must catch this.
        journal_path.write_bytes(b"TAMPERED-HEAD\n" + data)

    with pytest.raises(CapabilityUnavailable, match="RESTART_PROBE_FAILED"):
        _adapter(dst, rpc_resume=mutating_probe).import_exact(archive)
    assert not (dst / rel).exists()  # rolled back


def test_import_overwrite_idempotent_retry_then_prior_mismatch_refusal(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d7")
    rel = "2026/10/27/x.jsonl"
    lineage = "codex-rollout-v1:" + rel
    prior_bytes = _rollout_bytes(sid, "/work")
    _seed(dst, rel, prior_bytes)
    engine = _adapter(dst, rpc_resume=_append_probe())
    evolved = _rollout_bytes(sid, "/work", extra_line={"text": "evolved"})
    metadata = {
        "logical_id": sid,
        "native_id": sid,
        "generation": 1,
        "lineage": lineage,
    }
    records = _records(sid, evolved, generation=1, lineage=lineage, metadata=metadata)
    # Divergent prior bytes refuse FIRST: the store still holds the prior
    # rollout, and the target bytes share only the header prefix — never
    # touched on refusal.
    with pytest.raises(CapabilityUnavailable, match="OVERWRITE_PRIOR_MISMATCH"):
        engine.import_overwrite(
            engine=_ENGINE,
            native_id=sid,
            records=records,
            expected_prior_records={"session.jsonl": b"divergent"},
            generation=1,
            lineage=lineage,
        )
    assert (dst / rel).read_bytes() == prior_bytes
    hashes = engine.import_overwrite(
        engine=_ENGINE,
        native_id=sid,
        records=records,
        expected_prior_records={"session.jsonl": prior_bytes},
        generation=1,
        lineage=lineage,
    )
    assert hashes["session.jsonl"] == hashlib.sha256(evolved).hexdigest()
    after_first = (dst / rel).read_bytes()
    assert after_first.startswith(evolved)  # probe appended the settings event
    assert (
        json.loads((dst / ".untether-migration" / (sid + ".json")).read_bytes())[
            "generation"
        ]
        == 1
    )
    # Idempotent retry: current bytes carry the target as exact prefix.
    engine.import_overwrite(
        engine=_ENGINE,
        native_id=sid,
        records=records,
        expected_prior_records={"session.jsonl": prior_bytes},
        generation=1,
        lineage=lineage,
    )
    assert (dst / rel).read_bytes() == after_first


def test_import_overwrite_refuses_foreign_engine_and_identity(tmp_path):
    dst = _store(tmp_path, "dst")
    sid = _session_id("d8")
    rel = "2026/10/27/x.jsonl"
    lineage = "codex-rollout-v1:" + rel
    prior_bytes = _rollout_bytes(sid, "/work")
    _seed(dst, rel, prior_bytes)
    engine = _adapter(dst, rpc_resume=_append_probe())
    evolved = _rollout_bytes(sid, "/work", extra_line={"text": "evolved"})
    records = _records(
        sid,
        evolved,
        generation=1,
        lineage=lineage,
        metadata={
            "logical_id": sid,
            "native_id": sid,
            "generation": 1,
            "lineage": lineage,
        },
    )
    with pytest.raises(CapabilityUnavailable, match="ENGINE_MISMATCH"):
        engine.import_overwrite(
            engine="other",
            native_id=sid,
            records=records,
            expected_prior_records={"session.jsonl": prior_bytes},
            generation=1,
            lineage=lineage,
        )
    # Metadata generation disagrees with the declared generation.
    with pytest.raises(CapabilityUnavailable, match="ARCHIVE_IDENTITY_INVALID"):
        engine.import_overwrite(
            engine=_ENGINE,
            native_id=sid,
            records=records,
            expected_prior_records={"session.jsonl": prior_bytes},
            generation=2,
            lineage=lineage,
        )


@pytest.mark.parametrize("turns", [[], [{"items": []}], [{"items": [{"id": "wrong"}]}]])
def test_history_probe_refuses_missing_native_items(turns):
    data = _rollout_bytes(
        "session",
        "/work",
        extra_line={"type": "item_completed", "item": {"id": "expected"}},
    )
    with pytest.raises(CapabilityUnavailable, match="RESTART_PROBE_FAILED"):
        _assert_read_history(data, {"thread": {"turns": turns}})


def test_history_probe_accepts_metadata_only_and_visible_items():
    _assert_read_history(_rollout_bytes("session", "/work"), {"thread": {}})
    data = _rollout_bytes(
        "session",
        "/work",
        extra_line={"type": "item_completed", "item": {"id": "expected"}},
    )
    _assert_read_history(data, {"thread": {"turns": [{"items": [{"id": "expected"}]}]}})


def test_default_probe_used_when_no_injection(tmp_path):
    """Without an injected rpc_resume the adapter falls through to the real
    subprocess probe, which refuses a bogus executable without spawning."""
    dst = _store(tmp_path, "dst")
    sid = _session_id("d9")
    rel = "2026/10/27/x.jsonl"
    lineage = "codex-rollout-v1:" + rel
    archive = _archive(sid, _rollout_bytes(sid, "/work"), lineage=lineage)
    engine = CodexRolloutPersistenceAdapter(
        engine=_ENGINE,
        sessions_root=dst,
        app_server_executable=Path("C:/fake/definitely-absent-codex.exe"),
    )
    with pytest.raises(CapabilityUnavailable):
        engine.import_exact(archive)
    assert not (dst / rel).exists()
