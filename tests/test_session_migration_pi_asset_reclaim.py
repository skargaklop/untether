"""Reclaim of pi spill-asset archives: A->B->A through the real adapter.

``import_overwrite`` must accept a validated spill-manifest archive the way
``import_exact`` does: preflight every asset destination before ANY store
write, restore absent spill files, require no asset-set equality between the
evolved and prior journal, and refuse collisions and post-probe divergence
without mutating foreign bytes. The OS temp directory is monkeypatched to a
disposable per-test directory; crash/retry fence semantics (journal
replacement durable, retry completes) are exercised after a metadata
interruption. No source journal byte is ever rewritten by a reclaim.
"""

import json
import tempfile
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from untether.session_migration.file_bundle_adapter import (
    FileBundlePersistenceAdapter,
)
from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.settings import EnginePersistenceProfile

_LOG_A = "pi-bash-0123456789abcdef.log"
_LOG_B = "pi-bash-fedcba9876543210.log"
_BLOB_A = b"SPILL-A-" * 6
_BLOB_B = b"SPILL-B-" * 6
_SID = "s1"


@pytest.fixture(autouse=True)
def disposable_tmpdir(tmp_path, monkeypatch):
    """Redirect tempfile.gettempdir() to a disposable per-test directory."""
    spill_dir = tmp_path / "os-tmp"
    spill_dir.mkdir()
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(spill_dir))
    return spill_dir


@pytest.fixture(autouse=True)
def configured_gate(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gates"))


def _journal(sid: str, refs: tuple[Path, ...]) -> bytes:
    header = {"type": "session", "version": 3, "id": sid}
    entries = [
        {
            "type": "message",
            "message": {"role": "bashExecution", "fullOutputPath": str(ref)},
        }
        for ref in refs
    ] or [{"type": "message", "message": {"role": "user"}}]
    return (
        json.dumps(header) + "\n" + "\n".join(json.dumps(e) for e in entries) + "\n"
    ).encode()


def _adapter(root: Path, restart_runner=None) -> FileBundlePersistenceAdapter:
    root.mkdir(mode=0o700, exist_ok=True)
    profile = EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(root),
        session_record="pi",
        restart_argv=("fixture-restart-probe", "{sessions_dir}", "{session_id}"),
    )
    return FileBundlePersistenceAdapter(
        "pi",
        profile,
        root,
        restart_runner=restart_runner
        if restart_runner is not None
        else lambda _argv: None,
    )


def _store_state(store: Path) -> dict[str, bytes]:
    """Every store byte: journal plus migration metadata."""
    return {
        str(path.relative_to(store)): path.read_bytes()
        for path in sorted(store.rglob("*"))
        if path.is_file()
    }


def _push_a(source: FileBundlePersistenceAdapter, log_a: Path):
    (source.sessions_dir / f"{_SID}.jsonl").write_bytes(_journal(_SID, (log_a,)))
    return source.export_exact(_SID)


def _evolve_to_b(source: FileBundlePersistenceAdapter, log_a: Path, log_b: Path):
    """Descendant journal: unchanged A ref plus a new B spill reference."""
    evolved = _journal(_SID, (log_a,)) + _journal(_SID, (log_b,)).split(b"\n", 1)[1]
    (source.sessions_dir / f"{_SID}.jsonl").write_bytes(evolved)
    return source.export_exact(_SID)


def test_reclaim_evolved_asset_session_a_to_b(tmp_path, disposable_tmpdir):
    log_a = disposable_tmpdir / _LOG_A
    log_a.write_bytes(_BLOB_A)
    source = _adapter(tmp_path / "source")
    archive_a = _push_a(source, log_a)

    target = _adapter(tmp_path / "target")
    target.import_exact(archive_a)
    prior = target.export_exact(_SID)
    assert prior.records == archive_a.records

    log_b = disposable_tmpdir / _LOG_B
    log_b.write_bytes(_BLOB_B)
    archive_b = _evolve_to_b(source, log_a, log_b)
    evolved_source_bytes = (source.sessions_dir / f"{_SID}.jsonl").read_bytes()
    assert set(archive_b.records) == {
        "session.jsonl",
        f"pi-output/{_LOG_A}",
        f"pi-output/{_LOG_B}",
        "pi-output-manifest.json",
    }

    # Reclaim: incoming journal evolved (A ref unchanged, B ref new); the
    # asset sets of prior and incoming deliberately differ — that is the
    # contract under test, not an error.
    proof = target.import_overwrite(
        engine="pi",
        native_id=_SID,
        records=archive_b.records,
        expected_prior_records=prior.records,
        generation=1,
        lineage=archive_b.lineage,
    )
    assert proof == {
        name: sha256(blob).hexdigest() for name, blob in archive_b.records.items()
    }
    assert (target.sessions_dir / f"{_SID}.jsonl").read_bytes() == (
        archive_b.records["session.jsonl"]
    )
    # The unchanged asset was reused; the new one was restored byte-exactly.
    assert log_a.read_bytes() == _BLOB_A
    assert log_b.read_bytes() == _BLOB_B
    # No journal byte was rewritten by the reclaim itself.
    assert (source.sessions_dir / f"{_SID}.jsonl").read_bytes() == evolved_source_bytes
    # The reclaimed store re-exports exactly the evolved archive.
    assert target.export_exact(_SID).records == archive_b.records


def test_reclaim_back_to_a_never_touched_prior_only_asset(tmp_path, disposable_tmpdir):
    """A->B->A: reclaiming the earlier state refuses nothing and leaves the
    now-foreign B asset exactly as it stands (no set equality demanded)."""
    log_a = disposable_tmpdir / _LOG_A
    log_a.write_bytes(_BLOB_A)
    source = _adapter(tmp_path / "source")
    archive_a = _push_a(source, log_a)
    target = _adapter(tmp_path / "target")
    target.import_exact(archive_a)
    prior_a = target.export_exact(_SID)
    log_b = disposable_tmpdir / _LOG_B
    log_b.write_bytes(_BLOB_B)
    archive_b = _evolve_to_b(source, log_a, log_b)
    target.import_overwrite(
        engine="pi",
        native_id=_SID,
        records=archive_b.records,
        expected_prior_records=prior_a.records,
        generation=1,
        lineage=archive_b.lineage,
    )

    # ... and back: incoming is the A archive, prior is the B archive.
    proof = target.import_overwrite(
        engine="pi",
        native_id=_SID,
        records=archive_a.records,
        expected_prior_records=archive_b.records,
        generation=2,
        lineage=archive_a.lineage,
    )
    assert proof == {
        name: sha256(blob).hexdigest() for name, blob in archive_a.records.items()
    }
    assert (target.sessions_dir / f"{_SID}.jsonl").read_bytes() == (
        archive_a.records["session.jsonl"]
    )
    assert log_a.read_bytes() == _BLOB_A
    assert log_b.read_bytes() == _BLOB_B  # foreign now; nobody mutated it


def test_collision_refusal_zero_store_writes(tmp_path, disposable_tmpdir):
    log_a = disposable_tmpdir / _LOG_A
    log_a.write_bytes(_BLOB_A)
    source = _adapter(tmp_path / "source")
    archive_a = _push_a(source, log_a)
    target = _adapter(tmp_path / "target")
    target.import_exact(archive_a)
    prior = target.export_exact(_SID)
    store_state = _store_state(target.sessions_dir)

    # Colliding foreign bytes at the new asset destination, written AFTER
    # the evolved archive was captured so the carried bytes differ.
    log_b = disposable_tmpdir / _LOG_B
    log_b.write_bytes(_BLOB_B)
    archive_b = _evolve_to_b(source, log_a, log_b)
    log_b.write_bytes(b"FOREIGN-CONTENT")
    with pytest.raises(CapabilityUnavailable):
        target.import_overwrite(
            engine="pi",
            native_id=_SID,
            records=archive_b.records,
            expected_prior_records=prior.records,
            generation=1,
            lineage=archive_b.lineage,
        )
    # Zero store writes: journal and metadata bytes are exactly as before.
    assert _store_state(target.sessions_dir) == store_state
    # The colliding asset was never modified either.
    assert log_b.read_bytes() == b"FOREIGN-CONTENT"


def test_retry_after_metadata_interruption(tmp_path, disposable_tmpdir):
    log_a = disposable_tmpdir / _LOG_A
    log_a.write_bytes(_BLOB_A)
    source = _adapter(tmp_path / "source")
    archive_a = _push_a(source, log_a)
    target = _adapter(tmp_path / "target")
    target.import_exact(archive_a)
    prior = target.export_exact(_SID)
    log_b = disposable_tmpdir / _LOG_B
    log_b.write_bytes(_BLOB_B)
    archive_b = _evolve_to_b(source, log_a, log_b)
    metadata_path = target.sessions_dir / ".untether-migration" / f"{_SID}.json"

    def losing_probe(_argv):
        metadata_path.unlink()  # metadata lost in the crash window

    interrupted = replace(target, restart_runner=losing_probe)
    with pytest.raises(OSError, match="metadata"):
        interrupted.import_overwrite(
            engine="pi",
            native_id=_SID,
            records=archive_b.records,
            expected_prior_records=prior.records,
            generation=1,
            lineage=archive_b.lineage,
        )
    # Crash-consistent: the journal replacement is durable, assets identical.
    assert (target.sessions_dir / f"{_SID}.jsonl").read_bytes() == (
        archive_b.records["session.jsonl"]
    )
    assert log_a.read_bytes() == _BLOB_A and log_b.read_bytes() == _BLOB_B
    # Fenced retry with the same package completes: replaced bytes accepted,
    # metadata rewritten, probes verify.
    retry = target.import_overwrite(
        engine="pi",
        native_id=_SID,
        records=archive_b.records,
        expected_prior_records=prior.records,
        generation=1,
        lineage=archive_b.lineage,
    )
    assert retry == {
        name: sha256(blob).hexdigest() for name, blob in archive_b.records.items()
    }
    assert metadata_path.is_file()
    assert target.export_exact(_SID).records == archive_b.records


def test_orphan_asset_record_refused_before_writes(tmp_path, disposable_tmpdir):
    log_a = disposable_tmpdir / _LOG_A
    log_a.write_bytes(_BLOB_A)
    source = _adapter(tmp_path / "source")
    archive_a = _push_a(source, log_a)
    target = _adapter(tmp_path / "target")
    target.import_exact(archive_a)
    prior = target.export_exact(_SID)
    log_b = disposable_tmpdir / _LOG_B
    log_b.write_bytes(_BLOB_B)
    archive_b = _evolve_to_b(source, log_a, log_b)
    tampered = dict(archive_b.records)
    tampered["pi-output/pi-bash-ffffffffffffffff.log"] = b"orphan"
    store_state = _store_state(target.sessions_dir)
    with pytest.raises(CapabilityUnavailable, match="RECORD_LAYOUT_REFUSED"):
        target.import_overwrite(
            engine="pi",
            native_id=_SID,
            records=tampered,
            expected_prior_records=prior.records,
            generation=1,
            lineage=archive_b.lineage,
        )
    assert _store_state(target.sessions_dir) == store_state


def test_manifest_without_prior_manifest_refused(tmp_path, disposable_tmpdir):
    """A manifest-carrying reclaim onto a legacy (manifest-less) prior push
    has no verifiable asset baseline and refuses closed before any write."""
    log_a = disposable_tmpdir / _LOG_A
    log_a.write_bytes(_BLOB_A)
    source = _adapter(tmp_path / "source")
    archive_a = _push_a(source, log_a)
    target = _adapter(tmp_path / "target")
    target.import_exact(archive_a)
    log_b = disposable_tmpdir / _LOG_B
    log_b.write_bytes(_BLOB_B)
    archive_b = _evolve_to_b(source, log_a, log_b)
    legacy_prior = {"session.jsonl": archive_a.records["session.jsonl"]}
    store_state = _store_state(target.sessions_dir)
    with pytest.raises(CapabilityUnavailable, match="PI_OUTPUT_IMPORT_NOT_IMPLEMENTED"):
        target.import_overwrite(
            engine="pi",
            native_id=_SID,
            records=archive_b.records,
            expected_prior_records=legacy_prior,
            generation=1,
            lineage=archive_b.lineage,
        )
    assert _store_state(target.sessions_dir) == store_state
