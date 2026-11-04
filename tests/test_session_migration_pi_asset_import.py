"""Import-side tests for pi bash full-output spill asset restore.

``import_exact`` must validate the complete allowed record set (a strict
manifest plus only manifest-listed ``pi-output/`` records), require the
manifest whenever a journal carries ``fullOutputPath``, preflight every
asset/parent/root collision before any store write, restore the spilled
bytes into the trusted OS temp directory inside the owned rollback scope,
and independently reread every asset after the restart probe. The OS temp
directory is monkeypatched to a disposable per-test directory; no test ever
touches the real system temp directory.
"""

import json
import tempfile
from dataclasses import replace
from pathlib import Path

import pytest

from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.native_archive import NativeArchive

from .test_session_migration_file_bundle_adapter import (
    adapter,
)

_LOG = "pi-bash-0123456789abcdef.log"
_BLOB = b"SPILL-BYTES-" * 5


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


def _journal(sid: str, full_output_path: str | None) -> bytes:
    header = {"type": "session", "version": 3, "id": sid}
    lines = [header]
    if full_output_path is not None:
        lines.append(
            {
                "type": "message",
                "message": {
                    "role": "bashExecution",
                    "fullOutputPath": full_output_path,
                },
            }
        )
    else:
        lines.append({"type": "message", "message": {"role": "user"}})
    return (
        json.dumps(header) + "\n" + "\n".join(json.dumps(e) for e in lines[1:]) + "\n"
    ).encode()


def _exported_archive(source, sid: str, log: Path) -> NativeArchive:
    (source.sessions_dir / f"{sid}.jsonl").write_bytes(_journal(sid, str(log)))
    return source.export_exact(sid)


def _asset_record_name() -> str:
    return f"pi-output/{_LOG}"


def _manifest_bytes(log_path: Path, blob: bytes) -> bytes:
    import hashlib

    return json.dumps(
        {
            str(log_path): {
                "record": _asset_record_name(),
                "sha256": hashlib.sha256(blob).hexdigest(),
                "size": len(blob),
            }
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _no_probe_runner(engine):
    """Replace the restart probe with a no-op that records invocations."""
    calls: list[tuple[str, ...]] = []
    return replace(engine, restart_runner=lambda argv: calls.append(argv)), calls


def test_import_restores_exact_bytes_and_root_unchanged(tmp_path, disposable_tmpdir):
    source = adapter(tmp_path / "source")
    log = disposable_tmpdir / _LOG
    log.write_bytes(_BLOB)
    archive = _exported_archive(source, "s1", log)
    assert archive.records["session.jsonl"] == _journal("s1", str(log))
    assert archive.records[_asset_record_name()] == _BLOB

    fresh = tmp_path / "fresh-store"
    engine, calls = _no_probe_runner(adapter(fresh))
    proof = engine.import_exact(archive)

    # Restored bytes are exact.
    assert (disposable_tmpdir / _LOG).read_bytes() == _BLOB
    # The root journal is byte-identical, store name unchanged (canonical).
    restored = (fresh / "s1.jsonl").read_bytes()
    assert restored == archive.records["session.jsonl"]
    assert proof.record_hashes["session.jsonl"] in [
        __import__("hashlib").sha256(restored).hexdigest()
    ]
    # Restart probe ran against the restored root.
    assert calls and "s1.jsonl" in str(calls[0][-2])


def test_import_materializes_absent_spill_before_probe(tmp_path, disposable_tmpdir):
    source = adapter(tmp_path / "source")
    log = disposable_tmpdir / _LOG
    log.write_bytes(_BLOB)
    archive = _exported_archive(source, "s1", log)
    log.unlink()  # disjoint destination must not rely on the source's spill file
    observed = []

    def reading_probe(argv):
        observed.append(log.read_bytes())

    engine = replace(adapter(tmp_path / "destination"), restart_runner=reading_probe)
    proof = engine.import_exact(archive)
    assert observed == [_BLOB]
    assert log.read_bytes() == _BLOB
    assert engine.export_exact("s1").records == archive.records
    assert proof.restarted and proof.resumed


def test_identical_reuse_no_second_write(tmp_path, disposable_tmpdir):
    source = adapter(tmp_path / "source")
    log = disposable_tmpdir / _LOG
    log.write_bytes(_BLOB)
    archive = _exported_archive(source, "s1", log)
    fresh = tmp_path / "fresh-store"
    engine, _ = _no_probe_runner(adapter(fresh))
    # Pre-place a byte-identical spill file: import must reuse it.
    log.write_bytes(_BLOB)
    before = log.stat().st_mtime_ns, log.stat().st_size
    engine.import_exact(archive)
    after = log.stat().st_mtime_ns, log.stat().st_size
    assert before == after  # reused, never rewritten


def test_asset_collision_refuses_with_zero_store_writes(tmp_path, disposable_tmpdir):
    source = adapter(tmp_path / "source")
    log = disposable_tmpdir / _LOG
    log.write_bytes(_BLOB)
    archive = _exported_archive(source, "s1", log)
    fresh = tmp_path / "fresh-store"
    engine, _ = _no_probe_runner(adapter(fresh))
    # A colliding, non-identical spill file exists at the destination.
    log.write_bytes(b"FOREIGN-CONTENT")
    with pytest.raises(
        CapabilityUnavailable, match=r"PI_OUTPUT_REFUSED|COLLISION|REFUSED"
    ):
        engine.import_exact(archive)
    # Zero store writes: only the empty store dir exists.
    assert list(fresh.iterdir()) == []
    # The colliding asset was never modified.
    assert log.read_bytes() == b"FOREIGN-CONTENT"


def test_missing_asset_after_probe_refuses_with_owned_rollback(
    tmp_path, disposable_tmpdir
):
    source = adapter(tmp_path / "source")
    log = disposable_tmpdir / _LOG
    log.write_bytes(_BLOB)
    archive = _exported_archive(source, "s1", log)
    fresh = tmp_path / "fresh-store"
    engine, _ = _no_probe_runner(adapter(fresh))

    # Mutate the spill file during the restart probe window so the
    # post-probe independent reread sees non-identical bytes.
    def mutating_probe(argv):
        log.write_bytes(b"MUTATED")

    engine = replace(engine, restart_runner=mutating_probe)
    with pytest.raises(CapabilityUnavailable):
        engine.import_exact(archive)
    # Owned rollback: the store journal and metadata are gone (the empty
    # .untether-migration dir is pre-existing adapter behavior).
    assert not (fresh / "s1.jsonl").exists()
    assert list((fresh / ".untether-migration").iterdir()) == []
    # The mutated asset file is owned by the OS temp consumer, not the
    # import (it pre-existed at preflight, so it is NOT rolled back): it
    # must remain untouched by the failure path.
    assert log.read_bytes() == b"MUTATED"


def test_asset_deleted_between_restore_and_probe_refuses(tmp_path, disposable_tmpdir):
    source = adapter(tmp_path / "source")
    log = disposable_tmpdir / _LOG
    log.write_bytes(_BLOB)
    archive = _exported_archive(source, "s1", log)
    fresh = tmp_path / "fresh-store"
    engine, _ = _no_probe_runner(adapter(fresh))

    def deleting_probe(argv):
        log.unlink()

    engine = replace(engine, restart_runner=deleting_probe)
    with pytest.raises(CapabilityUnavailable):
        engine.import_exact(archive)
    assert not (fresh / "s1.jsonl").exists()
    assert list((fresh / ".untether-migration").iterdir()) == []


def test_journal_ref_without_manifest_refused_before_writes(
    tmp_path, disposable_tmpdir
):
    # Hand-crafted archive: journal has fullOutputPath but no manifest.
    blob = _journal("s1", "C:\\" + _LOG if False else _LOG)
    archive = NativeArchive(
        engine="pi",
        logical_id="s1",
        native_id="s1",
        generation=0,
        lineage="file-bundle:pi:s1.jsonl",
        records={"session.jsonl": blob},
        digest="0" * 64,
    )
    fresh = tmp_path / "fresh-store"
    engine, _ = _no_probe_runner(adapter(fresh))
    with pytest.raises(CapabilityUnavailable, match="PI_OUTPUT_REFUSED"):
        engine.import_exact(archive)
    assert list(fresh.iterdir()) == []


def test_orphan_asset_record_refused(tmp_path, disposable_tmpdir):
    source = adapter(tmp_path / "source")
    log = disposable_tmpdir / _LOG
    log.write_bytes(_BLOB)
    archive = _exported_archive(source, "s1", log)
    tampered = dict(archive.records)
    tampered["pi-output/pi-bash-ffffffffffffffff.log"] = b"orphan"
    fresh = tmp_path / "fresh-store"
    engine, _ = _no_probe_runner(adapter(fresh))
    bad = NativeArchive(
        engine=archive.engine,
        logical_id=archive.logical_id,
        native_id=archive.native_id,
        generation=archive.generation,
        lineage=archive.lineage,
        records=tampered,
        digest=archive.digest,
    )
    with pytest.raises(CapabilityUnavailable, match="RECORD_LAYOUT_REFUSED"):
        engine.import_exact(bad)
    assert list(fresh.iterdir()) == []


def test_no_manifest_no_ref_behavior_unchanged(tmp_path, disposable_tmpdir):
    plain = _journal("s1", None)
    archive = NativeArchive(
        engine="pi",
        logical_id="s1",
        native_id="s1",
        generation=0,
        lineage="file-bundle:pi:s1.jsonl",
        records={"session.jsonl": plain},
        digest="0" * 64,
    )
    fresh = tmp_path / "fresh-store"
    engine, _ = _no_probe_runner(adapter(fresh))
    proof = engine.import_exact(archive)
    assert (fresh / "s1.jsonl").read_bytes() == plain
    assert proof.record_hashes == {
        "session.jsonl": proof.record_hashes["session.jsonl"]
    }
    # No spill dir writes at all.
    assert list(disposable_tmpdir.iterdir()) == []
