"""Restore collected OMP bundles into disposable trees; no production wiring."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from untether.session_migration.native_archive import FidelityError
from untether.session_migration.omp_bundle import collect_omp_records
from untether.session_migration.omp_restore import restore_omp_records

JOURNAL_NAME = "20260101T000000Z_aaa111.jsonl"


def _journal(sid: str, cwd: str, *entries: dict) -> bytes:
    lines = [{"type": "session", "version": 3, "id": sid, "cwd": cwd}, *entries]
    return "".join(json.dumps(e) + "\n" for e in lines).encode()


def _bundle(tmp_path: Path, sid: str = "aaa111") -> dict[str, bytes]:
    sessions = tmp_path / "sessions"
    blobs = tmp_path / "agent" / "blobs" / "data"
    sessions.mkdir(parents=True)
    blobs.mkdir(parents=True)
    digest = hashlib.sha256(b"image-data").hexdigest()
    (blobs / digest).write_bytes(b"image-data")
    journal = sessions / JOURNAL_NAME
    journal.write_bytes(
        _journal(
            sid,
            "/work/demo",
            {"type": "message", "id": "m1", "text": "blob:sha256:" + digest},
        )
    )
    artifacts = sessions / JOURNAL_NAME[: -len(".jsonl")] / "artifacts"
    artifacts.mkdir(parents=True)
    (artifacts / "tool-output.txt").write_bytes(b"truncated output")
    return collect_omp_records(
        sessions_dir=sessions,
        blobs_dir=blobs,
        native_id=sid,
        max_expanded=1 << 20,
        max_file_bytes=1 << 20,
    )


def _restore(tmp_path: Path, records: dict[str, bytes], tag: str, **overrides):
    kwargs = {
        "records": records,
        "sessions_dir": tmp_path / tag / "sessions",
        "blobs_dir": tmp_path / tag / "agent" / "blobs" / "data",
        "journal_name": JOURNAL_NAME,
    }
    kwargs.update(overrides)
    return restore_omp_records(**kwargs)


@pytest.mark.parametrize("corrupt", [False, True])
def test_restore_shared_blob_preserves_existing_content(
    tmp_path: Path, corrupt: bool
) -> None:
    """Moving another session must reuse shared assets without overwriting them."""
    records = _bundle(tmp_path)
    digest = hashlib.sha256(b"image-data").hexdigest()
    blobs = tmp_path / "dest" / "agent" / "blobs" / "data"
    blobs.mkdir(parents=True)
    original = b"corrupt-existing" if corrupt else b"image-data"
    blob = blobs / digest
    blob.write_bytes(original)
    if corrupt:
        with pytest.raises(ValueError, match="OMP_RESTORE_BLOB_MISMATCH"):
            _restore(tmp_path, records, "dest")
        assert not (tmp_path / "dest" / "sessions" / JOURNAL_NAME).exists()
    else:
        journal = _restore(tmp_path, records, "dest")
        assert journal.read_bytes() == records["session.jsonl"]
    assert blob.read_bytes() == original


def test_restore_roundtrip_exact_bytes_and_refs(tmp_path: Path) -> None:
    records = _bundle(tmp_path)
    journal = tmp_path / "sessions" / JOURNAL_NAME
    restored = _restore(tmp_path, records, "dest")
    assert restored.read_bytes() == journal.read_bytes()
    digest = hashlib.sha256(b"image-data").hexdigest()
    assert (tmp_path / "dest" / "agent" / "blobs" / "data" / digest).read_bytes() == (
        b"image-data"
    )
    artifact = (
        tmp_path
        / "dest"
        / "sessions"
        / JOURNAL_NAME[:-6]
        / "artifacts"
        / "tool-output.txt"
    )
    assert artifact.read_bytes() == b"truncated output"
    # Second restore refuses rather than overwriting the journal.
    with pytest.raises(FileExistsError):
        _restore(tmp_path, records, "dest")
    assert restored.read_bytes() == journal.read_bytes()


def test_restore_refusals(tmp_path: Path) -> None:
    records = _bundle(tmp_path)
    dest_sessions = tmp_path / "dest2" / "sessions"
    dest_blobs = tmp_path / "dest2" / "agent" / "blobs" / "data"
    common = {
        "sessions_dir": dest_sessions,
        "blobs_dir": dest_blobs,
        "journal_name": JOURNAL_NAME,
    }
    with pytest.raises(ValueError, match="OMP_RESTORE_RECORD_UNKNOWN"):
        restore_omp_records(records={**records, "extra/evil": b"x"}, **common)
    with pytest.raises(ValueError, match="OMP_RESTORE_PATH_INVALID"):
        restore_omp_records(
            records={
                "session.jsonl": records["session.jsonl"],
                "artifacts/../escape.txt": b"x",
            },
            **common,
        )
    bad = {
        k: (b"tampered" if k.startswith("blobs/") else v) for k, v in records.items()
    }
    with pytest.raises(ValueError, match="OMP_RESTORE_BLOB_MISMATCH"):
        restore_omp_records(records=bad, **common)
    with pytest.raises(ValueError, match="OMP_RESTORE_BLOB_NAME_INVALID"):
        restore_omp_records(
            records={
                "session.jsonl": records["session.jsonl"],
                "blobs/not-hex": b"x",
            },
            **common,
        )
    with pytest.raises(ValueError, match="OMP_RESTORE_MISSING_JOURNAL"):
        restore_omp_records(records={"artifacts/x": b"x"}, **common)
    with pytest.raises(ValueError, match="OMP_RESTORE_JOURNAL_NAME_INVALID"):
        restore_omp_records(
            records=records,
            sessions_dir=dest_sessions,
            blobs_dir=dest_blobs,
            journal_name="../escape.jsonl",
        )
    # Nothing was created by refused runs.
    assert not dest_sessions.exists() or not any(dest_sessions.iterdir())
    assert not dest_blobs.exists() or not any(dest_blobs.iterdir())


@pytest.mark.parametrize("child", [False, True])
def test_restore_missing_blob_fails_before_writes(tmp_path: Path, child: bool) -> None:
    records = _bundle(tmp_path)
    digest = hashlib.sha256(b"missing").hexdigest()
    journal = _journal(
        "child" if child else "aaa111",
        "/work/demo",
        {"type": "message", "text": "blob:sha256:" + digest},
    )
    records["artifacts/child.jsonl" if child else "session.jsonl"] = journal
    with pytest.raises(ValueError, match="OMP_RESTORE_MISSING_BLOB"):
        _restore(tmp_path, records, "dest")
    assert not (tmp_path / "dest").exists()


@pytest.mark.parametrize(
    "name",
    [r"..\escape", r"nested\escape", "", ".", "a/./b", "NUL.txt", "COM1", "a.", "a "],
)
def test_restore_rejects_path_aliases(tmp_path: Path, name: str) -> None:
    records = _bundle(tmp_path)
    records["artifacts/" + name] = b"bad"
    with pytest.raises(ValueError, match="OMP_RESTORE_PATH_INVALID"):
        _restore(tmp_path, records, "dest")
    assert not (tmp_path / "dest").exists()


def test_restore_artifact_collision_leaves_no_journal(tmp_path: Path) -> None:
    records = _bundle(tmp_path)
    sessions = tmp_path / "dest" / "sessions"
    artifact = sessions / JOURNAL_NAME[:-6] / "artifacts" / "tool-output.txt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        _restore(tmp_path, records, "dest")
    assert not (sessions / JOURNAL_NAME).exists()
    assert artifact.read_bytes() == b"keep"


@pytest.mark.parametrize("names", [("a", "a/b"), ("A", "a")])
def test_restore_planned_conflicts_leave_no_journal(tmp_path: Path, names) -> None:
    records = _bundle(tmp_path)
    records.update({"artifacts/" + name: b"x" for name in names})
    with pytest.raises(ValueError, match="OMP_RESTORE_TARGET_COLLISION"):
        _restore(tmp_path, records, "dest")
    assert not (tmp_path / "dest").exists()


def test_restore_parent_symlink_refused(tmp_path: Path) -> None:
    records = _bundle(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    dest = tmp_path / "dest"
    try:
        dest.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("directory symlinks unavailable")
    with pytest.raises(FidelityError, match="OMP_LINK_REFUSED"):
        _restore(tmp_path, records, "dest")
    assert not any(outside.iterdir())


def test_collect_missing_blob_fails_closed(tmp_path: Path) -> None:
    _bundle(tmp_path)
    digest = hashlib.sha256(b"image-data").hexdigest()
    (tmp_path / "agent" / "blobs" / "data" / digest).unlink()
    with pytest.raises(FidelityError, match="OMP_FILESYSTEM_INVALID"):
        collect_omp_records(
            sessions_dir=tmp_path / "sessions",
            blobs_dir=tmp_path / "agent" / "blobs" / "data",
            native_id="aaa111",
            max_expanded=1 << 20,
            max_file_bytes=1 << 20,
        )
