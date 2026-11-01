"""Behavioral tests for bounded local asset preflight and restore.

``preflight_assets`` validates a manifest archive against the local trusted
tmpdir (all paths checked before any write; nonexistent destinations allowed,
existing destinations must match carried bytes exactly), and
``restore_assets`` materializes only absent files O_EXCL/0600 with rollback
of its own creations on failure. Everything runs on disposable ``tmp_path``
trees; the real OS temp directory is never touched.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from pathlib import Path

import pytest

from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.pi_output_assets import (
    MANIFEST_RECORD,
    RECORD_PREFIX,
    preflight_assets,
    restore_assets,
)


def _canon(obj: object) -> bytes:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True).encode()


def _journal(*refs: str) -> bytes:
    header = {"type": "session", "version": 3, "id": "s"}
    lines = [header]
    lines.extend(
        {"type": "message", "message": {"role": "bashExecution", "fullOutputPath": ref}}
        for ref in refs
    )
    return b"\n".join(_canon(e) for e in lines) + b"\n"


def _records(tmpdir: Path, blobs: dict[str, bytes]) -> dict[str, bytes]:
    manifest = {}
    extra: dict[str, bytes] = {}
    for name, blob in sorted(blobs.items()):
        ref = str(tmpdir / name)
        record = RECORD_PREFIX + name
        manifest[ref] = {
            "record": record,
            "sha256": hashlib.sha256(blob).hexdigest(),
            "size": len(blob),
        }
        extra[record] = blob
    return {MANIFEST_RECORD: _canon(manifest), **extra}


def test_preflight_two_absent_assets_plan_only(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    blobs = {
        "pi-bash-" + "a" * 16 + ".log": b"first",
        "pi-bash-" + "b" * 16 + ".log": b"second",
    }
    records = _records(tmpdir, blobs)
    journals = [_journal(*(str(tmpdir / n) for n in blobs))]
    plan = preflight_assets(records, journals, tmpdir)
    assert [(p, b) for p, b in plan] == [
        (tmpdir / n, blob) for n, blob in sorted(blobs.items())
    ]
    # Preflight writes nothing.
    assert list(tmpdir.iterdir()) == []


def test_identical_existing_bytes_reused_no_plan_entry(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    name = "pi-bash-" + "a" * 16 + ".log"
    (tmpdir / name).write_bytes(b"already-there")
    records = _records(tmpdir, {name: b"already-there"})
    plan = preflight_assets(records, [_journal(str(tmpdir / name))], tmpdir)
    assert plan == []


def test_mismatching_existing_bytes_refuse(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    name = "pi-bash-" + "a" * 16 + ".log"
    (tmpdir / name).write_bytes(b"different")
    records = _records(tmpdir, {name: b"carried"})
    with pytest.raises(CapabilityUnavailable):
        preflight_assets(records, [_journal(str(tmpdir / name))], tmpdir)
    assert (tmpdir / name).read_bytes() == b"different"


def test_foreign_and_traversal_refs_refuse(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    name = "pi-bash-" + "a" * 16 + ".log"
    for bad in (str(tmpdir / "sub" / name), str(tmpdir.parent / name)):
        with pytest.raises(CapabilityUnavailable):
            preflight_assets(_records(tmpdir, {name: b"x"}), [_journal(bad)], tmpdir)
    # A traversal ref cannot be expressed as a canonical-manifest ref without
    # also failing the journal-ref set match, so probe the lexical gate via a
    # mismatched journal ref set.
    records = _records(tmpdir, {name: b"x"})
    traversal = str(tmpdir / ".." / name)
    with pytest.raises(CapabilityUnavailable):
        preflight_assets(records, [_journal(traversal)], tmpdir)


def test_symlink_destination_refuses(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    name = "pi-bash-" + "a" * 16 + ".log"
    victim = tmp_path / "victim"
    victim.write_bytes(b"victim")
    target = tmpdir / name
    try:
        os.symlink(victim, target)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable on this host")
    records = _records(tmpdir, {name: b"carried"})
    with pytest.raises(CapabilityUnavailable):
        preflight_assets(records, [_journal(str(target))], tmpdir)
    assert victim.read_bytes() == b"victim"


def test_restore_creates_absent_files_0600(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    name = "pi-bash-" + "a" * 16 + ".log"
    plan = [(tmpdir / name, b"restored")]
    created = restore_assets(plan)
    assert created == [tmpdir / name]
    assert (tmpdir / name).read_bytes() == b"restored"
    mode = stat.S_IMODE(os.stat(tmpdir / name).st_mode)
    if sys.platform != "win32":
        assert mode == 0o600


def test_restore_rejects_collision_without_writing(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    other = "pi-bash-" + "b" * 16 + ".log"
    (tmpdir / other).write_bytes(b"unrelated")
    victim = tmp_path / "victim"
    victim.write_bytes(b"victim")
    try:
        os.symlink(victim, tmpdir / other)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable on this host")
    plan = [(tmpdir / other, b"evil")]
    with pytest.raises(CapabilityUnavailable):
        restore_assets(plan)
    assert victim.read_bytes() == b"victim"


def test_restore_failure_rolls_back_own_creations(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    first = "pi-bash-" + "a" * 16 + ".log"
    second = "pi-bash-" + "b" * 16 + ".log"
    # A directory at the second path makes its O_EXCL open fail (EEXIST path
    # reused-identically only accepts regular files; a directory refuses).
    (tmpdir / second).mkdir()
    plan = [(tmpdir / first, b"one"), (tmpdir / second, b"two")]
    with pytest.raises(OSError, match=r"."):
        restore_assets(plan)
    assert not (tmpdir / first).exists()  # own creation rolled back
    assert (tmpdir / second).is_dir()  # pre-existing path untouched


def test_restore_identical_collision_reused(tmp_path) -> None:
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    name = "pi-bash-" + "a" * 16 + ".log"
    (tmpdir / name).write_bytes(b"same")
    created = restore_assets([(tmpdir / name, b"same")])
    assert created == []
    assert (tmpdir / name).read_bytes() == b"same"
