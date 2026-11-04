"""Importer side of the pi parent-journal bundle: parents first, then root.

The archives here are produced by the adapter's own ``export_exact`` (a real
child -> parent -> grandparent chain), so the import path is exercised against
the exact record pairing the exporter emits. Every failure case asserts zero or
strictly-bounded writes into the destination store.
"""

import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from untether.session_migration import file_bundle_adapter as bundle
from untether.session_migration.file_bundle_adapter import FileBundlePersistenceAdapter
from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.native_archive import NativeArchive
from untether.session_migration.settings import EnginePersistenceProfile


@pytest.fixture(autouse=True)
def configured_gate(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "shared-gates"))


def _journal_bytes(sid: str, parent: str | Path | None = None) -> bytes:
    header = {"type": "session", "version": 3, "id": sid}
    if parent is not None:
        header["parentSession"] = str(parent)
    return (json.dumps(header) + "\n{}\n").encode()


def _journal(path: Path, sid: str, parent: str | Path | None = None) -> Path:
    path.write_bytes(_journal_bytes(sid, parent))
    return path


def _parents_bytes(*names: str) -> bytes:
    return json.dumps({"version": 1, "journals": list(names)}, sort_keys=True).encode()


def adapter(root, **overrides):
    root.mkdir(mode=0o700, exist_ok=True)
    profile = EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(root),
        session_record="pi",
        restart_argv=(
            sys.executable,
            "-c",
            "import sys",
            "{session_id}",
            "{session_path}",
        ),
    )
    engine = FileBundlePersistenceAdapter("pi", profile, root)
    return replace(engine, **overrides) if overrides else engine


def _chain(tmp_path):
    """Real source store with a child -> parent -> grandparent lineage."""
    source = adapter(tmp_path / "source")
    gp = _journal(source.sessions_dir / "gp.jsonl", "gp")
    parent = _journal(source.sessions_dir / "parent.jsonl", "parent", gp)
    child = _journal(source.sessions_dir / "child.jsonl", "child", parent)
    return source, child, parent, gp


def test_import_child_parent_grandparent_restores_exact_bytes(tmp_path):
    source = adapter(tmp_path / "source")
    target = adapter(tmp_path / "target")
    gp = _journal(source.sessions_dir / "gp.jsonl", "gp")
    parent = _journal(source.sessions_dir / "parent.jsonl", "parent", gp)
    # Timestamped root basename: the layout record carries it and the parent
    # chain is carried alongside it.
    child = _journal(
        source.sessions_dir / "2026-01-02T03-04-05-child.jsonl", "child", parent
    )
    archive = source.export_exact("child")
    assert set(archive.records) == {
        "session.jsonl",
        "layout.json",
        "parent-1.jsonl",
        "parent-2.jsonl",
        "parents.json",
    }
    proof = target.import_exact(archive)
    assert proof.native_id == "child" and proof.restarted and proof.resumed
    assert proof.record_hashes == {
        name: hashlib.sha256(blob).hexdigest() for name, blob in archive.records.items()
    }
    # The child journal is byte-identical: no header rewrite, no pointer claim.
    assert (target.sessions_dir / child.name).read_bytes() == child.read_bytes()
    assert (target.sessions_dir / "parent.jsonl").read_bytes() == parent.read_bytes()
    assert (target.sessions_dir / "gp.jsonl").read_bytes() == gp.read_bytes()
    assert not (target.sessions_dir / "child.jsonl").exists()


def test_import_reuses_identical_existing_parent_and_never_owns_it(tmp_path):
    source, _, parent, _ = _chain(tmp_path)
    target = adapter(tmp_path / "target")
    archive = source.export_exact("child")
    reused = target.sessions_dir / "parent.jsonl"
    reused.write_bytes(parent.read_bytes())
    before = reused.stat().st_mtime_ns
    assert target.import_exact(archive).native_id == "child"
    assert reused.stat().st_mtime_ns == before  # untouched, not rewritten
    assert reused.read_bytes() == parent.read_bytes()


def test_import_foreign_parent_bytes_refused_with_zero_writes(tmp_path):
    source, _, _, _ = _chain(tmp_path)
    target = adapter(tmp_path / "target")
    archive = source.export_exact("child")
    foreign = _journal(target.sessions_dir / "parent.jsonl", "someone-else")
    with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
        target.import_exact(archive)
    assert foreign.read_bytes() == _journal_bytes("someone-else")
    assert [p.name for p in target.sessions_dir.iterdir()] == ["parent.jsonl"]


def test_import_parent_destination_directory_refused_with_zero_writes(tmp_path):
    source, _, _, _ = _chain(tmp_path)
    target = adapter(tmp_path / "target")
    archive = source.export_exact("child")
    (target.sessions_dir / "parent.jsonl").mkdir()
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_UNSAFE"):
        target.import_exact(archive)
    assert [p.name for p in target.sessions_dir.iterdir()] == ["parent.jsonl"]


def test_import_parent_destination_symlink_refused_with_zero_writes(tmp_path):
    source, _, parent, _ = _chain(tmp_path)
    target = adapter(tmp_path / "target")
    archive = source.export_exact("child")
    outside = tmp_path / "outside.jsonl"
    outside.write_bytes(parent.read_bytes())
    link = target.sessions_dir / "parent.jsonl"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("symlink creation unavailable")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_UNSAFE"):
        target.import_exact(archive)
    assert outside.read_bytes() == parent.read_bytes()
    assert [p.name for p in target.sessions_dir.iterdir()] == ["parent.jsonl"]


def test_probe_failure_cleans_created_parents_and_keeps_reused(tmp_path):
    source, child, parent, _ = _chain(tmp_path)
    target = adapter(tmp_path / "target")
    archive = source.export_exact("child")
    reused = target.sessions_dir / "parent.jsonl"
    reused.write_bytes(parent.read_bytes())

    def boom(argv):
        raise CapabilityUnavailable("RESTART_PROBE_FAILED")

    with pytest.raises(CapabilityUnavailable, match="RESTART_PROBE_FAILED"):
        replace(target, restart_runner=boom).import_exact(archive)
    # Only the reused (never owned) parent survives the rollback.
    assert [p.name for p in target.sessions_dir.iterdir() if p.is_file()] == [
        "parent.jsonl"
    ]
    assert reused.read_bytes() == parent.read_bytes()
    assert not (target.sessions_dir / "gp.jsonl").exists()
    assert not (target.sessions_dir / child.name).exists()
    assert not (target.sessions_dir / ".untether-migration" / "child.json").exists()


def _archive(records: dict[str, bytes]) -> NativeArchive:
    return NativeArchive(
        engine="pi",
        logical_id="child",
        native_id="child",
        generation=0,
        lineage="lineage",
        records=records,
        digest=hashlib.sha256(b"".join(sorted(records.values()))).hexdigest(),
    )


@pytest.mark.parametrize(
    ("extra", "match"),
    [
        ({"parents.json": _parents_bytes("parent.jsonl")}, "PARENT_REF_MISSING"),
        (
            {
                "parents.json": _parents_bytes("../x.jsonl"),
                "parent-1.jsonl": _journal_bytes("parent"),
            },
            "PARENTS_RECORD_REFUSED",
        ),
        (
            {
                "parents.json": _parents_bytes("a.jsonl"),
                "parent-1.jsonl": _journal_bytes("a"),
            },
            "PARENTS_CHAIN_MISMATCH",
        ),
        (
            {
                "parents.json": _parents_bytes("parent.jsonl"),
                "parent-1.jsonl": _journal_bytes("parent", "also.jsonl"),
            },
            "PARENT_CHAIN_INCOMPLETE",
        ),
        (
            {
                "parents.json": _parents_bytes("parent.jsonl"),
                "parent-1.jsonl": _journal_bytes("child"),
            },
            "PARENT_REF_CYCLE",
        ),
        (
            {
                "parents.json": _parents_bytes("child.jsonl"),
                "parent-1.jsonl": _journal_bytes("parent"),
            },
            "PARENTS_RECORD_REFUSED",
        ),
        (
            {
                "parents.json": _parents_bytes("parent.jsonl"),
                "parent-1.jsonl": _journal_bytes("parent"),
                "parent-2.jsonl": _journal_bytes("gp"),
            },
            "PARENT_RECORD_UNEXPECTED",
        ),
    ],
)
def test_malformed_parent_archive_writes_nothing(tmp_path, extra, match):
    target = adapter(tmp_path / "target")
    records = {"session.jsonl": _journal_bytes("child", "parent.jsonl")}
    records.update(extra)
    with pytest.raises(CapabilityUnavailable, match=match):
        target.import_exact(_archive(records))
    assert list(target.sessions_dir.iterdir()) == []


def _record_writes(monkeypatch):
    """Count _write_record calls; the stub never touches the store."""
    calls = []
    monkeypatch.setattr(bundle, "_write_record", lambda *a, **k: calls.append((a, k)))
    return calls


def test_import_late_parent_foreign_collision_writes_nothing(tmp_path, monkeypatch):
    source, _, _, _ = _chain(tmp_path)
    target = adapter(tmp_path / "target")
    archive = source.export_exact("child")
    # First parent destination is absent, the second already holds foreign
    # bytes: preflight must refuse before the first parent is created.
    foreign = _journal(target.sessions_dir / "gp.jsonl", "someone-else")
    assert not (target.sessions_dir / "parent.jsonl").exists()
    calls = _record_writes(monkeypatch)
    with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
        target.import_exact(archive)
    assert calls == []
    assert foreign.read_bytes() == _journal_bytes("someone-else")
    assert [p.name for p in target.sessions_dir.iterdir()] == ["gp.jsonl"]


def test_import_root_collision_preflighted_before_parent_writes(tmp_path, monkeypatch):
    source, _, _, _ = _chain(tmp_path)
    target = adapter(tmp_path / "target")
    archive = source.export_exact("child")
    # Root destination collides while both parents are absent: the root
    # preflight must refuse before either parent is created.
    foreign = _journal(target.sessions_dir / "child.jsonl", "someone-else")
    calls = _record_writes(monkeypatch)
    with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
        target.import_exact(archive)
    assert calls == []
    assert foreign.read_bytes() == _journal_bytes("someone-else")
    assert [p.name for p in target.sessions_dir.iterdir()] == ["child.jsonl"]
