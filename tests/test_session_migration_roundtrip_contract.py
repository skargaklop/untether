"""Roundtrip identity contract for the file-bundle persistence adapter.

Characterization only (tests-only scope): full identity — engine, logical_id,
native_id, records, digest, generation, lineage — must survive
source export -> disjoint-destination import -> destination re-export.
Lineage is filename-derived inside each store, so these tests pin the actual
behavior, including the mismatch when the source record filename differs from
the destination's deterministic ``<id>.jsonl`` name.
"""

import hashlib
import json
import sys
from dataclasses import replace

import pytest

from untether.session_migration.file_bundle_adapter import FileBundlePersistenceAdapter
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    assert_exact_restore,
)
from untether.session_migration.settings import EnginePersistenceProfile


@pytest.fixture(autouse=True)
def explicit_gate_root(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "explicit-gates"))


def adapter(root, **overrides):
    root.mkdir(mode=0o700, exist_ok=True)
    profile = EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(root),
        session_record="pi",
        restart_argv=(
            sys.executable,
            "-c",
            "import json,sys; p=sys.argv[1]; sid=sys.argv[2];"
            " assert json.loads(open(p, encoding='utf8').readline())['id']==sid",
            "{session_path}",
            "{session_id}",
        ),
    )
    engine = FileBundlePersistenceAdapter("pi", profile, root)
    return replace(engine, **overrides) if overrides else engine


def session_bytes(sid: str, text: str) -> bytes:
    return (
        json.dumps({"type": "session", "version": 3, "id": sid})
        + f'\n{{"type":"message","text":"{text}"}}\n'
    ).encode()


def test_roundtrip_preserves_full_identity_across_disjoint_stores(tmp_path):
    source = adapter(tmp_path / "source")
    dest = adapter(tmp_path / "dest", restart_runner=lambda argv: None)
    sid = "migrate-me"
    (source.sessions_dir / f"2026_evolved_{sid}.jsonl").write_bytes(
        session_bytes(sid, "original")
    )
    exported = source.export_exact(sid)
    assert exported.generation == 0
    proof = dest.import_exact(exported)
    # Import proof must echo the exporter's identity verbatim.
    assert_exact_restore(exported, proof)
    assert proof.lineage == exported.lineage
    assert proof.generation == exported.generation
    assert proof.record_hashes == {
        name: hashlib.sha256(blob).hexdigest()
        for name, blob in exported.records.items()
    }
    # Destination re-export must reproduce the full exported identity.
    reexported = dest.export_exact(sid)
    assert reexported == exported


def test_identity_preserved_only_when_source_filename_is_deterministic(tmp_path):
    """Control: with the degenerate ``<id>.jsonl`` source name, lineage is
    deterministic and full identity survives. Isolates the filename-derived
    lineage as the sole cause of the mismatch above."""
    source = adapter(tmp_path / "source")
    dest = adapter(tmp_path / "dest", restart_runner=lambda argv: None)
    sid = "same-name"
    (source.sessions_dir / f"{sid}.jsonl").write_bytes(session_bytes(sid, "v1"))
    exported = source.export_exact(sid)
    dest.import_exact(exported)
    assert dest.export_exact(sid) == exported


def test_second_evolved_roundtrip_same_id_conflict_and_completed_fence(tmp_path):
    from untether.session_migration.source_fence import place_source_fence

    source = adapter(tmp_path / "source")
    dest = adapter(tmp_path / "dest", restart_runner=lambda argv: None)
    sid = "evolved"
    v1 = session_bytes(sid, "first")
    (source.sessions_dir / f"{sid}.jsonl").write_bytes(v1)
    dest.import_exact(source.export_exact(sid))

    # The session evolves at the source under the same native id.
    v2 = session_bytes(sid, "second")
    (source.sessions_dir / f"{sid}.jsonl").write_bytes(v2)
    evolved = source.export_exact(sid)
    assert evolved.records["session.jsonl"] == v2

    # Destination still holds v1: re-import of the same id must fail closed,
    # leaving the stored record untouched (no overwrite, no partial write).
    with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
        dest.import_exact(evolved)
    assert (dest.sessions_dir / f"{sid}.jsonl").read_bytes() == v1

    # A completed transfer flips the pending source fence to ``completed``:
    # the tombstone remains and further export of the same id must refuse.
    from uuid import uuid4

    from untether.session_migration.source_fence import complete_source_fence

    fences = tmp_path / "explicit-gates" / "fences"
    fences.mkdir(parents=True, exist_ok=True)
    operation_id = str(uuid4())
    package_sha256 = hashlib.sha256(v2).hexdigest()
    place_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        operation_id=operation_id,
        package_sha256=package_sha256,
    )
    completed = complete_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        operation_id=operation_id,
        package_sha256=package_sha256,
    )
    assert completed.state == "completed"
    from untether.session_migration.source_fence import SourceFenced

    with pytest.raises(SourceFenced, match="state completed"):
        source.export_exact(sid)
