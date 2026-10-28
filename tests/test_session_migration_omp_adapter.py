"""Disposable-root tests for OMP adapter routing, exact bundle roundtrip,
scoped restart proof, gated export, project cwd authorization and bounded
reclaim blob reuse policy."""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from untether.session_migration.file_bundle_adapter import register_from_settings
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    assert_exact_restore,
    registered_adapter_engines,
    require_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_archive import (
    NativeArchive,
    decode_native,
    encode_native,
)
from untether.session_migration.omp_adapter import OmpPersistenceAdapter
from untether.session_migration.session_gate import GateRefused
from untether.session_migration.settings import (
    EnginePersistenceProfile,
    OmpPersistenceProfile,
    SessionMigrationSettings,
)


@pytest.fixture(autouse=True)
def gate_root(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "shared-gates"))
    return tmp_path / "shared-gates"


@pytest.fixture(autouse=True)
def isolated_registry():
    before = set(registered_adapter_engines())
    yield
    for engine in set(registered_adapter_engines()) - before:
        unregister_persistence_adapter(engine)


def make_store(root: Path):
    sessions = root / "sessions"
    blobs = root / "agent" / "blobs" / "data"
    sessions.mkdir(parents=True)
    blobs.mkdir(parents=True)
    return sessions, blobs


def omp_profile(sessions: Path, blobs: Path) -> OmpPersistenceProfile:
    return OmpPersistenceProfile(
        format="omp-v1",
        sessions_dir=str(sessions),
        blobs_dir=str(blobs),
        restart_argv=(
            sys.executable,
            "-c",
            "import json,sys;"
            "p=sys.argv[1];sid=sys.argv[2];"
            "first=open(p,encoding='utf8').readline();"
            "h=json.loads(first);"
            "assert h['type']=='session' and h['id']==sid",
            "{session_path}",
            "{session_id}",
        ),
    )


def omp_adapter(sessions: Path, blobs: Path, **kwargs) -> OmpPersistenceAdapter:
    return OmpPersistenceAdapter(
        engine="omp",
        profile=omp_profile(sessions, blobs),
        **kwargs,
    )


def journal_bytes(sid: str, cwd: str, *entries: dict) -> bytes:
    lines = [{"type": "session", "version": 3, "id": sid, "cwd": cwd}, *entries]
    return "".join(json.dumps(entry) + "\n" for entry in lines).encode()


def seed_journal(
    sessions: Path,
    blobs: Path,
    name: str,
    sid: str,
    cwd: str,
    blob_bytes: bytes | None = None,
) -> str | None:
    digest = None
    entry: dict = {"type": "message", "id": "m1", "text": "hello"}
    if blob_bytes is not None:
        digest = hashlib.sha256(blob_bytes).hexdigest()
        (blobs / digest).write_bytes(blob_bytes)
        entry = {
            "type": "message",
            "id": "m1",
            "content": [{"blobRef": "blob:sha256:" + digest}],
        }
    raw = journal_bytes(sid, cwd, entry)
    (sessions / name).write_bytes(raw)
    return digest


# -- profile validation -------------------------------------------------


def test_omp_profile_requires_absolute_dirs(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    for kwargs in (
        {"sessions_dir": "sessions", "blobs_dir": str(blobs)},
        {"sessions_dir": str(sessions), "blobs_dir": "blobs"},
    ):
        with pytest.raises(ValueError, match="absolute path required"):
            OmpPersistenceProfile(format="omp-v1", restart_argv=("x",), **kwargs)


# -- registration routing -----------------------------------------------


def test_register_routes_omp_engine_to_omp_adapter(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    settings = SessionMigrationSettings(
        enabled=True,
        peers={"p": _peer()},
        persistence={"engines": {"omp": omp_profile(sessions, blobs)}},
    )
    register_from_settings(settings, engine="omp")
    assert "omp" in registered_adapter_engines()
    adapter = require_persistence_adapter("omp")
    assert isinstance(adapter, OmpPersistenceAdapter)
    assert adapter.engine == "omp"


def test_register_wrong_engine_format_stays_with_file_bundle(tmp_path):
    sessions = tmp_path / "pi-store"
    sessions.mkdir()
    settings = SessionMigrationSettings(
        enabled=True,
        peers={"p": _peer()},
        persistence={
            "engines": {
                "pi": EnginePersistenceProfile(
                    format="pi-jsonl-v3",
                    sessions_dir=str(sessions),
                    session_record="pi",
                    restart_argv=(sys.executable, "-c", "print('ok')"),
                )
            }
        },
    )
    register_from_settings(settings, engine="pi")
    from untether.session_migration.file_bundle_adapter import (
        FileBundlePersistenceAdapter,
    )

    adapter = require_persistence_adapter("pi")
    assert isinstance(adapter, FileBundlePersistenceAdapter)
    assert not isinstance(adapter, OmpPersistenceAdapter)


def test_register_skips_engine_without_profile(tmp_path):
    settings = SessionMigrationSettings(enabled=True, peers={"p": _peer()})
    register_from_settings(settings, engine="omp")
    assert "omp" not in registered_adapter_engines()
    # No profile, no plugin: require must refuse, proving the no-op path.
    with pytest.raises(CapabilityUnavailable, match="ENGINE_NATIVE_PERSISTENCE"):
        require_persistence_adapter("omp")


def test_register_omp_profile_with_missing_store_refused_at_construction(tmp_path):
    settings = SessionMigrationSettings(
        enabled=True,
        peers={"p": _peer()},
        persistence={
            "engines": {
                "omp": OmpPersistenceProfile(
                    format="omp-v1",
                    sessions_dir=str(tmp_path / "absent-sessions"),
                    blobs_dir=str(tmp_path / "absent-blobs"),
                    restart_argv=("x",),
                )
            }
        },
    )
    register_from_settings(settings, engine="omp")
    with pytest.raises(CapabilityUnavailable, match="PERSISTENCE_STORE_NOT_READY"):
        require_persistence_adapter("omp")


# -- exact bundle export/import roundtrip --------------------------------


def test_export_collects_exact_bundle_with_blob_and_metadata(tmp_path):
    sessions, blobs = make_store(tmp_path / "src")
    name = "2026_journal_abc.jsonl"
    image = b"\x00\xffimage-bytes"
    seed_journal(sessions, blobs, name, "abc", "/work/demo", blob_bytes=image)
    (sessions / name[: -len(".jsonl")] / "sub").mkdir(parents=True)
    (sessions / name[: -len(".jsonl")] / "sub" / "tool.txt").write_bytes(b"artifact")
    engine = omp_adapter(sessions, blobs)
    archive = engine.export_exact("abc")
    assert archive.engine == "omp"
    assert archive.native_id == "abc"
    assert archive.lineage == "omp-v1:" + name
    assert archive.generation == 0
    assert archive.records["session.jsonl"] == (sessions / name).read_bytes()
    assert archive.records["artifacts/sub/tool.txt"] == b"artifact"
    assert archive.records["blobs/" + hashlib.sha256(image).hexdigest()] == image
    metadata = json.loads(archive.records["untether.json"])
    assert metadata == {
        "logical_id": "abc",
        "native_id": "abc",
        "generation": 0,
        "lineage": "omp-v1:" + name,
    }
    encoded = encode_native(
        engine="omp",
        logical_id=archive.logical_id,
        native_id=archive.native_id,
        generation=archive.generation,
        lineage=archive.lineage,
        records=archive.records,
        max_bytes=1 << 20,
    )
    decoded = decode_native(encoded, max_archive=1 << 20, max_expanded=1 << 20)
    assert (
        decoded.engine,
        decoded.logical_id,
        decoded.native_id,
        decoded.generation,
        decoded.lineage,
        decoded.records,
    ) == (
        archive.engine,
        archive.logical_id,
        archive.native_id,
        archive.generation,
        archive.lineage,
        archive.records,
    )  # codec fidelity incl. per-record manifest hashes; digest is wire-scoped


def test_export_unknown_session_refused(tmp_path):
    sessions, blobs = make_store(tmp_path / "src")
    seed_journal(sessions, blobs, "a.jsonl", "known", "/work")
    engine = omp_adapter(sessions, blobs)
    with pytest.raises(CapabilityUnavailable, match="SESSION_NOT_FOUND"):
        engine.export_exact("missing")
    (sessions / "b.jsonl").write_bytes(journal_bytes("known", "/elsewhere"))
    with pytest.raises(CapabilityUnavailable, match="SESSION_LOOKUP_AMBIGUOUS"):
        engine.export_exact("known")


def test_export_requires_gate_root(tmp_path, monkeypatch):
    sessions, blobs = make_store(tmp_path / "src")
    seed_journal(sessions, blobs, "a.jsonl", "abc", "/work")
    engine = omp_adapter(sessions, blobs)
    monkeypatch.delenv("UNTETHER_SESSION_GATE_ROOT")
    with pytest.raises(CapabilityUnavailable, match="SOURCE_GATE_UNCONFIGURED"):
        engine.export_exact("abc")


def test_export_locked_requires_owned_migration_lock(tmp_path):
    sessions, blobs = make_store(tmp_path / "src")
    seed_journal(sessions, blobs, "a.jsonl", "abc", "/work")
    engine = omp_adapter(sessions, blobs)
    from untether.session_migration.session_gate import migration_lock

    with pytest.raises(GateRefused, match="MIGRATION_LOCK_NOT_OWNED"):
        engine.export_locked("abc")
    with migration_lock(root=_gate_root(), engine_id="omp", native_id="abc"):
        engine.export_locked("abc")  # owned: succeeds


def test_import_roundtrip_preserves_identity_and_bytes(tmp_path):
    src_sessions, src_blobs = make_store(tmp_path / "src")
    name = "2026_journal_abc.jsonl"
    image = b"\x00\xffimage-bytes"
    seed_journal(src_sessions, src_blobs, name, "abc", "/work/demo", image)
    (src_sessions / name[: -len(".jsonl")] / "sub").mkdir(parents=True)
    (src_sessions / name[: -len(".jsonl")] / "sub" / "tool.txt").write_bytes(
        b"artifact"
    )
    source = omp_adapter(src_sessions, src_blobs)
    dst_sessions, dst_blobs = make_store(tmp_path / "dst")
    target = omp_adapter(dst_sessions, dst_blobs)
    archive = source.export_exact("abc")
    proof = target.import_exact(archive)
    assert_exact_restore(archive, proof)
    assert proof.restarted and proof.resumed
    restored_name = archive.lineage[len("omp-v1:") :]
    assert (dst_sessions / restored_name).read_bytes() == archive.records[
        "session.jsonl"
    ]
    assert (dst_blobs / hashlib.sha256(image).hexdigest()).read_bytes() == image
    assert (
        dst_sessions / restored_name[: -len(".jsonl")] / "sub" / "tool.txt"
    ).read_bytes() == b"artifact"
    reexported = target.export_exact("abc")
    assert reexported.lineage == archive.lineage
    assert reexported.records["session.jsonl"] == archive.records["session.jsonl"]
    with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
        target.import_exact(archive)


def test_import_refuses_wrong_engine_and_forged_metadata(tmp_path):
    sessions, blobs = make_store(tmp_path / "dst")
    engine = omp_adapter(sessions, blobs)
    good = NativeArchive(
        engine="omp",
        logical_id="abc",
        native_id="abc",
        generation=3,
        lineage="omp-v1:j.jsonl",
        records={
            "session.jsonl": journal_bytes("abc", "/work"),
            "untether.json": json.dumps(
                {
                    "logical_id": "abc",
                    "native_id": "abc",
                    "generation": 3,
                    "lineage": "omp-v1:j.jsonl",
                }
            ).encode(),
        },
        digest="0" * 64,
    )
    proof = engine.import_exact(good)
    assert proof.generation == 3
    assert engine.export_exact("abc").generation == 3
    forged = replace(good, generation=9)
    with pytest.raises(CapabilityUnavailable, match="ARCHIVE_IDENTITY_INVALID"):
        engine.import_exact(forged)
    with pytest.raises(CapabilityUnavailable, match="ENGINE_MISMATCH"):
        engine.import_exact(replace(good, engine="other"))
    truncated = replace(good, records={"untether.json": good.records["untether.json"]})
    with pytest.raises(CapabilityUnavailable, match="RECORD_LAYOUT_REFUSED"):
        engine.import_exact(truncated)


def test_import_failed_probe_rolls_back_journal_and_metadata(tmp_path):
    src_sessions, src_blobs = make_store(tmp_path / "src")
    seed_journal(src_sessions, src_blobs, "j.jsonl", "abc", "/work")
    source = omp_adapter(src_sessions, src_blobs)
    archive = source.export_exact("abc")
    dst_sessions, dst_blobs = make_store(tmp_path / "dst")
    target = omp_adapter(
        dst_sessions,
        dst_blobs,
        restart_runner=lambda argv: (_ for _ in ()).throw(
            CapabilityUnavailable("RESTART_PROBE_FAILED")
        ),
    )
    with pytest.raises(CapabilityUnavailable, match="RESTART_PROBE_FAILED"):
        target.import_exact(archive)
    assert not (dst_sessions / "j.jsonl").exists()
    assert not (dst_sessions / ".untether-migration" / "abc.json").exists()


def test_unscoped_probe_refused(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    engine = omp_adapter(sessions, blobs)
    engine = replace(
        engine,
        profile=engine.profile.model_copy(update={"restart_argv": ("echo", "hi")}),
    )
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROBE_UNSCOPED"):
        engine._run_restart_probe("sid", sessions / "j.jsonl")


def test_probe_receives_scoped_substituted_argv(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    engine = omp_adapter(sessions, blobs)
    seen = {}
    engine = replace(
        engine,
        run_argv=lambda argv, timeout: seen.update(argv=list(argv), timeout=timeout),
    )
    engine._run_restart_probe("sid", sessions / "j.jsonl")
    assert seen["timeout"] == engine.profile.restart_timeout_s
    assert sessions / "j.jsonl" == Path(seen["argv"][-2])
    assert seen["argv"][-1] == "sid"
    assert not any("{" in arg for arg in seen["argv"])


# -- project authorization ----------------------------------------------


def test_lookup_for_project_authorizes_exact_header_cwd(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    seed_journal(sessions, blobs, "mine.jsonl", "abc", "/work/demo")
    seed_journal(sessions, blobs, "other.jsonl", "xyz", "/elsewhere")
    engine = omp_adapter(sessions, blobs)
    assert engine.lookup_for_project("abc", PurePosixPath("/work/demo")) == "abc"
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        engine.lookup_for_project("xyz", PurePosixPath("/work/demo"))
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        engine.lookup_for_project("abc", PurePosixPath("/work/other"))


def test_windows_cwd_matches_drive_case_insensitively(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    seed_journal(sessions, blobs, "win.jsonl", "abc", "D:\\Work\\Demo")
    engine = omp_adapter(sessions, blobs)
    assert engine.lookup_for_project("abc", PureWindowsPath("d:/Work/Demo")) == "abc"


def title_slot() -> bytes:
    slot = {
        "type": "title",
        "v": 1,
        "title": "Native title",
        "updatedAt": "",
        "pad": "",
    }
    raw = json.dumps(slot).encode()
    slot["pad"] = " " * (255 - len(raw))
    result = json.dumps(slot).encode() + b"\n"
    assert len(result) == 256
    return result


def test_native_title_slot_lookup_and_export(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    raw = title_slot() + journal_bytes("abc", "/work/demo")
    (sessions / "native.jsonl").write_bytes(raw)
    engine = omp_adapter(sessions, blobs)
    assert engine.lookup_exact("abc") == "abc"
    assert engine.lookup_for_project("abc", PurePosixPath("/work/demo")) == "abc"
    assert engine.export_exact("abc").records["session.jsonl"] == raw
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        engine.lookup_for_project("abc", PurePosixPath("/work"))
    # A second identity is ambiguous even when its cwd is not authorised.
    (sessions / "duplicate.jsonl").write_bytes(
        title_slot() + journal_bytes("abc", "/elsewhere")
    )
    with pytest.raises(CapabilityUnavailable, match="SESSION_LOOKUP_AMBIGUOUS"):
        engine.lookup_exact("abc")
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        engine.lookup_for_project("abc", PurePosixPath("/work/demo"))


@pytest.mark.parametrize(
    "raw",
    [
        title_slot()[:-1] + b" \n" + journal_bytes("abc", "/work"),
        title_slot() + b" " * 65537,
        title_slot(),
    ],
    ids=["bad-slot-length", "oversized-header", "missing-header"],
)
def test_native_title_slot_refuses_malformed_or_unbounded_header(tmp_path, raw):
    sessions, blobs = make_store(tmp_path / "store")
    (sessions / "bad.jsonl").write_bytes(raw)
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_MALFORMED"):
        omp_adapter(sessions, blobs).lookup_exact("abc")


def test_native_title_slot_does_not_bypass_unsafe_file_rejection(tmp_path):
    sessions, blobs = make_store(tmp_path / "store")
    (sessions / "native.jsonl").write_bytes(
        title_slot() + journal_bytes("abc", "/work")
    )
    try:
        (sessions / "unsafe.jsonl").symlink_to(sessions / "native.jsonl")
    except OSError:
        pytest.skip("symlink creation unavailable")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_UNSAFE"):
        omp_adapter(sessions, blobs).lookup_exact("abc")


# -- bounded reclaim overwrite and blob reuse ----------------------------


def _evolved_journal(sid: str, cwd: str) -> bytes:
    return journal_bytes(
        sid,
        cwd,
        {"type": "message", "id": "m2", "text": "evolved"},
    )


def _overwrite_payload(
    prior: NativeArchive,
    *,
    generation: int,
    lineage: str,
    session: bytes,
    blob_bytes: bytes | None,
    extra_records: dict[str, bytes] | None = None,
) -> dict[str, bytes]:
    records = dict(extra_records or {})
    records["session.jsonl"] = session
    if blob_bytes is not None:
        digest = hashlib.sha256(blob_bytes).hexdigest()
        records["blobs/" + digest] = blob_bytes
    metadata = {
        "logical_id": prior.native_id,
        "native_id": prior.native_id,
        "generation": generation,
        "lineage": lineage,
    }
    records["untether.json"] = json.dumps(metadata).encode()
    return records


def test_import_overwrite_reuses_identical_shared_blob(tmp_path):
    sessions, blobs = make_store(tmp_path / "dst")
    shared = b"\x00\xffshared-image"
    digest = hashlib.sha256(shared).hexdigest()
    seed_journal(sessions, blobs, "j.jsonl", "abc", "/work", blob_bytes=shared)
    engine = omp_adapter(sessions, blobs)
    prior = engine.export_exact("abc")
    evolved = _evolved_journal("abc", "/work")
    records = _overwrite_payload(
        prior,
        generation=1,
        lineage=prior.lineage,
        session=evolved,
        blob_bytes=shared,  # identical blob already present -> reuse, no collision
    )
    hashes = engine.import_overwrite(
        engine="omp",
        native_id="abc",
        records=records,
        expected_prior_records={"session.jsonl": prior.records["session.jsonl"]},
        generation=1,
        lineage=prior.lineage,
    )
    assert set(hashes) == set(records)
    assert hashes["session.jsonl"] == hashlib.sha256(evolved).hexdigest()
    assert (
        hashes["untether.json"] == hashlib.sha256(records["untether.json"]).hexdigest()
    )
    assert (blobs / digest).read_bytes() == shared  # reused, byte-identical
    assert (sessions / "j.jsonl").read_bytes() == evolved
    assert engine.export_exact("abc").generation == 1


def test_import_overwrite_refuses_divergent_shared_blob_as_gap(tmp_path):
    sessions, blobs = make_store(tmp_path / "dst")
    shared = b"\x00\xffshared-image"
    digest = hashlib.sha256(shared).hexdigest()
    seed_journal(sessions, blobs, "j.jsonl", "abc", "/work", blob_bytes=shared)
    engine = omp_adapter(sessions, blobs)
    prior = engine.export_exact("abc")
    evolved = _evolved_journal("abc", "/work")
    records = _overwrite_payload(
        prior,
        generation=1,
        lineage=prior.lineage,
        session=evolved,
        blob_bytes=shared,
    )
    # Same digest, divergent bytes in the shared store: bounded safe
    # overwrite does not fit; the reclaim must refuse, not delete/overwrite.
    (blobs / digest).write_bytes(b"\x00\xffTAMPERED")
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_BLOB_GAP"):
        engine.import_overwrite(
            engine="omp",
            native_id="abc",
            records=records,
            expected_prior_records={"session.jsonl": prior.records["session.jsonl"]},
            generation=1,
            lineage=prior.lineage,
        )
    # Refusal kept the prior bytes and the shared blob untouched.
    assert (blobs / digest).read_bytes() == b"\x00\xffTAMPERED"
    assert (sessions / "j.jsonl").read_bytes() == prior.records["session.jsonl"]


def test_import_overwrite_writes_absent_blob_and_refuses_prior_mismatch(tmp_path):
    sessions, blobs = make_store(tmp_path / "dst")
    seed_journal(sessions, blobs, "j.jsonl", "abc", "/work")
    engine = omp_adapter(sessions, blobs)
    prior = engine.export_exact("abc")
    evolved = _evolved_journal("abc", "/work")
    fresh = b"new-blob-bytes"
    records = _overwrite_payload(
        prior,
        generation=1,
        lineage=prior.lineage,
        session=evolved,
        blob_bytes=fresh,
    )
    with pytest.raises(CapabilityUnavailable, match="OVERWRITE_PRIOR_MISMATCH"):
        engine.import_overwrite(
            engine="omp",
            native_id="abc",
            records=records,
            expected_prior_records={"session.jsonl": b"divergent"},
            generation=1,
            lineage=prior.lineage,
        )
    engine.import_overwrite(
        engine="omp",
        native_id="abc",
        records=records,
        expected_prior_records={"session.jsonl": prior.records["session.jsonl"]},
        generation=1,
        lineage=prior.lineage,
    )
    assert (blobs / hashlib.sha256(fresh).hexdigest()).read_bytes() == fresh


def test_import_overwrite_idempotent_retry_with_target_bytes(tmp_path):
    sessions, blobs = make_store(tmp_path / "dst")
    seed_journal(sessions, blobs, "j.jsonl", "abc", "/work")
    engine = omp_adapter(sessions, blobs)
    prior = engine.export_exact("abc")
    evolved = _evolved_journal("abc", "/work")
    records = _overwrite_payload(
        prior,
        generation=1,
        lineage=prior.lineage,
        session=evolved,
        blob_bytes=None,
    )
    engine.import_overwrite(
        engine="omp",
        native_id="abc",
        records=records,
        expected_prior_records={"session.jsonl": prior.records["session.jsonl"]},
        generation=1,
        lineage=prior.lineage,
    )
    engine.import_overwrite(
        engine="omp",
        native_id="abc",
        records=records,
        expected_prior_records={"session.jsonl": prior.records["session.jsonl"]},
        generation=1,
        lineage=prior.lineage,
    )  # current == target bytes: retry succeeds without writes
    assert (sessions / "j.jsonl").read_bytes() == evolved


def test_import_overwrite_refuses_foreign_engine_and_identity(tmp_path):
    sessions, blobs = make_store(tmp_path / "dst")
    seed_journal(sessions, blobs, "j.jsonl", "abc", "/work")
    engine = omp_adapter(sessions, blobs)
    prior = engine.export_exact("abc")
    evolved = _evolved_journal("abc", "/work")
    records = _overwrite_payload(
        prior,
        generation=1,
        lineage=prior.lineage,
        session=evolved,
        blob_bytes=None,
    )
    with pytest.raises(CapabilityUnavailable, match="ENGINE_MISMATCH"):
        engine.import_overwrite(
            engine="other",
            native_id="abc",
            records=records,
            expected_prior_records={"session.jsonl": prior.records["session.jsonl"]},
            generation=1,
            lineage=prior.lineage,
        )
    with pytest.raises(CapabilityUnavailable, match="ARCHIVE_IDENTITY_INVALID"):
        engine.import_overwrite(
            engine="omp",
            native_id="abc",
            records=records,
            expected_prior_records={"session.jsonl": prior.records["session.jsonl"]},
            generation=2,
            lineage=prior.lineage,
        )


@pytest.mark.parametrize("overwrite", [False, True], ids=["exact", "overwrite"])
@pytest.mark.parametrize(
    "record", ["artifact", "blob", "unreferenced-blob", "metadata", "journal"]
)
@pytest.mark.parametrize("action", ["mutate", "delete"])
def test_import_refuses_records_changed_by_restart_probe(
    tmp_path, overwrite, record, action
):
    src_sessions, src_blobs = make_store(tmp_path / "src")
    image = b"referenced-image"
    digest = seed_journal(src_sessions, src_blobs, "j.jsonl", "abc", "/work", image)
    artifact = src_sessions / "j" / "sub" / "tool.txt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"artifact")
    archive = omp_adapter(src_sessions, src_blobs).export_exact("abc")
    orphan = b"unreferenced-image"
    orphan_digest = hashlib.sha256(orphan).hexdigest()
    archive = replace(
        archive, records={**archive.records, "blobs/" + orphan_digest: orphan}
    )
    sessions, blobs = make_store(tmp_path / "dst")
    paths = {
        "artifact": sessions / "j" / "sub" / "tool.txt",
        "blob": blobs / str(digest),
        "unreferenced-blob": blobs / orphan_digest,
        "metadata": sessions / ".untether-migration" / "abc.json",
        "journal": sessions / "j.jsonl",
    }
    calls = []

    def restart(argv):
        calls.append(argv)
        path = paths[record]
        assert path.is_file()
        if action == "delete":
            path.unlink()
        else:
            path.write_bytes(path.read_bytes() + b"tampered")

    engine = omp_adapter(sessions, blobs, restart_runner=restart)
    with pytest.raises(CapabilityUnavailable, match="EXACT_RESTORE_UNVERIFIED"):
        if overwrite:
            seed_journal(sessions, blobs, "j.jsonl", "abc", "/work")
            engine.import_overwrite(
                engine="omp",
                native_id="abc",
                records=archive.records,
                expected_prior_records={
                    "session.jsonl": (sessions / "j.jsonl").read_bytes()
                },
                generation=archive.generation,
                lineage=archive.lineage,
            )
        else:
            engine.import_exact(archive)
    assert len(calls) == 1
    if not overwrite:
        assert not (sessions / "j.jsonl").exists()
        assert not (sessions / ".untether-migration" / "abc.json").exists()


@pytest.mark.parametrize("overwrite", [False, True], ids=["exact", "overwrite"])
def test_import_verifies_artifact_and_unreferenced_blob_after_probe(
    tmp_path, overwrite
):
    src_sessions, src_blobs = make_store(tmp_path / "src")
    seed_journal(src_sessions, src_blobs, "j.jsonl", "abc", "/work")
    artifact = src_sessions / "j" / "sub" / "tool.txt"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"artifact")
    archive = omp_adapter(src_sessions, src_blobs).export_exact("abc")
    orphan = b"unreferenced-image"
    digest = hashlib.sha256(orphan).hexdigest()
    archive = replace(archive, records={**archive.records, "blobs/" + digest: orphan})
    sessions, blobs = make_store(tmp_path / "dst")
    calls = []
    engine = omp_adapter(sessions, blobs, restart_runner=calls.append)
    if overwrite:
        seed_journal(sessions, blobs, "j.jsonl", "abc", "/work")
        hashes = engine.import_overwrite(
            engine="omp",
            native_id="abc",
            records=archive.records,
            expected_prior_records={
                "session.jsonl": (sessions / "j.jsonl").read_bytes()
            },
            generation=archive.generation,
            lineage=archive.lineage,
        )
        assert hashes == {
            name: hashlib.sha256(data).hexdigest()
            for name, data in archive.records.items()
        }
    else:
        assert_exact_restore(archive, engine.import_exact(archive))
    assert len(calls) == 1
    assert (sessions / "j" / "sub" / "tool.txt").read_bytes() == b"artifact"
    assert (blobs / digest).read_bytes() == orphan


# -- helpers ---------------------------------------------------------


def _gate_root() -> Path:
    import os

    return Path(os.environ["UNTETHER_SESSION_GATE_ROOT"])


def _peer():
    from untether.session_migration.settings import MigrationPeer

    return MigrationPeer(
        host="example.invalid",
        user="u",
        identity_dir="d",
        identity_file="id",
        known_hosts_file="kh",
        host_key_fingerprint="SHA256:" + "A" * 43,
        receiver_path="/recv",
        expected_endpoint_id="e",
        destination_binding="b",
        projects={"demo": "/work/demo"},
    )
