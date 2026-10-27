import json
import os
import sys
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from untether.session_migration.file_bundle_adapter import FileBundlePersistenceAdapter
from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.settings import EnginePersistenceProfile


@pytest.fixture(autouse=True)
def configured_gate(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "shared-gates"))


def adapter(root):
    root.mkdir(mode=0o700, exist_ok=True)
    profile = EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(root),
        session_record="pi",
        restart_argv=(
            sys.executable,
            "-c",
            "import json,sys; p=sys.argv[1]; sid=sys.argv[2]; assert json.loads(open(p,encoding='utf8').readline())['id']==sid",
            "{session_path}",
            "{session_id}",
        ),
    )
    return FileBundlePersistenceAdapter("pi", profile, root)


def test_unscoped_default_probe_refused(tmp_path):
    store = tmp_path / "store"
    engine = adapter(store)
    engine = replace(
        engine,
        profile=engine.profile.model_copy(update={"restart_argv": ("echo", "hi")}),
    )
    raw = (
        json.dumps({"type": "session", "version": 3, "id": "s1"}) + "\n{}\n"
    ).encode()
    (store / "s1.jsonl").write_bytes(raw)
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROBE_UNSCOPED"):
        engine._run_restart_probe("s1", store / "s1.jsonl")


def test_injected_runner_receives_substituted_scoped_argv(tmp_path):
    store = tmp_path / "store"
    engine = adapter(store)
    seen = {}
    engine = replace(
        engine,
        restart_runner=lambda argv: seen.setdefault("argv", argv),
    )
    engine._run_restart_probe("sid", store / "sid.jsonl")
    argv = seen["argv"]
    assert "sid.jsonl" in argv[-2] and argv[-1] == "sid"
    assert not any("{" in arg for arg in argv)


def test_injected_run_argv_receives_substituted_scoped_argv(tmp_path):
    store = tmp_path / "store"
    engine = adapter(store)
    seen = {}
    engine = replace(
        engine,
        run_argv=lambda argv, timeout: seen.update(argv=list(argv), timeout=timeout),
    )
    engine._run_restart_probe("sid", store / "sid.jsonl")
    assert seen["timeout"] == engine.profile.restart_timeout_s
    assert "sid.jsonl" in seen["argv"][-2] and seen["argv"][-1] == "sid"
    assert not any("{" in arg for arg in seen["argv"])


def test_foreign_message_header_with_same_id_does_not_mask_valid_header(tmp_path):
    store = tmp_path / "store"
    engine = adapter(store)
    valid = json.dumps({"type": "session", "version": 3, "id": "same"})
    (store / "valid.jsonl").write_bytes((valid + "\n{}\n").encode())
    foreign = json.dumps({"type": "message", "version": 3, "id": "same"})
    (store / "foreign.jsonl").write_bytes((foreign + "\n{}\n").encode())
    archive = engine.export_exact("same")
    assert archive.lineage.endswith("valid.jsonl")
    v2 = json.dumps({"type": "session", "version": 2, "id": "same"})
    (store / "v2.jsonl").write_bytes((v2 + "\n{}\n").encode())
    assert engine.export_exact("same").lineage.endswith("valid.jsonl")


def test_two_valid_headers_same_id_reported_ambiguous(tmp_path):
    store = tmp_path / "store"
    engine = adapter(store)
    for name in ("a.jsonl", "b.jsonl"):
        header = json.dumps({"type": "session", "version": 3, "id": "dup"})
        (store / name).write_bytes((header + "\n{}\n").encode())
    with pytest.raises(CapabilityUnavailable, match="SESSION_LOOKUP_AMBIGUOUS"):
        engine.export_exact("dup")


def test_two_sessions_restore_exact_bytes_and_refuse_collision(tmp_path):
    source = adapter(tmp_path / "source")
    target = adapter(tmp_path / "target")
    for sid in ("session-one", "session-two"):
        raw = (
            json.dumps({"type": "session", "version": 3, "id": sid})
            + '\r\n{"type":"message","text":"original"}\r\n'
        ).encode()
        (source.sessions_dir / f"2026_timestamp_{sid}.jsonl").write_bytes(raw)
        archive = source.export_exact(sid)
        proof = target.import_exact(archive)
        assert proof.native_id == sid
        assert (target.sessions_dir / f"{sid}.jsonl").read_bytes() == raw
        assert target.export_exact(sid).records == archive.records
        with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
            target.import_exact(archive)
        assert (target.sessions_dir / f"{sid}.jsonl").read_bytes() == raw


def test_imported_identity_survives_engine_edits(tmp_path):
    source = adapter(tmp_path / "source")
    target = adapter(tmp_path / "target")
    raw = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    (source.sessions_dir / "2026_evolved_sid.jsonl").write_bytes(raw)
    archive = replace(source.export_exact("sid"), generation=7)
    proof = target.import_exact(archive)
    assert proof.generation == 7
    assert target.export_exact("sid") == archive
    path = target.sessions_dir / "sid.jsonl"
    evolved = raw + b'{"type":"message"}\n'
    path.write_bytes(evolved)
    reexported = target.export_exact("sid")
    assert reexported.lineage == archive.lineage
    assert reexported.generation == 7
    assert reexported.records == {"session.jsonl": evolved}
    assert path.read_bytes() == evolved


@pytest.mark.parametrize(
    "metadata",
    [
        b"{",
        b"{}",
        b"x" * 4097,
        b'{"logical_id":"sid","native_id":"sid","generation":true,"lineage":"x"}',
        b'{"logical_id":"sid","native_id":"sid","generation":-1,"lineage":"x"}',
        b'{"logical_id":"wrong","native_id":"sid","generation":0,"lineage":"x"}',
        b'{"logical_id":"sid","native_id":"sid","generation":0,"lineage":" "}',
        b'{"logical_id":"sid","native_id":"sid","generation":0,"generation":1,"lineage":"x"}',
    ],
)
def test_corrupt_metadata_fails_closed(tmp_path, metadata):
    engine = adapter(tmp_path / "store")
    raw = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    (engine.sessions_dir / "sid.jsonl").write_bytes(raw)
    private = engine.sessions_dir / ".untether-migration"
    private.mkdir(mode=0o700)
    (private / "sid.json").write_bytes(metadata)
    with pytest.raises(CapabilityUnavailable, match="SESSION_METADATA_MALFORMED"):
        engine.export_exact("sid")


def test_metadata_collision_rolls_back_only_owned_record(tmp_path):
    source = adapter(tmp_path / "source")
    target = adapter(tmp_path / "target")
    raw = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    (source.sessions_dir / "sid.jsonl").write_bytes(raw)
    private = target.sessions_dir / ".untether-migration"
    private.mkdir(mode=0o700)
    foreign = private / "sid.json"
    foreign.write_bytes(b"unrelated")
    with pytest.raises(CapabilityUnavailable, match="SESSION_METADATA_WRITE_FAILED"):
        target.import_exact(source.export_exact("sid"))
    assert foreign.read_bytes() == b"unrelated"
    assert not (target.sessions_dir / "sid.jsonl").exists()


@pytest.mark.parametrize("link_directory", [False, True])
def test_metadata_symlink_refused(tmp_path, link_directory):
    engine = adapter(tmp_path / "store")
    raw = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    (engine.sessions_dir / "sid.jsonl").write_bytes(raw)
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (foreign / "sid.json").write_bytes(b"unrelated")
    private = engine.sessions_dir / ".untether-migration"
    try:
        if link_directory:
            private.symlink_to(foreign, target_is_directory=True)
        else:
            private.mkdir(mode=0o700)
            (private / "sid.json").symlink_to(foreign / "sid.json")
    except OSError:
        pytest.skip("symlink creation unavailable")
    with pytest.raises(CapabilityUnavailable, match="SESSION_METADATA_UNSAFE"):
        engine.export_exact("sid")
    assert (foreign / "sid.json").read_bytes() == b"unrelated"


def test_failed_probe_removes_owned_metadata(tmp_path):
    source = adapter(tmp_path / "source")
    target = adapter(tmp_path / "target")
    raw = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    (source.sessions_dir / "sid.jsonl").write_bytes(raw)
    target = replace(
        target,
        profile=target.profile.model_copy(update={"restart_argv": ("unscoped",)}),
    )
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROBE_UNSCOPED"):
        target.import_exact(source.export_exact("sid"))
    assert not (target.sessions_dir / "sid.jsonl").exists()
    assert not (target.sessions_dir / ".untether-migration" / "sid.json").exists()


def test_locked_read_requires_live_owned_identity(tmp_path):
    from untether.session_migration.session_gate import GateRefused, migration_lock

    engine = adapter(tmp_path / "store")
    (engine.sessions_dir / "sid.jsonl").write_bytes(
        b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    )
    root = tmp_path / "shared-gates"
    with pytest.raises(GateRefused, match="NOT_OWNED"):
        engine.export_locked("sid")
    with (
        migration_lock(root=root, engine_id="pi", native_id="other"),
        pytest.raises(GateRefused, match="NOT_OWNED"),
    ):
        engine.export_locked("sid")
    with migration_lock(root=root, engine_id="pi", native_id="sid"):
        assert engine.export_locked("sid").native_id == "sid"
    with pytest.raises(GateRefused, match="NOT_OWNED"):
        engine.export_locked("sid")


@pytest.mark.parametrize(
    "data", [b"{}\n", b"{bad\n", b'{"type":"session","version":3,"id":"other"}\n']
)
def test_overwrite_malformed_input_preserves_old_content(tmp_path, data):
    engine = adapter(tmp_path / "store")
    old = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    path = engine.sessions_dir / "sid.jsonl"
    path.write_bytes(old)
    with pytest.raises(CapabilityUnavailable):
        engine.import_overwrite(
            engine="pi",
            native_id="sid",
            records={"session.jsonl": data},
            expected_prior_records={"session.jsonl": old},
            generation=4,
            lineage="sid",
        )
    assert path.read_bytes() == old


def test_traversal_id_refused(tmp_path):
    engine = adapter(tmp_path / "store")
    with pytest.raises(CapabilityUnavailable, match="NATIVE_ID_INVALID"):
        engine.export_exact("../foreign")


def test_env_gate_root_busy_then_export_after_release(tmp_path, monkeypatch):
    from untether.session_migration.session_gate import SourceBusy, session_gate

    store = tmp_path / "store"
    engine = adapter(store)
    header = json.dumps({"type": "session", "version": 3, "id": "gated"})
    (store / "gated.jsonl").write_bytes((header + "\n{}\n").encode())
    gate_root = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gate_root))
    with (
        session_gate(root=gate_root, engine_id=engine.engine, native_id="gated"),
        pytest.raises(SourceBusy),
    ):
        engine.export_exact("gated")
    archive = engine.export_exact("gated")
    assert archive.lineage.endswith("gated.jsonl")


def test_export_snapshot_holds_gate_against_external_acquire(tmp_path, monkeypatch):
    from untether.session_migration.session_gate import SourceBusy, session_gate

    store = tmp_path / "store"
    engine = adapter(store)
    header = json.dumps({"type": "session", "version": 3, "id": "held"})
    (store / "held.jsonl").write_bytes((header + "\n{}\n").encode())
    gate_root = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gate_root))
    with engine.export_snapshot("held") as archive:
        assert archive.native_id == "held"
        with pytest.raises(SourceBusy):
            session_gate(
                root=gate_root, engine_id=engine.engine, native_id="held"
            ).__enter__()
    # released: external acquire succeeds again
    with session_gate(root=gate_root, engine_id=engine.engine, native_id="held"):
        pass


def test_export_snapshot_releases_gate_on_body_exception(tmp_path, monkeypatch):
    from untether.session_migration.session_gate import session_gate

    store = tmp_path / "store"
    engine = adapter(store)
    header = json.dumps({"type": "session", "version": 3, "id": "boom"})
    (store / "boom.jsonl").write_bytes((header + "\n{}\n").encode())
    gate_root = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gate_root))
    with (
        pytest.raises(RuntimeError, match="body-failure"),
        engine.export_snapshot("boom"),
    ):
        raise RuntimeError("body-failure")
    with session_gate(root=gate_root, engine_id=engine.engine, native_id="boom"):
        pass
    assert not any((gate_root).glob("gate-*.lock"))


def test_project_lookup_two_sessions_no_mutation(tmp_path, monkeypatch):
    engine = adapter(tmp_path / "store")
    for sid, cwd in (("one", "/projects/one"), ("two", "/projects/two")):
        (engine.sessions_dir / f"timestamp-{sid}.jsonl").write_bytes(
            json.dumps(
                {"type": "session", "version": 3, "id": sid, "cwd": cwd}
            ).encode()
            + b"\nopaque content\n"
        )
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    profile_before = engine.profile.model_dump()
    environment_before = dict(os.environ)

    def forbidden(*args, **kwargs):
        pytest.fail("project lookup invoked export/restart/content read")

    monkeypatch.setattr(FileBundlePersistenceAdapter, "_export_snapshot", forbidden)
    monkeypatch.setattr(FileBundlePersistenceAdapter, "_run_restart_probe", forbidden)
    original = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", forbidden)
    assert engine.lookup_for_project("one", PurePosixPath("/projects/one")) == "one"
    assert engine.lookup_for_project("two", PurePosixPath("/projects/two")) == "two"
    for sid in ("two", "missing"):
        with pytest.raises(CapabilityUnavailable) as error:
            engine.lookup_for_project(sid, PurePosixPath("/projects/one"))
        assert str(error.value) == "SESSION_PROJECT_REFUSED"
    monkeypatch.setattr(Path, "read_bytes", original)
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before
    assert engine.profile.model_dump() == profile_before
    assert dict(os.environ) == environment_before
    assert not (tmp_path / "shared-gates").exists()


@pytest.mark.parametrize(
    "cwd, root, accepted",
    [
        ("/projects/one", "/projects/one/", True),
        ("/projects/One", "/projects/one", False),
        ("/projects/one/child", "/projects/one", False),
        ("C:\\Projects\\one", "c:/Projects/one", True),
        ("C:/projects/one", "c:/Projects/one", False),
        ("C:/Projects/one", "D:/Projects/one", False),
        (None, "/projects/one", False),
        (17, "/projects/one", False),
        ("", "/projects/one", False),
        ("projects/one", "/projects/one", False),
        ("/projects/../one", "/one", False),
        ("/projects/one\n", "/projects/one", False),
        ("C:Projects/one", "C:/Projects/one", False),
        ("//server/share", "//server/share", False),
        ("/projects/one", "relative", False),
    ],
)
def test_project_lookup_portable_cwd(tmp_path, cwd, root, accepted):
    from pathlib import PurePosixPath

    engine = adapter(tmp_path / "store")
    header = {"type": "session", "version": 3, "id": "sid"}
    if cwd is not None:
        header["cwd"] = cwd
    (engine.sessions_dir / "record.jsonl").write_bytes(
        json.dumps(header).encode() + b"\n"
    )
    # Preserve foreign-host spelling even when this test runs on Windows.
    project = PurePosixPath(root)
    if accepted:
        assert engine.lookup_for_project("sid", project) == "sid"
    else:
        with pytest.raises(CapabilityUnavailable) as error:
            engine.lookup_for_project("sid", project)
        assert str(error.value) == "SESSION_PROJECT_REFUSED"
    assert engine.lookup_exact("sid") == "sid"


def test_lookup_two_ids_is_identity_only_and_has_no_side_effects(tmp_path, monkeypatch):
    from pathlib import Path

    engine = adapter(tmp_path / "store")
    for sid in ("one", "two"):
        (engine.sessions_dir / f"timestamp-{sid}.jsonl").write_bytes(
            json.dumps({"type": "session", "version": 3, "id": sid}).encode()
            + b"\nnot a transcript\n"
        )
    sentinel = tmp_path / "config-fence-cache"
    sentinel.write_bytes(b"unchanged")
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    profile_before = engine.profile.model_dump()
    environment_before = dict(os.environ)
    read_bytes = Path.read_bytes

    def forbidden(*args, **kwargs):
        pytest.fail("lookup invoked a content/export/gate/restart operation")

    monkeypatch.setattr(FileBundlePersistenceAdapter, "_export_snapshot", forbidden)
    monkeypatch.setattr(FileBundlePersistenceAdapter, "_run_restart_probe", forbidden)
    monkeypatch.setattr(Path, "read_bytes", forbidden)
    assert engine.lookup_exact("one") == "one"
    assert engine.lookup_exact("two") == "two"
    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before
    assert engine.profile.model_dump() == profile_before
    assert dict(os.environ) == environment_before
    assert not (tmp_path / "shared-gates").exists()


@pytest.mark.parametrize("sid", ["", "../foreign", "a/b", "a\\b", "x" * 129])
def test_lookup_invalid_id(tmp_path, sid):
    with pytest.raises(CapabilityUnavailable, match="NATIVE_ID_INVALID"):
        adapter(tmp_path / "store").lookup_exact(sid)


@pytest.mark.parametrize(
    "header",
    [
        b"{bad",
        b"\xff",
        b"{}",
        b'{"type":"message","version":3,"id":"sid"}',
        b'{"type":"session","version":true,"id":"sid"}',
        b'{"type":"session","version":2,"id":"sid"}',
    ],
)
def test_lookup_invalid_header_is_not_identity(tmp_path, header):
    engine = adapter(tmp_path / "store")
    (engine.sessions_dir / "record.jsonl").write_bytes(header + b"\n")
    with pytest.raises(CapabilityUnavailable, match="SESSION_NOT_FOUND"):
        engine.lookup_exact("sid")


def test_lookup_missing_and_ambiguous(tmp_path):
    engine = adapter(tmp_path / "store")
    with pytest.raises(CapabilityUnavailable, match="SESSION_NOT_FOUND"):
        engine.lookup_exact("sid")
    for name in ("a", "b"):
        (engine.sessions_dir / f"{name}.jsonl").write_bytes(
            b'{"type":"session","version":3,"id":"sid"}\n'
        )
    with pytest.raises(CapabilityUnavailable, match="SESSION_LOOKUP_AMBIGUOUS"):
        engine.lookup_exact("sid")


def test_lookup_requires_declared_store_scope(tmp_path):
    engine = adapter(tmp_path / "store")
    for scope in ("", str(tmp_path / "other")):
        mismatched = replace(
            engine, profile=engine.profile.model_copy(update={"sessions_dir": scope})
        )
        with pytest.raises(
            CapabilityUnavailable, match="PERSISTENCE_STORE_SCOPE_MISMATCH"
        ):
            mismatched.lookup_exact("sid")


def test_lookup_symlink_refused(tmp_path):
    engine = adapter(tmp_path / "store")
    target = tmp_path / "target"
    target.write_bytes(b'{"type":"session","version":3,"id":"sid"}\n')
    try:
        (engine.sessions_dir / "link.jsonl").symlink_to(target)
    except OSError:
        pytest.skip("symlink creation unavailable")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_UNSAFE"):
        engine.lookup_exact("sid")


@pytest.mark.parametrize("size, accepted", [(65536, True), (65537, False)])
def test_lookup_header_byte_bound(tmp_path, size, accepted):
    engine = adapter(tmp_path / "store")
    header = b'{"type":"session","version":3,"id":"sid"}'
    (engine.sessions_dir / "record.jsonl").write_bytes(
        header + b" " * (size - len(header) - 1) + b"\n"
    )
    if accepted:
        assert engine.lookup_exact("sid") == "sid"
    else:
        with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_MALFORMED"):
            engine.lookup_exact("sid")


@pytest.mark.parametrize("count, accepted", [(10000, True), (10001, False)])
def test_lookup_candidate_bound(tmp_path, monkeypatch, count, accepted):
    from pathlib import Path

    engine = adapter(tmp_path / "store")
    match = engine.sessions_dir / "match.jsonl"
    match.write_bytes(b'{"type":"session","version":3,"id":"sid"}\n')
    foreign = engine.sessions_dir / "foreign.jsonl"
    foreign.write_bytes(b"{}\n")
    monkeypatch.setattr(
        Path, "glob", lambda *_: iter([match] + [foreign] * (count - 1))
    )
    if accepted:
        assert engine.lookup_exact("sid") == "sid"
    else:
        with pytest.raises(CapabilityUnavailable, match="SESSION_LOOKUP_LIMIT"):
            engine.lookup_exact("sid")
