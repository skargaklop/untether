import json
import os
import sys
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from untether.session_migration.file_bundle_adapter import (
    FileBundlePersistenceAdapter,
    _collect_parent_chain,
    _layout_root_journal,
    _parents_record_parse,
    _safe_journal_basename,
)
from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.settings import EnginePersistenceProfile


@pytest.fixture(autouse=True)
def configured_gate(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "shared-gates"))


def _layout_bytes(name: str) -> bytes:
    return json.dumps({"version": 1, "root_journal": name}, sort_keys=True).encode()


def _parents_bytes(*names: str) -> bytes:
    return json.dumps({"version": 1, "journals": list(names)}, sort_keys=True).encode()


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
        # The ORIGINAL timestamped root journal basename survives import:
        # native parentSession entries reference journal paths.
        assert (
            archive.records["layout.json"]
            == json.dumps(
                {"version": 1, "root_journal": f"2026_timestamp_{sid}.jsonl"},
                sort_keys=True,
            ).encode()
        )
        assert (target.sessions_dir / f"2026_timestamp_{sid}.jsonl").read_bytes() == raw
        assert target.export_exact(sid) == archive
        with pytest.raises(CapabilityUnavailable, match="SESSION_ID_COLLISION"):
            target.import_exact(archive)
        assert (target.sessions_dir / f"2026_timestamp_{sid}.jsonl").read_bytes() == raw


def test_imported_identity_survives_engine_edits(tmp_path):
    source = adapter(tmp_path / "source")
    target = adapter(tmp_path / "target")
    raw = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    (source.sessions_dir / "2026_evolved_sid.jsonl").write_bytes(raw)
    archive = replace(source.export_exact("sid"), generation=7)
    proof = target.import_exact(archive)
    assert proof.generation == 7
    assert target.export_exact("sid") == archive
    path = target.sessions_dir / "2026_evolved_sid.jsonl"
    evolved = raw + b'{"type":"message"}\n'
    path.write_bytes(evolved)
    reexported = target.export_exact("sid")
    assert reexported.lineage == archive.lineage
    assert reexported.generation == 7
    assert reexported.records == {"session.jsonl": evolved} | {
        k: v for k, v in archive.records.items() if k != "session.jsonl"
    }
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


def test_timestamped_journal_import_preserves_original_basename(tmp_path):
    """Timestamped native journal imports under its ORIGINAL basename: the
    archive carries a validated hashed versioned layout record, and parent-
    session path references in the native journal stay intact."""
    source = adapter(tmp_path / "source")
    target = adapter(tmp_path / "target")
    raw = (
        b'{"type":"session","version":3,"id":"sid"}\n'
        b'{"parentSession":"sessions/2026-01-02T03-04-05-sid.jsonl"}\n'
    )
    (source.sessions_dir / "2026-01-02T03-04-05-sid.jsonl").write_bytes(raw)
    archive = source.export_exact("sid")
    assert set(archive.records) == {"session.jsonl", "layout.json"}
    assert archive.records["layout.json"] == _layout_bytes(
        "2026-01-02T03-04-05-sid.jsonl"
    )
    proof = target.import_exact(archive)
    assert proof.record_hashes == {
        name: __import__("hashlib").sha256(blob).hexdigest()
        for name, blob in archive.records.items()
    }
    assert (target.sessions_dir / "2026-01-02T03-04-05-sid.jsonl").read_bytes() == raw
    assert not (target.sessions_dir / "sid.jsonl").exists()
    assert target.export_exact("sid") == archive


def test_evolved_return_reclaim_preserves_original_basename(tmp_path):
    """An evolved descendant archive reclaims the pushed store record and
    keeps the original timestamped basename; the rename attempt (canonical
    name on one side only) is refused before any write."""
    store = adapter(tmp_path / "store")
    sid = "sid"
    v1 = b'{"type":"session","version":3,"id":"sid"}\nv1\n'
    v2 = b'{"type":"session","version":3,"id":"sid"}\nv2\n'
    layout = _layout_bytes("2026-01-02T03-04-05-sid.jsonl")
    (store.sessions_dir / "2026-01-02T03-04-05-sid.jsonl").write_bytes(v1)
    prior = {"session.jsonl": v1, "layout.json": layout}
    proof = store.import_overwrite(
        engine="pi",
        native_id=sid,
        records={"session.jsonl": v2, "layout.json": layout},
        expected_prior_records=prior,
        generation=1,
        lineage="reclaim-lineage",
    )
    assert set(proof) == {"session.jsonl", "layout.json"}
    assert (store.sessions_dir / "2026-01-02T03-04-05-sid.jsonl").read_bytes() == v2
    assert not (store.sessions_dir / "sid.jsonl").exists()
    assert store.export_exact("sid").records == {
        "session.jsonl": v2,
        "layout.json": layout,
    }
    # Renaming during reclaim (layout dropped from the incoming side while
    # the prior push carried it) is refused before any durable write.
    with pytest.raises(CapabilityUnavailable, match="RECORD_LAYOUT_REFUSED"):
        store.import_overwrite(
            engine="pi",
            native_id=sid,
            records={"session.jsonl": v1},
            expected_prior_records={"session.jsonl": v2, "layout.json": layout},
            generation=2,
            lineage="reclaim-lineage",
        )
    assert (store.sessions_dir / "2026-01-02T03-04-05-sid.jsonl").read_bytes() == v2
    assert not (store.sessions_dir / "sid.jsonl").exists()


def test_legacy_archive_without_layout_keeps_canonical_name(tmp_path):
    """Legacy archives (no layout record) retain existing behaviour: the
    canonical ``<id>.jsonl`` name, exact single-record set, and identical
    reclaim contracts."""
    from untether.session_migration.native_archive import NativeArchive

    engine = adapter(tmp_path / "store")
    raw = b'{"type":"session","version":3,"id":"legacy"}\n{}\n'
    archive = NativeArchive(
        engine="pi",
        logical_id="legacy",
        native_id="legacy",
        generation=0,
        lineage="file-bundle:pi:legacy.jsonl",
        records={"session.jsonl": raw},
        digest="0" * 64,
    )
    engine.import_exact(archive)
    assert (engine.sessions_dir / "legacy.jsonl").read_bytes() == raw
    assert set(engine.export_exact("legacy").records) == {"session.jsonl"}
    v2 = b'{"type":"session","version":3,"id":"legacy"}\nv2\n'
    proof = engine.import_overwrite(
        engine="pi",
        native_id="legacy",
        records={"session.jsonl": v2},
        expected_prior_records={"session.jsonl": raw},
        generation=1,
        lineage="file-bundle:pi:legacy.jsonl",
    )
    assert proof == {"session.jsonl": __import__("hashlib").sha256(v2).hexdigest()}
    assert (engine.sessions_dir / "legacy.jsonl").read_bytes() == v2


@pytest.mark.parametrize(
    "layout",
    [
        b"{",
        b"{}",
        b"x" * 513,
        b'{"version":2,"root_journal":"a.jsonl"}',
        b'{"version":1,"root_journal":"../evil.jsonl"}',
        b'{"version":1,"root_journal":"..\\\\evil.jsonl"}',
        b'{"version":1,"root_journal":"C:\\\\evil.jsonl"}',
        b'{"version":1,"root_journal":"file.jsonl:hidden"}',
        b'{"version":1,"root_journal":"/abs.jsonl"}',
        b'{"version":1,"root_journal":".hidden.jsonl"}',
        b'{"version":1,"root_journal":"..jsonl"}',
        b'{"version":1,"root_journal":"CON.jsonl"}',
        b'{"version":1,"root_journal":"com1.jsonl"}',
        b'{"version":1,"root_journal":"nul.jsonl"}',
        b'{"version":1,"root_journal":"file.txt"}',
        b'{"version":1,"root_journal":"file.JSONL"}',
        b'{"version":1,"root_journal":""}',
        b'{"version":1,"root_journal":"a\\u0000b.jsonl"}',
        b'{"version":1,"version":1,"root_journal":"a.jsonl"}',
        b'{"version":1,"root_journal":"a.jsonl","extra":1}',
    ],
)
def test_malicious_layout_record_refused_before_writes(tmp_path, layout):
    """A malicious layout record must be refused BEFORE any durable write:
    no journal file, no metadata, nothing created in the store."""
    from untether.session_migration.native_archive import NativeArchive

    engine = adapter(tmp_path / "store")
    raw = b'{"type":"session","version":3,"id":"evil"}\n{}\n'
    archive = NativeArchive(
        engine="pi",
        logical_id="evil",
        native_id="evil",
        generation=0,
        lineage="file-bundle:pi:evil.jsonl",
        records={"session.jsonl": raw, "layout.json": layout},
        digest="0" * 64,
    )
    before = {p.name for p in engine.sessions_dir.rglob("*")}
    with pytest.raises(CapabilityUnavailable, match="LAYOUT_RECORD_REFUSED"):
        engine.import_exact(archive)
    assert {p.name for p in engine.sessions_dir.rglob("*")} == before
    # Reclaim with the same malicious layout also refuses before writes.
    store = adapter(tmp_path / "reclaim")
    (store.sessions_dir / "evil.jsonl").write_bytes(raw)
    with pytest.raises(CapabilityUnavailable, match="LAYOUT_RECORD_REFUSED"):
        store.import_overwrite(
            engine="pi",
            native_id="evil",
            records={"session.jsonl": raw, "layout.json": layout},
            expected_prior_records={"session.jsonl": raw, "layout.json": layout},
            generation=1,
            lineage="lineage",
        )
    assert (store.sessions_dir / "evil.jsonl").read_bytes() == raw


def test_foreign_layout_record_in_reclaim_refused_before_writes(tmp_path):
    """A reclaim that would rename the store record (layout bytes differ
    between prior and incoming) is refused before the overwrite."""
    store = adapter(tmp_path / "store")
    raw = b'{"type":"session","version":3,"id":"sid"}\n{}\n'
    (store.sessions_dir / "2026-01-02T03-04-05-sid.jsonl").write_bytes(raw)
    with pytest.raises(CapabilityUnavailable, match="OVERWRITE_PRIOR_MISMATCH"):
        store.import_overwrite(
            engine="pi",
            native_id="sid",
            records={
                "session.jsonl": raw,
                "layout.json": _layout_bytes("2026-OTHER-sid.jsonl"),
            },
            expected_prior_records={
                "session.jsonl": raw,
                "layout.json": _layout_bytes("2026-01-02T03-04-05-sid.jsonl"),
            },
            generation=1,
            lineage="lineage",
        )
    assert (store.sessions_dir / "2026-01-02T03-04-05-sid.jsonl").read_bytes() == raw
    assert not (store.sessions_dir / "2026-OTHER-sid.jsonl").exists()


class ParentsRecordValidation:
    """parents.json archive record: parent journals validated fail-closed.

    The default ``python_classes = Test*`` glob would skip this class, so it
    opts in explicitly (pytest treats a truthy ``__test__`` as a nose-style
    test class) while keeping the task-required name.
    """

    __test__ = True

    def test_valid_two_parent_chain_parses(self):
        names = ["2026-01-01T00-00-00-root.jsonl", "2026-01-02T00-00-00-mid.jsonl"]
        assert _parents_record_parse(_parents_bytes(*names)) == names

    def test_shared_safety_gate_is_the_layout_gate(self):
        """The helper extracted from _layout_root_journal refuses the same
        basenames the layout record validator refuses."""
        for name in ("con.jsonl", "../x.jsonl", "a.txt", ".hidden.jsonl"):
            assert not _safe_journal_basename(name)
            with pytest.raises(CapabilityUnavailable, match="LAYOUT_RECORD_REFUSED"):
                _layout_root_journal(_layout_bytes(name))
        assert _safe_journal_basename("2026-01-01T00-00-00-root.jsonl")

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(_parents_bytes("a.jsonl") + b" " * 512, id="over-512-bytes"),
            pytest.param(b"{", id="malformed-json"),
            pytest.param(b'{"version":2,"journals":["a.jsonl"]}', id="wrong-version"),
            pytest.param(
                b'{"version":1,"journals":["a.jsonl"],"extra":1}', id="extra-key"
            ),
            pytest.param(
                b'{"version":1,"version":1,"journals":["a.jsonl"]}',
                id="duplicate-json-key",
            ),
            pytest.param(b'{"version":1,"journals":[]}', id="empty-journals"),
            pytest.param(
                _parents_bytes(*(f"p{i}.jsonl" for i in range(9))), id="nine-journals"
            ),
            pytest.param(_parents_bytes("a" * 201 + ".jsonl"), id="over-200-item"),
            pytest.param(_parents_bytes("../../x.jsonl"), id="traversal"),
            pytest.param(_parents_bytes("a\\b.jsonl"), id="backslash"),
            pytest.param(_parents_bytes("a.jsonl:hidden"), id="ads-colon"),
            pytest.param(_parents_bytes("con.jsonl"), id="reserved-stem"),
            pytest.param(_parents_bytes("a.txt"), id="wrong-suffix"),
            pytest.param(_parents_bytes("a.jsonl", "A.jsonl"), id="duplicate-basename"),
            pytest.param(_parents_bytes("a\u0001b.jsonl"), id="control-char"),
        ],
    )
    def test_parents_record_refused(self, data):
        with pytest.raises(CapabilityUnavailable, match="PARENTS_RECORD_REFUSED"):
            _parents_record_parse(data)


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


# --- ParentChainCollection -----------------------------------------------


def _session_header(store: Path, session_id: str, **extra) -> bytes:
    header = {"type": "session", "version": 3, "id": session_id}
    header.update(extra)
    return (json.dumps(header) + "\n{}\n").encode()


def _write_journal(store: Path, name: str, session_id: str, **extra) -> Path:
    path = store / name
    path.write_bytes(_session_header(store, session_id, **extra))
    return path


def _chain_profile(store: Path) -> EnginePersistenceProfile:
    """Declared pi profile over ``store``; also used by the ParentExport section."""
    return EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(store),
        session_record="pi",
        restart_argv=("echo", "{session_id}"),
    )


def test_parent_chain_child_parent_grandparent_ordered(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    grandparent = _write_journal(store, "gp.jsonl", "gp")
    parent = _write_journal(
        store, "parent.jsonl", "parent", parentSession=str(grandparent)
    )
    child = _write_journal(store, "child.jsonl", "child", parentSession=str(parent))
    profile = _chain_profile(store)
    assert _collect_parent_chain(profile, store, child) == [parent, grandparent]


def test_parent_chain_no_parent_session_is_empty(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    root = _write_journal(store, "root.jsonl", "root")
    profile = _chain_profile(store)
    assert _collect_parent_chain(profile, store, root) == []


def test_parent_chain_missing_parent_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    child = _write_journal(
        store, "child.jsonl", "child", parentSession=str(store / "gone.jsonl")
    )
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_MISSING"):
        _collect_parent_chain(profile, store, child)


def test_parent_chain_cycle_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    parent = _write_journal(
        store, "parent.jsonl", "parent", parentSession=str(store / "child.jsonl")
    )
    child = _write_journal(store, "child.jsonl", "child", parentSession=str(parent))
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_CYCLE"):
        _collect_parent_chain(profile, store, child)


def test_parent_chain_self_cycle_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    root = _write_journal(
        store, "root.jsonl", "root", parentSession=str(store / "root.jsonl")
    )
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_CYCLE"):
        _collect_parent_chain(profile, store, root)


def test_parent_chain_depth_nine_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    # A ten-journal lineage: nine parent hops, one over the cap of eight.
    previous = _write_journal(store, "j9.jsonl", "j9")
    for index in range(8, -1, -1):
        previous = _write_journal(
            store, f"j{index}.jsonl", f"j{index}", parentSession=str(previous)
        )
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_DEPTH"):
        _collect_parent_chain(profile, store, previous)


def test_parent_chain_parent_outside_store_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    other = tmp_path / "other"
    other.mkdir()
    parent = _write_journal(other, "parent.jsonl", "parent")
    child = _write_journal(store, "child.jsonl", "child", parentSession=str(parent))
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="PARENTS_RECORD_REFUSED"):
        _collect_parent_chain(profile, store, child)


def test_parent_chain_traversal_basename_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    child = _write_journal(
        store,
        "child.jsonl",
        "child",
        parentSession=str(store / "..\\x.jsonl"),
    )
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="PARENTS_RECORD_REFUSED"):
        _collect_parent_chain(profile, store, child)


def test_parent_chain_symlink_parent_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    outside = tmp_path / "outside"
    outside.mkdir()
    real = _write_journal(outside, "parent.jsonl", "parent")
    link = store / "link.jsonl"
    try:
        link.symlink_to(real)
    except OSError:
        pytest.skip("symlink creation unavailable")
    child = _write_journal(store, "child.jsonl", "child", parentSession=str(link))
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_UNSAFE"):
        _collect_parent_chain(profile, store, child)


def test_parent_chain_parent_header_without_id_refused(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    parent = store / "parent.jsonl"
    parent.write_bytes(b'{"type":"session","version":3}\n{}\n')
    child = _write_journal(store, "child.jsonl", "child", parentSession=str(parent))
    profile = _chain_profile(store)
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_MALFORMED"):
        _collect_parent_chain(profile, store, child)


# --- ParentExport ---------------------------------------------------------


def test_export_carries_parent_chain_records(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    gp = _write_journal(store, "gp.jsonl", "gp")
    parent = _write_journal(store, "parent.jsonl", "parent", parentSession=str(gp))
    child = _write_journal(store, "child.jsonl", "child", parentSession=str(parent))
    engine = adapter(store)
    archive = engine.export_exact("child")
    assert archive.records == {
        "session.jsonl": child.read_bytes(),
        "parent-1.jsonl": parent.read_bytes(),
        "parent-2.jsonl": gp.read_bytes(),
        "parents.json": _parents_bytes("parent.jsonl", "gp.jsonl"),
    }
    parsed = _parents_record_parse(archive.records["parents.json"])
    assert parsed == ["parent.jsonl", "gp.jsonl"]


def test_export_no_parent_session_has_no_parents_record(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    root = _write_journal(store, "r.jsonl", "r")
    engine = adapter(store)
    archive = engine.export_exact("r")
    assert archive.records == {"session.jsonl": root.read_bytes()}
    assert "parents.json" not in archive.records


def test_export_ignores_parent_migration_metadata(tmp_path):
    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    parent = _write_journal(store, "parent.jsonl", "parent")
    child = _write_journal(store, "child.jsonl", "child", parentSession=str(parent))
    metadata = store / ".untether-migration" / "parent.json"
    metadata.parent.mkdir()
    # Any well-formed metadata for the parent id; export must ignore it.
    metadata.write_text(
        '{"logical_id":"parent","native_id":"parent",'
        '"generation":3,"lineage":"file-bundle:pi:parent.jsonl"}',
        encoding="utf8",
    )
    engine = adapter(store)
    archive = engine.export_exact("child")
    assert archive.records == {
        "session.jsonl": child.read_bytes(),
        "parent-1.jsonl": parent.read_bytes(),
        "parents.json": _parents_bytes("parent.jsonl"),
    }


def test_export_record_cap_refuses_with_parent_ref_depth(tmp_path, monkeypatch):
    import untether.session_migration.file_bundle_adapter as module

    store = tmp_path / "store"
    store.mkdir(mode=0o700)
    # The real _MAX_PARENTS cap of eight refuses this depth first; widen it
    # here so the _MAX_RECORDS guard is what actually fires.
    monkeypatch.setattr(module, "_MAX_PARENTS", 1000)
    previous = _write_journal(store, "j64.jsonl", "j64")
    for index in range(63, -1, -1):
        previous = _write_journal(
            store, f"j{index}.jsonl", f"j{index}", parentSession=str(previous)
        )
    engine = adapter(store)
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_DEPTH"):
        engine.export_exact("j0")
