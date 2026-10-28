"""Coordinator-integrated real-engine round trip for OMP 18.1.6 (bounded).

Requirement under test: ``transfer.perform`` drives the REAL installed OMP
adapter (``OmpPersistenceAdapter``, ``omp-v1``) through a full push/pull cycle
with the real receiver boundary (``NativeReceiver`` + ``ReceiverEndpoint`` +
protocol codec) standing in only for the SSH byte transport. The engine
adapter is bound by the PRODUCTION profile registration path — the declared
``SessionMigrationSettings.persistence`` profile is routed through
``file_bundle_adapter``/``omp_adapter.register_from_settings`` by
``transfer.perform`` itself — never by a test-injected registry lambda. The
destination store is installed inside the receiver stand-in by the same
production entry point, exactly as a separate receiver process does at
startup from its own settings; the caller's local registration is restored
afterwards.

Sessions are created by the existing installed-SDK fixture
(``fixtures/omp_persistence_probe.ts``) and every import is proven by the
adapter's own embedded native RPC restart probe. Test 1 pushes two sessions
A -> B, re-reads destination bytes with the SDK fixture, pulls both back into
a third fresh store, asserts the durable chat/topic bindings, then runs a
native OMP RPC ``bash`` follow-up in the pulled store, proves the persisted
``bashExecution`` survives an independent restart, and pushes the evolved
journal BACK through the coordinator into a fresh store. Native ids, exact
journal bytes and message history are preserved; the follow-up is offline
shell execution, never model or provider continuation. No network, no API
keys, no live Untether state, no remote hosts: disposable stores only.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path

import anyio
import pytest

from tests.test_session_migration_omp_runtime import (
    BUN,
    PACKAGE,
    _environment,
    _rpc,
    _run,
)
from untether.session_migration import native_adapter, omp_adapter, transfer
from untether.session_migration.native_adapter import (
    require_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.omp_adapter import OmpPersistenceAdapter
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    handle_receiver,
)
from untether.session_migration.settings import (
    EnginePersistenceSettings,
    MigrationPeer,
    OmpPersistenceProfile,
    SessionMigrationSettings,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS
from untether.telegram.chat_sessions import ChatSessionStore, resolve_sessions_path
from untether.telegram.topic_state import TopicStateStore, resolve_state_path

ENGINE = "omp"
FOLLOWUP_COMMAND = "echo omp-offline-followup"
PIN = "SHA256:" + "A" * 43
_READY = BUN.is_file() and (PACKAGE / "package.json").is_file()


@pytest.fixture(autouse=True)
def _isolate_adapter_registry():
    """No registration leaks between the tests in this module."""
    unregister_persistence_adapter(ENGINE)
    yield
    unregister_persistence_adapter(ENGINE)


def _store(root: Path) -> tuple[Path, Path]:
    """Disposable ``omp-v1`` layout: journal tree plus canonical blob store."""
    sessions = root / "sessions"
    sessions.mkdir(parents=True, exist_ok=True, mode=0o700)
    blobs = root / "agent" / "blobs" / "data"
    blobs.mkdir(parents=True, exist_ok=True)
    return sessions, blobs


def _stored_messages(journal: Path) -> list[dict]:
    """Raw journal message entries parsed from bytes: no engine projection."""
    return [
        json.loads(line)["message"]
        for line in journal.read_bytes().splitlines()
        if line.strip() and json.loads(line).get("type") == "message"
    ]


def _projection_drift(stored: dict, projected: dict) -> set[str]:
    """Fields where an engine read-time projection differs from stored bytes.

    ``timestamp`` is the one tolerated read-time re-stamp on a
    ``bashExecution`` message; the raw stored field is still asserted present
    (integer) and byte-stable, so archive byte and identity fidelity are
    untouched by this projection-level allowance.
    """
    return {
        key
        for key in stored.keys() | projected.keys()
        if stored.get(key) != projected.get(key)
    }


def _profile(
    sessions_dir: Path, blobs_dir: Path, workdir: Path
) -> OmpPersistenceProfile:
    return OmpPersistenceProfile(
        format="omp-v1",
        sessions_dir=str(sessions_dir),
        blobs_dir=str(blobs_dir),
        # Legacy scoped template: only a contract for injected runners, which
        # this test never uses.
        restart_argv=("offline-native-rpc", "{session_path}"),
        # Production probe launcher: the adapter substitutes ``{probe}`` with
        # its embedded RPC probe script and appends the restored journal,
        # evidence and allowlisted environment paths itself.
        restart_probe_argv=(str(BUN), "{probe}", "rpc", str(PACKAGE), str(workdir)),
        restart_timeout_s=25.0,
    )


def _settings(
    peer: MigrationPeer, profile: OmpPersistenceProfile
) -> SessionMigrationSettings:
    return SessionMigrationSettings(
        enabled=True,
        peers={"vps": peer},
        persistence=EnginePersistenceSettings(engines={ENGINE: profile}),
    )


def _install(settings: SessionMigrationSettings) -> None:
    """Production registration: the declared profile becomes the adapter."""
    unregister_persistence_adapter(ENGINE)
    omp_adapter.register_from_settings(settings, engine=ENGINE)


def _peer() -> MigrationPeer:
    return MigrationPeer(
        host="example.invalid",
        user="operator",
        identity_dir="keys",
        identity_file="selected",
        known_hosts_file="known",
        host_key_fingerprint=PIN,
        receiver_path="/usr/local/bin/untether-migration-receiver",
        execution_mode="native",
        expected_endpoint_id="endpoint-omp",
        destination_binding="enrolled-omp",
        projects={"demo": "/srv/demo"},
    )


def _receiver_call(
    native_root: Path,
    *,
    dest_settings: SessionMigrationSettings,
    local_settings: SessionMigrationSettings,
) -> transfer.ReceiverCall:
    """In-process stand-in for the SSH RPC: real protocol + receiver + adapter.

    Only the SSH byte transport is absent. The destination engine adapter is
    installed through the PRODUCTION profile registration entry point, and the
    caller's local registration is restored before returning so the
    coordinator's local import/export seam keeps its owner.
    """
    exported: dict[str, str] = {}

    def call(
        operation_id,
        package=b"",
        *,
        peer,
        project,
        engine,
        action="receive",
        session_id=None,
    ):
        if action == "export-complete":
            digest = json.loads(package)["package_sha256"]
            assert exported[operation_id] == digest
            return {
                "status": "export_completed",
                "operation_id": operation_id,
                "engine": engine,
                "session_id": session_id,
                "package_sha256": digest,
                "endpoint_id": peer.expected_endpoint_id,
            }
        native = NativeReceiver(native_root, max_archive=262144, max_expanded=262144)
        checks = dict.fromkeys(_REQUIRED_CHECKS, True)
        endpoint = ReceiverEndpoint(
            endpoint_id=peer.expected_endpoint_id,
            execution_mode="native",
            destination_binding=peer.destination_binding,
            project=project,
            deployment="direct-linux",
            arch="x86_64",
            probe_health=lambda: checks,
            native_receiver=native,
            authorized_peer="local",
        )
        fields = {
            "protocol": 2,
            "action": action,
            "operation_id": operation_id,
            "project": project,
            "size": len(package),
            "sha256": hashlib.sha256(package).hexdigest(),
            "engine": engine,
        }
        if session_id:
            fields["session_id"] = session_id
        frame = json.dumps(fields).encode() + b"\n" + package
        saved_gate = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        os.environ["UNTETHER_SESSION_GATE_ROOT"] = str(native_root / "gate")
        _install(dest_settings)
        try:
            raw = handle_receiver(endpoint, verb=action, payload=frame)
        finally:
            if saved_gate is None:
                os.environ.pop("UNTETHER_SESSION_GATE_ROOT", None)
            else:
                os.environ["UNTETHER_SESSION_GATE_ROOT"] = saved_gate
            _install(local_settings)
        reply = json.loads(raw)
        if action == "export" and "package_base64" in reply:
            reply["package"] = base64.b64decode(reply["package_base64"])
            exported[operation_id] = hashlib.sha256(reply["package"]).hexdigest()
        return reply

    return call


@pytest.mark.skipif(
    not _READY, reason=f"installed Bun/OMP package absent: {BUN}, {PACKAGE}"
)
def test_omp_coordinator_push_pull_roundtrip(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))

    # --- Real SDK sessions in store A (native title slot, v3 header, three
    # tree-linked messages) ---
    src_root = tmp_path / "a"
    src_root.mkdir()
    env_src = _environment(src_root)
    written: dict[str, dict] = {}
    for _ in range(2):
        record = _run("write", src_root, env_src)
        written[record["id"]] = record
    ids = sorted(written)
    assert len(ids) == 2
    src_sessions, src_blobs = _store(src_root)
    # Native OMP journal names are ``<timestamp>_<id>.jsonl``; the lineage and
    # every store key off that name, never off the bare session id.
    names = {sid: Path(written[sid]["file"]).name for sid in ids}
    originals = {sid: Path(written[sid]["file"]).read_bytes() for sid in ids}
    for sid in ids:
        first = originals[sid].split(b"\n", 1)[0]
        assert len(first) + 1 == 256 and json.loads(first)["type"] == "title"

    # Honest baseline read through the production adapter CLASS (verification
    # read only; the coordinator path below does not receive this object).
    src_profile = _profile(src_sessions, src_blobs, src_root)
    source = OmpPersistenceAdapter(engine=ENGINE, profile=src_profile)
    source_archives = {sid: source.export_exact(sid) for sid in ids}
    for sid in ids:
        assert source_archives[sid].records["session.jsonl"] == originals[sid]
        assert source_archives[sid].native_id == sid
        assert source_archives[sid].lineage == f"omp-v1:{names[sid]}"

    peer = _peer()
    src_cfg = _settings(peer, src_profile)
    dest_root = tmp_path / "b"
    dest_root.mkdir()
    _environment(dest_root)
    dest_sessions, dest_blobs = _store(dest_root)
    dest_profile = _profile(dest_sessions, dest_blobs, dest_root)
    dest_cfg = _settings(peer, dest_profile)
    ops = tmp_path / "ops"

    # Every import must be proven by the adapter's own embedded RPC probe; the
    # recorder captures argv and the probe's self-written evidence record.
    probes: list[list[str]] = []
    evidence: list[dict] = []
    real_probe = omp_adapter.run_probe

    def recording_probe(argv, timeout_s):
        real_probe(argv, timeout_s)
        probes.append(list(argv))
        payload = Path(argv[-2])
        if payload.is_file():
            evidence.append(json.loads(payload.read_text(encoding="utf-8")))

    monkeypatch.setattr(omp_adapter, "run_probe", recording_probe)

    # No test-injected registry entry: transfer.perform must register the
    # adapter itself from the declared profile.
    assert native_adapter._ADAPTERS.get(ENGINE) is None

    # --- Push A -> B for both sessions ---
    push_call = _receiver_call(
        tmp_path / "push-native", dest_settings=dest_cfg, local_settings=src_cfg
    )
    push_results = [
        transfer.perform(
            src_cfg,
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="to",
            root=ops,
            receiver_call=push_call,
        )
        for sid in ids
    ]
    registered = require_persistence_adapter(ENGINE)
    assert isinstance(registered, OmpPersistenceAdapter)
    assert (registered.sessions_dir, registered.blobs_dir) == (
        src_sessions,
        src_blobs,
    )
    assert push_results[0].operation_id != push_results[1].operation_id
    for result, sid in zip(push_results, ids, strict=True):
        assert result.state == "completed"
        assert result.native_id == sid and result.engine == ENGINE
        assert len(result.receipt_sha256) == 64
        # Destination bytes are the native source bytes, not a re-encoding.
        assert (dest_sessions / names[sid]).read_bytes() == originals[sid]

    # Independent SDK cold read of the destination store: native id, exact
    # file and ordered history all recovered from B alone.
    env_dest = _environment(dest_root)
    env_dest["OMP_SESSION_DIR"] = str(dest_sessions)
    env_dest["OMP_EXPECT_CWD"] = str(src_root)
    for sid in ids:
        loaded = _run("read", dest_root, env_dest, str(dest_sessions / names[sid]))
        assert loaded["id"] == sid
        assert loaded["entries"] == written[sid]["entries"]
        assert (dest_sessions / names[sid]).read_bytes() == originals[sid]
    dest_adapter = OmpPersistenceAdapter(engine=ENGINE, profile=dest_profile)
    with monkeypatch.context() as patch:
        # Destination re-export models a distinct local owner: the completed
        # push fences under the source gate root must not gate this store.
        patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "read-gate"))
        for sid in ids:
            assert (
                dest_adapter.export_exact(sid).records == source_archives[sid].records
            )

    # --- Pull B -> C into a THIRD fresh store; C is the local owner now ---
    return_root = tmp_path / "c"
    return_root.mkdir()
    return_sessions, return_blobs = _store(return_root)
    return_profile = _profile(return_sessions, return_blobs, return_root)
    return_cfg = _settings(peer, return_profile)
    _install(return_cfg)
    pull_call = _receiver_call(
        tmp_path / "pull-native", dest_settings=dest_cfg, local_settings=return_cfg
    )
    config_file = tmp_path / "untether.toml"
    pull_destinations = (
        {"destination_chat": -100123, "destination_topic": 42},
        {"destination_chat": -100456, "destination_topic": None},
    )
    with monkeypatch.context() as patch:
        patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "return-gate"))
        pull_results = [
            transfer.perform(
                return_cfg,
                peer_name="vps",
                project="demo",
                session_id=sid,
                engine=ENGINE,
                direction="from",
                root=ops,
                config_path=config_file,
                receiver_call=pull_call,
                **destination,
            )
            for sid, destination in zip(ids, pull_destinations, strict=True)
        ]
    for result, sid in zip(pull_results, ids, strict=True):
        assert result.state == "bound"
        assert result.native_id == sid and result.engine == ENGINE
        assert len(result.receipt_sha256) == 64
        assert (return_sessions / names[sid]).read_bytes() == originals[sid]

    # Durable destination bindings written by the pull through public stores.
    topic_token = anyio.run(
        TopicStateStore(resolve_state_path(config_file)).get_session_resume,
        -100123,
        42,
        ENGINE,
    )
    assert topic_token is not None
    assert topic_token.value == ids[0] and topic_token.engine == ENGINE
    chat_token = anyio.run(
        ChatSessionStore(resolve_sessions_path(config_file)).get_session_resume,
        -100456,
        None,
        ENGINE,
    )
    assert chat_token is not None
    assert chat_token.value == ids[1] and chat_token.engine == ENGINE
    assert len({r.operation_id for r in (*push_results, *pull_results)}) == 4

    # --- Native shell follow-up in the pulled store C, offline ---
    env_return = _environment(return_root)
    evolved_bytes: dict[str, bytes] = {}
    execution_by_sid: dict[str, dict] = {}
    for sid in ids:
        journal = return_sessions / names[sid]
        historical = [entry["message"] for entry in written[sid]["entries"]]
        state, messages = _rpc(return_root, env_return, journal)
        assert state["sessionId"] == sid and messages == historical
        state, evolved = _rpc(
            return_root, env_return, journal, command=FOLLOWUP_COMMAND
        )
        assert state["sessionId"] == sid
        assert evolved[:-1] == historical
        execution = evolved[-1]
        assert execution["role"] == "bashExecution"
        assert execution["command"] == FOLLOWUP_COMMAND
        assert execution["output"].strip() == "omp-offline-followup"
        assert execution["exitCode"] == 0 and execution["cancelled"] is False
        evolved_bytes[sid] = journal.read_bytes()
        assert evolved_bytes[sid] != originals[sid]
        persisted = [
            json.loads(line) for line in evolved_bytes[sid].splitlines() if line
        ]
        first_line = evolved_bytes[sid].split(b"\n", 1)[0]
        assert len(first_line) + 1 == 256  # native title slot intact
        assert json.loads(first_line)["type"] == "title"
        stored = [e["message"] for e in persisted if e.get("type") == "message"]
        assert stored[:-1] == historical  # raw journal history, from bytes
        # The engine re-stamps ``timestamp`` when it projects a bashExecution
        # message. That read-time field is the ONLY tolerated drift, and the
        # raw stored timestamp is itself durable engine state: present, typed,
        # and byte-stable (re-asserted after the independent restart read).
        assert isinstance(stored[-1]["timestamp"], int)
        assert _projection_drift(stored[-1], execution) <= {"timestamp"}
        execution_by_sid[sid] = execution
        # Independent restart must recover the persisted bashExecution and the
        # read must not mutate the journal: every recovered message matches the
        # raw stored entry except the read-time ``timestamp`` re-stamp, the raw
        # stored entries are unchanged (raw timestamp included), and the
        # journal bytes are identical.
        restarted_state, recovered = _rpc(return_root, env_return, journal)
        assert restarted_state["sessionId"] == sid
        assert recovered[:-1] == stored[:-1]
        assert recovered[-1]["role"] == "bashExecution"
        assert _projection_drift(stored[-1], recovered[-1]) <= {"timestamp"}
        assert _stored_messages(journal) == stored
        assert journal.read_bytes() == evolved_bytes[sid]

    # --- Push the evolved journal C -> D through the coordinator again ---
    evolved_root = tmp_path / "d"
    evolved_root.mkdir()
    _environment(evolved_root)
    evolved_sessions, evolved_blobs = _store(evolved_root)
    evolved_cfg = _settings(
        peer, _profile(evolved_sessions, evolved_blobs, evolved_root)
    )
    evolved_call = _receiver_call(
        tmp_path / "evolved-native",
        dest_settings=evolved_cfg,
        local_settings=return_cfg,
    )
    with monkeypatch.context() as patch:
        patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "evolved-gate"))
        evolved_results = [
            transfer.perform(
                return_cfg,
                peer_name="vps",
                project="demo",
                session_id=sid,
                engine=ENGINE,
                direction="to",
                root=ops,
                receiver_call=evolved_call,
            )
            for sid in ids
        ]
    for result, sid in zip(evolved_results, ids, strict=True):
        assert result.state == "completed" and result.native_id == sid
        landed = (evolved_sessions / names[sid]).read_bytes()
        assert landed == evolved_bytes[sid]
        landed_records = [json.loads(line) for line in landed.splitlines() if line]
        assert json.loads(landed.splitlines()[1])["id"] == sid
        landed_messages = [
            e["message"] for e in landed_records if e.get("type") == "message"
        ]
        assert landed_messages[:-1] == [
            entry["message"] for entry in written[sid]["entries"]
        ]
        assert _projection_drift(landed_messages[-1], execution_by_sid[sid]) <= {
            "timestamp"
        }

    # Byte-exact across stores: the push never rewrote store A, and store B
    # still holds the pre-follow-up native bytes.
    for sid in ids:
        assert (src_sessions / names[sid]).read_bytes() == originals[sid]
        assert (dest_sessions / names[sid]).read_bytes() == originals[sid]

    # Three imports per session (push, pull, evolved push) each proven by one
    # production probe run against the store that received the archive.
    assert len(probes) == 6
    assert len(evidence) == 6
    assert {item["sessionId"] for item in evidence} == set(ids)
    assert sorted(Path(item["sessionFile"]).name for item in evidence) == sorted(
        names[sid] for sid in ids for _ in range(3)
    )
    for argv in probes:
        assert argv[0] == str(BUN)
        assert argv[2] == "rpc" and argv[3] == str(PACKAGE)
        assert Path(argv[5]).name.endswith(".jsonl")
        assert argv[6].endswith("probe-evidence.json")


def test_omp_coordinator_registration_is_profile_bound(
    tmp_path: Path, monkeypatch
) -> None:
    """Production registration binds the declared store; no injected lambda."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    root = tmp_path / "store"
    root.mkdir()
    sessions, blobs = _store(root)
    cfg = _settings(_peer(), _profile(sessions, blobs, root))
    assert native_adapter._ADAPTERS.get(ENGINE) is None
    try:
        _install(cfg)
        adapter = require_persistence_adapter(ENGINE)
        assert isinstance(adapter, OmpPersistenceAdapter)
        assert (adapter.sessions_dir, adapter.blobs_dir) == (sessions, blobs)
        assert adapter.profile.format == "omp-v1"
    finally:
        unregister_persistence_adapter(ENGINE)
    assert native_adapter._ADAPTERS.get(ENGINE) is None
