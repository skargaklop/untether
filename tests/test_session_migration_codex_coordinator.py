"""Coordinator-integrated real-engine round trip for installed Codex (bounded).

Requirement under test: ``transfer.perform`` drives the REAL installed Codex
adapter (``CodexRolloutPersistenceAdapter``) for two distinct native sessions,
bound by the PRODUCTION ``codex-rollout-v1`` registration path: the declared
``persistence`` profile is routed through
``file_bundle_adapter.register_from_settings`` by ``transfer.perform`` itself,
never by a test-injected registry lambda. The destination engine adapter is
installed inside the receiver stand-in by the same production entry point,
exactly as a separate receiver process does at startup from its own settings;
the caller's local registration is restored afterwards.

TRANSPORT SUBSTITUTE (explicit): only the SSH byte transport is replaced by an
in-process ``receiver_call`` stand-in built from the real protocol codec,
``NativeReceiver`` and ``ReceiverEndpoint``. There is no real SSH, no network,
no remote host, no API key and no live Untether state. Every store is a
disposable ``CODEX_HOME`` holding only ``sessions/`` (no ``auth.json``,
``config.toml`` or ``installation_id``), so the probe cannot touch real
accounts. Restoration is proven by the installed ``codex`` app-server itself
(JSON-RPC initialize -> thread/resume -> thread/read), repeated by fresh
app-server processes for independent literal history assertions. The follow-up
is an offline native ``thread/shellCommand`` echo in the restored cwd: it is
NOT model or provider continuation, and no such continuation is claimed. Byte
identity is strict prefix preservation (a resume only appends), and native
id/cwd/history must be recoverable from destination bytes alone.

POST-RESUME CONTRACT (production fix under test): the installed app-server
appends a fresh ``thread_settings_applied`` event on EVERY resume (verified
locally: 2659 -> 3516 -> 4373 bytes across two resumes, each keeping the prior
bytes as an exact prefix). ``CodexRolloutPersistenceAdapter`` tolerates that
append-only growth in its own prefix check, and the coordinator's post-resume
verification must tolerate the same delta: the local import gate
``transfer._read_imported`` has to accept a re-export that only APPENDS to the
inbound archive instead of refusing with ``RESTORE_IDENTITY_MISMATCH``. The
outbound push, the offline native shell follow-up and the inbound pull from an
already-resumed source are all asserted as successful, unconditionally. The
inbound pull body pins that contract and is expected to FAIL until the
production fix lands.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import uuid
from pathlib import Path

import anyio
import pytest

from tests.test_session_migration_codex_runtime import (
    CODEX_CMD,
    _persisted_shell,
    _rollout_bytes,
    _shell_probe,
)
from untether.session_migration import native_adapter, transfer
from untether.session_migration.codex_rollout_adapter import (
    CodexRolloutPersistenceAdapter,
)
from untether.session_migration.file_bundle_adapter import (
    register_from_settings,
)
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    require_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    handle_receiver,
)
from untether.session_migration.settings import (
    CodexRolloutProfile,
    EnginePersistenceSettings,
    MigrationPeer,
    SessionMigrationSettings,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS
from untether.telegram.topic_state import TopicStateStore, resolve_state_path

ENGINE = "codex"
MODEL = "gpt-5.1-codex"
PROVIDER = "openai"
RESUME_TIMEOUT_S = 60.0
PIN = "SHA256:" + "A" * 43
_READY = CODEX_CMD.is_file()


@pytest.fixture(autouse=True)
def _isolate_adapter_registry():
    """No registration leaks between the tests in this module."""
    unregister_persistence_adapter(ENGINE)
    yield
    unregister_persistence_adapter(ENGINE)


def _home(root: Path) -> tuple[Path, Path]:
    """Disposable CODEX_HOME carrying only the sessions tree."""
    sessions = root / "sessions"
    sessions.mkdir(parents=True, exist_ok=True)
    for forbidden in ("auth.json", "config.toml", "installation_id"):
        assert not (root / forbidden).exists()
    return root, sessions


def _profile(sessions_root: Path) -> CodexRolloutProfile:
    return CodexRolloutProfile(
        format="codex-rollout-v1",
        sessions_root=str(sessions_root),
        app_server_executable=str(CODEX_CMD),
        resume_timeout_s=RESUME_TIMEOUT_S,
    )


def _settings(
    peer: MigrationPeer, profile: CodexRolloutProfile
) -> SessionMigrationSettings:
    return SessionMigrationSettings(
        enabled=True,
        peers={"vps": peer},
        persistence=EnginePersistenceSettings(engines={ENGINE: profile}),
    )


def _install(settings: SessionMigrationSettings) -> None:
    """Production registration: the declared profile becomes the adapter."""
    unregister_persistence_adapter(ENGINE)
    register_from_settings(settings, engine=ENGINE)


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
        expected_endpoint_id="endpoint-codex",
        destination_binding="enrolled-codex",
        projects={"demo": "/srv/demo"},
    )


def _adapter(sessions_root: Path) -> CodexRolloutPersistenceAdapter:
    return CodexRolloutPersistenceAdapter(
        engine=ENGINE,
        sessions_root=sessions_root,
        app_server_executable=CODEX_CMD,
        resume_timeout_s=RESUME_TIMEOUT_S,
    )


def _write_sessions(sessions_root: Path, ids, cwd, rel) -> dict[str, bytes]:
    """Two distinct native-shaped rollouts with completed history."""
    originals: dict[str, bytes] = {}
    for sid in ids:
        data = _rollout_bytes(sid, str(cwd[sid]), MODEL, PROVIDER, history=True)
        journal = sessions_root / rel[sid]
        journal.parent.mkdir(parents=True, exist_ok=True)
        journal.write_bytes(data)
        originals[sid] = data
    return originals


def _assert_history(read: dict, session_id: str, cwd: Path) -> None:
    """Literal history identity, not the adapter's own visibility check."""
    thread = read["thread"]
    assert thread["id"] == session_id
    assert os.path.normcase(thread["cwd"]) == os.path.normcase(str(cwd))
    turns = thread["turns"]
    assert turns, "restored Codex history vanished"
    turn = turns[0]
    assert turn["id"] == "turn-probe" and turn["status"] == "completed"
    user, assistant, tool = turn["items"]
    assert (user["type"], user["id"]) == ("userMessage", "user-probe")
    assert (assistant["type"], assistant["text"]) == (
        "agentMessage",
        "History assistant sentinel",
    )
    assert (tool["type"], tool["id"], tool["tool"]) == (
        "dynamicToolCall",
        "tool-probe",
        "history_probe",
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
    caller's local registration is restored before returning.
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


def _codex_ids(
    tmp_path: Path,
) -> tuple[tuple[str, str], dict[str, Path], dict[str, str]]:
    ids = (str(uuid.uuid4()), str(uuid.uuid4()))
    cwd = {sid: tmp_path / f"ws-{index}" for index, sid in enumerate(ids)}
    for path in cwd.values():
        path.mkdir()
    rel = {sid: f"2026/10/27/rollout-2026-10-27T00-00-00-{sid}.jsonl" for sid in ids}
    return ids, cwd, rel


def _recording_rpc(monkeypatch) -> tuple[list[tuple[str, Path]], object]:
    """Record only production import probes; direct reads stay unpatched."""
    real_rpc = CodexRolloutPersistenceAdapter._default_rpc_resume
    probes: list[tuple[str, Path]] = []

    def recording(adapter, native_id, journal_path, timeout_s):
        result = real_rpc(adapter, native_id, journal_path, timeout_s)
        probes.append((native_id, Path(journal_path)))
        return result

    monkeypatch.setattr(
        CodexRolloutPersistenceAdapter, "_default_rpc_resume", recording
    )
    return probes, real_rpc


@pytest.mark.skipif(
    not _READY, reason=f"installed codex not found at {CODEX_CMD}; resume cannot run"
)
def test_codex_coordinator_push_and_native_followup_roundtrip(
    tmp_path: Path, monkeypatch
) -> None:
    """Push A -> B for two distinct sessions, real shell follow-up, evolved push."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))

    ids, cwd, rel = _codex_ids(tmp_path)
    src_home, src_sessions = _home(tmp_path / "a-home")
    originals = _write_sessions(src_sessions, ids, cwd, rel)

    # Honest baseline read through the production adapter CLASS (verification
    # read only; the coordinator path below does not receive this object).
    src_profile = _profile(src_sessions)
    src_archives = {sid: _adapter(src_sessions).export_exact(sid) for sid in ids}
    for sid in ids:
        assert src_archives[sid].records["session.jsonl"] == originals[sid]
        assert src_archives[sid].native_id == sid
        assert src_archives[sid].generation == 0
        assert src_archives[sid].lineage == f"codex-rollout-v1:{rel[sid]}"

    peer = _peer()
    src_cfg = _settings(peer, src_profile)
    dest_home, dest_sessions = _home(tmp_path / "b-home")
    dest_cfg = _settings(peer, _profile(dest_sessions))
    ops = tmp_path / "ops"
    probes, real_rpc = _recording_rpc(monkeypatch)

    # No test-injected registry entry: transfer.perform must register the
    # adapter itself from the declared codex-rollout-v1 profile.
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
    assert isinstance(registered, CodexRolloutPersistenceAdapter)
    assert registered.sessions_root == src_sessions
    assert push_results[0].operation_id != push_results[1].operation_id
    for result, sid in zip(push_results, ids, strict=True):
        assert result.state == "completed"
        assert result.native_id == sid and result.engine == ENGINE
        assert len(result.receipt_sha256) == 64
        # Resume only appends: destination bytes keep the source bytes exact.
        landed = (dest_sessions / rel[sid]).read_bytes()
        assert landed[: len(originals[sid])] == originals[sid]

    # Independent cold app-server read of destination B: native id, cwd and
    # literal history recovered from B alone.
    for sid in ids:
        read = real_rpc(_adapter(dest_sessions), sid, dest_sessions / rel[sid], 60.0)
        _assert_history(read, sid, cwd[sid])

    # --- Native offline shell follow-up in store B, then independent restart ---
    evolved: dict[str, bytes] = {}
    markers: dict[str, str] = {}
    for sid in ids:
        journal = dest_sessions / rel[sid]
        before = journal.read_bytes()
        marker = "untetherCodexShell" + uuid.uuid4().hex
        markers[sid] = marker
        live, completed = _shell_probe(dest_home, cwd[sid], sid, marker)
        live_item = _persisted_shell(live, completed["id"], cwd[sid], marker)
        _assert_history(live, sid, cwd[sid])
        after = journal.read_bytes()
        assert after[: len(before)] == before
        assert marker.encode() in after, "shell output was not durably journalled"
        cold = real_rpc(_adapter(dest_sessions), sid, journal, 60.0)
        assert cold["thread"]["id"] == sid
        assert _persisted_shell(cold, completed["id"], cwd[sid], marker) == live_item
        assert journal.read_bytes()[: len(after)] == after
        evolved[sid] = after

    # --- Evolved push B -> D through the coordinator again ---
    evolved_home, evolved_sessions = _home(tmp_path / "d-home")
    evolved_src_cfg = _settings(peer, _profile(dest_sessions))
    evolved_cfg = _settings(peer, _profile(evolved_sessions))
    # transfer.perform only registers from the profile when the engine is not
    # already registered; the evolved LOCAL owner must be store B, or the
    # export silently re-reads unchanged store A, recomputes the original
    # identity, and reuses the completed push operation id -> OPERATION_CONFLICT.
    _install(evolved_src_cfg)
    evolved_call = _receiver_call(
        tmp_path / "evolved-native",
        dest_settings=evolved_cfg,
        local_settings=evolved_src_cfg,
    )
    with monkeypatch.context() as patch:
        # A fresh gate root models a distinct source owner: the original push
        # fence for these native ids stays completed and is never overwritten.
        patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "evolved-gate"))
        evolved_results = [
            transfer.perform(
                evolved_src_cfg,
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
        landed = (evolved_sessions / rel[sid]).read_bytes()
        assert landed[: len(evolved[sid])] == evolved[sid]
        assert landed[: len(originals[sid])] == originals[sid]
        assert markers[sid].encode() in landed
        header = json.loads(landed.split(b"\n", 1)[0])
        assert header["type"] == "session_meta"
        assert header["payload"]["session_id"] == sid

    # The evolved push is a distinct transfer identity, not a replay of the
    # original push (which would have reused the same operation id and fence).
    assert len({r.operation_id for r in (*push_results, *evolved_results)}) == 4

    # Source bytes were never rewritten; all homes stayed credential-free.
    for sid in ids:
        assert (src_sessions / rel[sid]).read_bytes() == originals[sid]
    for home in (src_home, dest_home, evolved_home):
        assert not (home / "auth.json").exists()

    # Two imports per session (push, evolved push), each proven by one
    # production app-server probe against the store that received the archive.
    assert len(probes) == 4
    assert {sid for sid, _ in probes} == set(ids)
    assert sorted(path.name for _, path in probes) == sorted(
        Path(rel[sid]).name for sid in ids for _ in range(2)
    )


@pytest.mark.skipif(not _READY, reason=f"installed codex not found at {CODEX_CMD}")
def test_codex_coordinator_pull_roundtrip_binds_appended_resume(
    tmp_path: Path, monkeypatch
) -> None:
    """Pull B -> C through the coordinator from an already-resumed source.

    PRODUCTION CONTRACT UNDER TEST: the local import gate
    ``transfer._read_imported`` must verify C's post-import re-export against
    the inbound archive while tolerating the append-only delta the installed
    codex app-server produces on every resume (it appends a fresh
    ``thread_settings_applied`` event, keeping the prior bytes as an exact
    prefix). The pull must therefore complete and bind, never refuse with
    ``RESTORE_IDENTITY_MISMATCH``.

    Bound outcome asserted unconditionally here: C binds the session
    (``state == "bound"``, native id, engine, receipt, durable topic token),
    C's bytes keep both the source's original bytes and its resumed bytes as
    exact prefixes, a cold app-server read of C recovers native id, cwd and
    literal history, and the pull leaves B's bytes untouched.
    """
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))

    ids, cwd, rel = _codex_ids(tmp_path)
    _, remote_sessions = _home(tmp_path / "remote-home")
    originals = _write_sessions(remote_sessions, ids, cwd, rel)
    _, return_sessions = _home(tmp_path / "return-home")
    config_file = tmp_path / "untether.toml"
    ops = tmp_path / "ops"

    # Source already resumed: the journal grew by one resume-appended event
    # and the originally written bytes stay an exact prefix of it.
    journal = remote_sessions / rel[ids[0]]
    before = journal.read_bytes()
    CodexRolloutPersistenceAdapter._default_rpc_resume(
        _adapter(remote_sessions), ids[0], journal, RESUME_TIMEOUT_S
    )
    grown = journal.read_bytes()
    assert before == originals[ids[0]]
    assert grown[: len(before)] == before and len(grown) > len(before)

    peer = _peer()
    remote_cfg = _settings(peer, _profile(remote_sessions))
    return_cfg = _settings(peer, _profile(return_sessions))
    _install(return_cfg)
    pull_call = _receiver_call(
        tmp_path / "pull-native", dest_settings=remote_cfg, local_settings=return_cfg
    )
    with monkeypatch.context() as patch:
        patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "return-gate"))
        result = transfer.perform(
            return_cfg,
            peer_name="vps",
            project="demo",
            session_id=ids[0],
            engine=ENGINE,
            direction="from",
            root=ops,
            config_path=config_file,
            receiver_call=pull_call,
            destination_chat=-100123,
            destination_topic=42,
        )

    # Unconditional success: an append-only resumed source must still bind.
    assert result.state == "bound" and result.native_id == ids[0]
    assert result.engine == ENGINE and len(result.receipt_sha256) == 64
    landed = (return_sessions / rel[ids[0]]).read_bytes()
    assert landed[: len(originals[ids[0]])] == originals[ids[0]]
    assert landed[: len(grown)] == grown
    token = anyio.run(
        TopicStateStore(resolve_state_path(config_file)).get_session_resume,
        -100123,
        42,
        ENGINE,
    )
    assert token is not None and token.value == ids[0] and token.engine == ENGINE

    # Destination bytes alone carry the native session: a cold app-server read
    # of C recovers id, cwd and literal history.
    _assert_history(
        CodexRolloutPersistenceAdapter._default_rpc_resume(
            _adapter(return_sessions),
            ids[0],
            return_sessions / rel[ids[0]],
            RESUME_TIMEOUT_S,
        ),
        ids[0],
        cwd[ids[0]],
    )

    # The export never rewrote the source store.
    assert journal.read_bytes() == grown


@pytest.mark.skipif(not _READY, reason=f"installed codex not found at {CODEX_CMD}")
def test_codex_coordinator_reclaim_crash_after_journal_before_metadata(
    tmp_path: Path, monkeypatch
) -> None:
    """Reclaim crash between journal replace and metadata upsert, then retry.

    Crash model: the reclaim overwrite replaced C's journal with the incoming
    bytes but the identity sidecar upsert failed, leaving the transitional
    store (incoming journal, prior-generation sidecar) that the pending
    coordinator fence keeps admission-closed. The same operation retried must
    complete through the adapter's declared recovery hook — accepted because
    the sidecar still holds the PRIOR generation identity, never because
    current records are trusted — bind exactly once, and append exactly one
    resume event (no duplicate journal history). Divergent sidecar metadata
    would refuse without any overwrite.
    """
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))

    sid = str(uuid.uuid4())
    ws = tmp_path / "ws-crash"
    ws.mkdir()
    cwd = {sid: ws}
    rel = {sid: "2026/10/27/rollout-2026-10-27T00-00-00-" + sid + ".jsonl"}
    _, local_sessions = _home(tmp_path / "local-home")
    originals = _write_sessions(local_sessions, (sid,), cwd, rel)
    _, remote_sessions = _home(tmp_path / "remote-home")
    config_file = tmp_path / "untether.toml"
    ops = tmp_path / "ops"

    peer = _peer()
    local_cfg = _settings(peer, _profile(local_sessions))
    remote_cfg = _settings(peer, _profile(remote_sessions))
    _install(local_cfg)

    # --- Push local C -> remote B: completed fence owns reclaim admission ---
    push_call = _receiver_call(
        tmp_path / "push-native", dest_settings=remote_cfg, local_settings=local_cfg
    )
    transfer.perform(
        local_cfg,
        peer_name="vps",
        project="demo",
        session_id=sid,
        engine=ENGINE,
        direction="to",
        root=ops,
        receiver_call=push_call,
    )

    # --- Remote B evolves: exactly one real-thread resume event appended ---
    remote_journal = remote_sessions / rel[sid]
    remote_before = remote_journal.read_bytes()
    settings_event = (
        json.dumps(
            {
                "timestamp": "2026-10-27T00:00:05.000Z",
                "ordinal": 999,
                "type": "event_msg",
                "payload": {
                    "type": "thread_settings_applied",
                    "thread_settings": {},
                },
            }
        ).encode()
        + b"\n"
    )
    remote_journal.write_bytes(remote_before + settings_event)
    evolved = remote_journal.read_bytes()
    assert evolved.startswith(remote_before) and len(evolved) > len(remote_before)
    with monkeypatch.context() as patch:
        patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "remote-gate"))
        incoming = _adapter(remote_sessions).export_exact(sid)
    from dataclasses import replace as _dc_replace

    # Canonical sidecar bytes: identical key order and compact separators to
    # the adapter's own model_dump_json(), so a post-import re-export is
    # byte-identical to this fabricated envelope.
    incoming = _dc_replace(
        incoming,
        generation=1,
        records={
            **incoming.records,
            "untether.json": json.dumps(
                {
                    "logical_id": sid,
                    "native_id": sid,
                    "generation": 1,
                    "lineage": incoming.lineage,
                },
                separators=(",", ":"),
            ).encode(),
        },
    )
    assert incoming.generation == 1  # one durable resume -> generation bumped
    package = transfer.build_inbound_package(local_cfg, incoming)
    incoming_journal = incoming.records["session.jsonl"]

    common: dict[str, object] = {
        "direction": "from",
        "peer_name": "vps",
        "project": "demo",
        "session_id": sid,
        "engine": ENGINE,
        "root": ops,
        "incoming_package": package,
        "config_path": config_file,
        "destination_chat": -100777,
        "destination_topic": 7,
    }

    real_write = CodexRolloutPersistenceAdapter._write_metadata

    def crashing_write(self, metadata, *, replace_existing=False):
        if replace_existing:
            raise CapabilityUnavailable("SESSION_METADATA_WRITE_FAILED")
        return real_write(self, metadata, replace_existing=replace_existing)

    def appending_probe(self, native_id, journal_path, timeout_s):
        journal_path.write_bytes(journal_path.read_bytes() + settings_event)

    # --- First pull: journal replaced, sidecar publication injected dead ---
    with monkeypatch.context() as patch:
        patch.setattr(
            CodexRolloutPersistenceAdapter,
            "_write_metadata",
            crashing_write,
        )
        patch.setattr(
            CodexRolloutPersistenceAdapter,
            "_run_resume_probe",
            appending_probe,
        )
        with pytest.raises(
            CapabilityUnavailable, match="SESSION_METADATA_WRITE_FAILED"
        ):
            transfer.perform(local_cfg, receiver_call=None, **common)

    # Transitional store: incoming journal bytes landed exactly (the probe
    # never ran: metadata publication precedes it), identity sidecar still
    # reflects the PRIOR state.
    local_journal = local_sessions / rel[sid]
    assert local_journal.read_bytes() == incoming_journal
    sidecar = local_sessions / ".untether-migration" / f"{sid}.json"
    assert not sidecar.exists() or json.loads(sidecar.read_bytes())["generation"] == 0
    from untether.session_migration.source_fence import read_source_fence

    fence = read_source_fence(
        tmp_path / "gate" / "fences", engine=ENGINE, native_id=sid
    )
    assert fence is not None and fence.state == "pending"

    # --- Retry of the SAME operation: recovery hook accepts the transition ---
    with monkeypatch.context() as patch:
        patch.setattr(
            CodexRolloutPersistenceAdapter,
            "_run_resume_probe",
            appending_probe,
        )
        result = transfer.perform(local_cfg, receiver_call=None, **common)
    assert result.state == "bound" and result.native_id == sid
    assert result.engine == ENGINE and len(result.receipt_sha256) == 64

    # No duplicate journal history: exactly one resume event appended beyond
    # the incoming bytes (the retry's own probe), and nothing else.
    landed = local_journal.read_bytes()
    suffix = landed[len(incoming_journal) :]
    events = [json.loads(line) for line in suffix.splitlines() if line.strip()]
    assert len(events) == 1, "duplicate resume history after reclaim retry"
    assert (
        events[0]["type"] == "event_msg"
        and events[0]["payload"]["type"] == "thread_settings_applied"
    )

    # Published identity is the incoming generation; fence ACTIVE (rebound
    # pull ownership, ready for the next round trip); binding is durable and
    # a repeat replay binds identically without new history.
    assert json.loads(sidecar.read_bytes())["generation"] == incoming.generation
    fence = read_source_fence(
        tmp_path / "gate" / "fences", engine=ENGINE, native_id=sid
    )
    assert fence is not None and fence.state == "active"
    token = anyio.run(
        TopicStateStore(resolve_state_path(config_file)).get_session_resume,
        -100777,
        7,
        ENGINE,
    )
    assert token is not None and token.value == sid and token.engine == ENGINE
    replay = transfer.perform(local_cfg, receiver_call=None, **common)
    assert replay.state == "bound" and replay.native_id == sid
    assert local_journal.read_bytes() == landed

    # The remote source store was never rewritten by the reclaim.
    assert remote_journal.read_bytes().startswith(originals[sid])
    assert remote_journal.read_bytes() != landed


def test_codex_coordinator_registration_is_profile_bound(
    tmp_path: Path, monkeypatch
) -> None:
    """Production registration binds the declared store; no injected lambda."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    _, sessions = _home(tmp_path / "store-home")
    cfg = _settings(_peer(), _profile(sessions))
    assert native_adapter._ADAPTERS.get(ENGINE) is None
    try:
        _install(cfg)
        adapter = require_persistence_adapter(ENGINE)
        assert isinstance(adapter, CodexRolloutPersistenceAdapter)
        assert adapter.engine == ENGINE
        assert adapter.sessions_root == sessions
        assert adapter.app_server_executable == CODEX_CMD
        assert adapter.resume_timeout_s == RESUME_TIMEOUT_S
    finally:
        unregister_persistence_adapter(ENGINE)
    assert native_adapter._ADAPTERS.get(ENGINE) is None
