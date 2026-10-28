"""Coordinator-integrated real-engine round trip for installed OpenCode.

Requirement under test: ``transfer.perform`` drives the REAL installed OpenCode
sync adapter (``OpenCodeSyncAdapter``, ``opencode-sync-v1``) for two genuine
engine-created sessions through a full push/pull cycle, bound by the PRODUCTION
``opencode-sync-v1`` registration path: the declared
``SessionMigrationSettings.persistence`` profile is routed through
``file_bundle_adapter.register_from_settings`` by ``transfer.perform`` itself,
never by a test-injected registry lambda. The destination store is installed
inside the receiver stand-in by the same production entry point, exactly as a
separate receiver process does at startup from its own settings; the caller's
local registration is restored afterwards.

Sessions are created by the installed engine itself (native ``serve`` ->
``POST /session`` -> admitted prompt -> offline shell) in disposable XDG/HOME
stores; history and event equality, stable native ids, an independent serve
restart and a shell follow-up are all asserted against engine reads. No model
is invoked: the admitted prompt uses ``resume:false`` and the follow-up is an
offline shell echo.

TRANSPORT SUBSTITUTE (EXPLICIT): only the SSH byte transport is replaced by an
in-process ``receiver_call`` stand-in built from the real protocol codec,
``NativeReceiver`` and ``ReceiverEndpoint``. There is no real SSH, no network,
no remote host, no API key, no real user store and no live Untether state. This
is engine + coordinator evidence, NOT SSH or model evidence.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
from pathlib import Path

import anyio
import pytest

from tests.test_session_migration_opencode_runtime import (
    _ok,
    _running,
    _tool_outputs,
)
from untether.session_migration import native_adapter, transfer
from untether.session_migration.file_bundle_adapter import (
    register_from_settings,
)
from untether.session_migration.native_adapter import (
    require_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.opencode_sync_adapter import OpenCodeSyncAdapter
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    handle_receiver,
)
from untether.session_migration.settings import (
    EnginePersistenceSettings,
    MigrationPeer,
    OpenCodeSyncProfile,
    SessionMigrationSettings,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS
from untether.telegram.chat_sessions import ChatSessionStore, resolve_sessions_path
from untether.telegram.topic_state import TopicStateStore, resolve_state_path

ENGINE = "opencode"
PIN = "SHA256:" + "A" * 43
_NPM_BIN = (
    Path.home()
    / "AppData"
    / "Roaming"
    / "npm"
    / "node_modules"
    / "opencode-ai"
    / "bin"
    / "opencode.exe"
)


def _discover_executable() -> Path | None:
    """Local installed-executable discovery; override wins, then npm bin."""
    candidates = []
    override = os.environ.get("UNTETHER_TEST_OPENCODE_EXE")
    if override:
        candidates.append(Path(override))
    candidates.append(_NPM_BIN)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


_EXE = _discover_executable()
_READY = _EXE is not None


@pytest.fixture(autouse=True)
def _isolate_adapter_registry():
    """No registration leaks between the tests in this module."""
    unregister_persistence_adapter(ENGINE)
    yield
    unregister_persistence_adapter(ENGINE)


def _free_port() -> int:
    """Ephemeral loopback port; the adapter still has a bind/readiness race."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _profile(root: Path, port: int) -> OpenCodeSyncProfile:
    return OpenCodeSyncProfile(
        format="opencode-sync-v1",
        executable=str(_EXE),
        data_root=str(root),
        port=port,
        request_timeout_s=20,
        serve_start_timeout_s=20,
    )


def _settings(
    peer: MigrationPeer, profile: OpenCodeSyncProfile
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
        expected_endpoint_id="endpoint-opencode",
        destination_binding="enrolled-opencode",
        projects={"demo": "/srv/demo"},
    )


def _create_session(
    adapter: OpenCodeSyncAdapter, title: str, admitted: str, marker: str
) -> dict:
    """One genuine engine-created session with admitted prompt + shell tool."""
    info = _ok(adapter, "/session", "POST", {"title": title})
    sid = info["id"]
    status, body = adapter._request(
        f"/api/session/{sid}/prompt",
        "POST",
        {"prompt": {"text": admitted}, "resume": False},
    )
    assert status in (200, 202), body
    _ok(
        adapter,
        f"/session/{sid}/shell",
        "POST",
        {"agent": "build", "command": f"echo {marker}"},
    )
    messages = _ok(adapter, f"/session/{sid}/message")
    events = adapter._collect_events(sid)
    # resume:false is durable admission, not a projected user message.
    assert admitted in json.dumps(events)
    assert any(event["type"] == "session.next.prompt.admitted.1" for event in events)
    assert any(marker in output for output in _tool_outputs(messages))
    return {
        "id": sid,
        "info": info,
        "messages": messages,
        "events": events,
        "marker": marker,
    }


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


@pytest.mark.skipif(not _READY, reason="installed OpenCode executable not found")
def test_opencode_coordinator_push_pull_roundtrip(tmp_path: Path, monkeypatch) -> None:
    """Push A -> B and pull B -> C for two engine sessions, restart, follow-up."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))

    src_root, dest_root, return_root = (
        tmp_path / "a",
        tmp_path / "b",
        tmp_path / "c",
    )
    for root in (src_root, dest_root, return_root):
        root.mkdir()
    src_profile = _profile(src_root, _free_port())
    dest_profile = _profile(dest_root, _free_port())
    return_profile = _profile(return_root, _free_port())

    # --- Real engine sessions in store A (native serve + admitted prompt) ---
    source = OpenCodeSyncAdapter(engine=ENGINE, profile=src_profile)
    created: dict[str, dict] = {}
    # Native serve binds sessions to its inherited cwd; no project file is
    # written and the disposable store lives entirely under tmp_path.
    with _running(source):
        for title, marker in (
            ("coordinator session one", "oc-coord-one"),
            ("coordinator session two", "oc-coord-two"),
        ):
            record = _create_session(source, title, f"admitted {marker}", marker)
            assert Path(record["info"]["directory"]) == Path.cwd()
            created[record["id"]] = record
    ids = sorted(created)
    assert len(ids) == 2

    # Honest baseline read through the production adapter CLASS.
    src_archives = {sid: source.export_exact(sid) for sid in ids}
    for sid in ids:
        assert src_archives[sid].native_id == sid
        assert src_archives[sid].lineage == "opencode-sync-v1:sync-events.json"
        assert (
            json.loads(src_archives[sid].records["sync-events.json"])
            == created[sid]["events"]
        )

    peer = _peer()
    src_cfg = _settings(peer, src_profile)
    dest_cfg = _settings(peer, dest_profile)
    ops = tmp_path / "ops"

    # No test-injected registry entry: transfer.perform must register the
    # adapter itself from the declared opencode-sync-v1 profile.
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
    assert isinstance(registered, OpenCodeSyncAdapter)
    assert registered.data_root == src_root
    assert registered.profile.format == "opencode-sync-v1"
    assert push_results[0].operation_id != push_results[1].operation_id
    for result, sid in zip(push_results, ids, strict=True):
        assert result.state == "completed"
        assert result.native_id == sid and result.engine == ENGINE
        assert len(result.receipt_sha256) == 64

    # Independent fresh serve of B alone: ids, events and projected history.
    dest_probe = OpenCodeSyncAdapter(engine=ENGINE, profile=dest_profile)
    with _running(dest_probe):
        for sid in ids:
            assert _ok(dest_probe, f"/session/{sid}")["id"] == sid
            assert dest_probe._collect_events(sid) == created[sid]["events"]
            assert (
                _ok(dest_probe, f"/session/{sid}/message") == created[sid]["messages"]
            )

    # --- Pull B -> C into a THIRD fresh store; C is the local owner now ---
    return_cfg = _settings(peer, return_profile)
    _install(return_cfg)
    pull_call = _receiver_call(
        tmp_path / "pull-native", dest_settings=dest_cfg, local_settings=return_cfg
    )
    config_file = tmp_path / "untether.toml"
    destinations = (
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
            for sid, destination in zip(ids, destinations, strict=True)
        ]
    for result, sid in zip(pull_results, ids, strict=True):
        assert result.state == "bound"
        assert result.native_id == sid and result.engine == ENGINE
        assert len(result.receipt_sha256) == 64

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

    # --- Native offline shell follow-up in C, then independent restart ---
    return_probe = OpenCodeSyncAdapter(engine=ENGINE, profile=return_profile)
    evolved: dict[str, list[dict]] = {}
    after: dict[str, list[dict]] = {}
    with _running(return_probe):
        for index, sid in enumerate(ids):
            messages = _ok(return_probe, f"/session/{sid}/message")
            assert messages == created[sid]["messages"]
            assert return_probe._collect_events(sid) == created[sid]["events"]
            follow = f"oc-followup-{index}"
            _ok(
                return_probe,
                f"/session/{sid}/shell",
                "POST",
                {"agent": "build", "command": f"echo {follow}"},
            )
            after[sid] = _ok(return_probe, f"/session/{sid}/message")
            outputs = _tool_outputs(after[sid])
            assert any(created[sid]["marker"] in output for output in outputs)
            assert any(follow in output for output in outputs)
            events = return_probe._collect_events(sid)
            assert events[: len(created[sid]["events"])] == created[sid]["events"]
            assert len(events) > len(created[sid]["events"])
            assert f"admitted {created[sid]['marker']}" in json.dumps(events)
            evolved[sid] = events
    # A fresh serve child (independent restart) reads the persisted follow-up.
    with _running(return_probe):
        for sid in ids:
            assert _ok(return_probe, f"/session/{sid}/message") == after[sid]
            assert return_probe._collect_events(sid) == evolved[sid]

    # Source events were never rewritten by the push. A fresh gate root models
    # a distinct local owner: the completed outbound fence under the original
    # gate root correctly blocks the pushed source there, so the honest
    # re-read runs where no fence for this source exists.
    with monkeypatch.context() as patch:
        patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "reread-gate"))
        assert source.export_exact(ids[0]).records == src_archives[ids[0]].records
