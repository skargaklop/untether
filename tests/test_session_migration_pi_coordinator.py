"""Coordinator-integrated real-engine round trip for Pi (bounded).

Requirement under test: ``transfer.perform`` drives the REAL installed Pi
adapter (``FileBundlePersistenceAdapter``, ``pi-jsonl-v3``) through a full
push/pull/return cycle with the real receiver boundary (NativeReceiver +
ReceiverEndpoint + protocol codec) standing in only for the SSH byte
transport. Test 1 pushes two sessions A -> B through the coordinator, re-reads
the destination archive, then pulls both back B -> A-return into a third
fresh store through a second receiver pair; the source, destination, and
return journals must all stay byte-exact.

Each store gets its own restart_runner: the adapter's restart probe IS a
bounded installed-Pi RPC coldload against the declared store, asserting the
engine reports the expected session id, a sessionFile under that store,
``messageCount == 2``, and the historical user text. Sessions are plain v3
journals (no ``parentSession`` headers), so parent-journal export behaviour is
not involved. No network, no API keys, no model calls: the child runs offline
with a temp ``PI_CODING_AGENT_DIR`` and temp ``HOME``/``USERPROFILE``.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
import uuid
from pathlib import Path

import pytest

from untether.session_migration import native_adapter, transfer
from untether.session_migration.file_bundle_adapter import (
    FileBundlePersistenceAdapter,
)
from untether.session_migration.native_adapter import (
    register_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    handle_receiver,
)
from untether.session_migration.settings import (
    EnginePersistenceProfile,
    MigrationPeer,
    SessionMigrationSettings,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS

ENGINE = "pi"
PI_CMD = Path(
    os.environ.get("UNTETHER_TEST_PI_CMD", r"C:\Users\DELL E5570\.pi\agent\bin\pi.cmd")
)
COLDLOAD_BUDGET_S = 20.0
NATIVE_USER_TEXT = "Historical user request from before migration"
ASSISTANT_TEXT = "Historical assistant answer retained"
PIN = "SHA256:" + "A" * 43
# Every adapter restart probe appends here, so the test can assert exactly how
# many bounded installed-Pi coldloads the round trip cost.
_COLDLOAD_LOG: list[tuple[str, str]] = []


def _write_v3_session(sessions_dir: Path, native_id: str, cwd: Path) -> bytes:
    """Real pi v3 JSONL per docs/session-format.md: session header, then
    tree-linked message entries with AgentMessage payloads."""
    lines = [
        {
            "type": "session",
            "version": 3,
            "id": native_id,
            "timestamp": "2025-01-01T00:00:00.000Z",
            "cwd": str(cwd),
        },
        {
            "type": "message",
            "id": "aaa01111",
            "parentId": None,
            "timestamp": "2025-01-01T00:00:01.000Z",
            "message": {
                "role": "user",
                "content": NATIVE_USER_TEXT,
                "timestamp": 1735689601000,
            },
        },
        {
            "type": "message",
            "id": "bbb02222",
            "parentId": "aaa01111",
            "timestamp": "2025-01-01T00:00:02.000Z",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": ASSISTANT_TEXT}],
                "api": "openai-completions",
                "provider": "openai",
                "model": "gpt-4o-mini",
                "stopReason": "stop",
                "timestamp": 1735689602000,
            },
        },
        # Real v3 entry type; pi persists its initial thinking level ("off")
        # on every coldload that lacks one, mutating the record. With the
        # entry present the coldload appends nothing and the restored bytes
        # stay exact.
        {
            "type": "thinking_level_change",
            "id": "ccc03333",
            "parentId": "bbb02222",
            "timestamp": "2025-01-01T00:00:03.000Z",
            "thinkingLevel": "off",
        },
    ]
    data = "".join(json.dumps(line) + "\n" for line in lines).encode("utf-8")
    (sessions_dir / f"{native_id}.jsonl").write_bytes(data)
    return data


def _child_env(root: Path) -> dict[str, str]:
    env = {
        "PI_CODING_AGENT_DIR": str(root / "agent"),
        "HOME": str(root),
        "USERPROFILE": str(root),
        "TEMP": str(root / "tmp"),
        "TMP": str(root / "tmp"),
        "PI_OFFLINE": "1",
        "PYTHONUTF8": "1",
        "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
        "COMSPEC": os.environ.get("COMSPEC", ""),
        "PATH": os.environ.get("PATH", ""),
    }
    (root / "agent").mkdir(parents=True, exist_ok=True)
    (root / "tmp").mkdir(parents=True, exist_ok=True)
    return env


def _rpc_coldload(pi_cmd: Path, argv_rest: list[str], root: Path) -> tuple[dict, list]:
    """Bounded coldload: start installed pi in RPC mode against the restored
    store, send get_state + get_messages, collect both responses, kill/reap.

    Raises AssertionError (never skips) with the child's stderr on failure.
    Budget: COLDLOAD_BUDGET_S total, hard kill/reap after.
    """
    argv = [str(pi_cmd), *argv_rest]
    try:
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_child_env(root),
            cwd=str(root),  # header cwd == child project dir, else pi demands a fork
            text=True,
            encoding="utf-8",
        )
    except OSError as exc:
        raise AssertionError(f"pi RPC child failed to launch ({argv}): {exc}") from exc

    qout: queue.Queue[str] = queue.Queue()
    qerr: queue.Queue[str] = queue.Queue()
    stdout, stderr = proc.stdout, proc.stderr
    assert stdout is not None and stderr is not None
    threading.Thread(
        target=lambda: [qout.put(line) for line in stdout], daemon=True
    ).start()
    threading.Thread(
        target=lambda: [qerr.put(line) for line in stderr], daemon=True
    ).start()

    deadline = time.monotonic() + COLDLOAD_BUDGET_S
    responses: dict[str, dict] = {}
    stderr_tail: list[str] = []
    expected_ids = {"gs", "gm"}
    try:
        assert proc.stdin is not None
        for request in (
            {"id": "gs", "type": "get_state"},
            {"id": "gm", "type": "get_messages"},
        ):
            proc.stdin.write(json.dumps(request) + "\n")
        proc.stdin.flush()
        while responses.keys() != expected_ids and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                line = qout.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                if proc.poll() is not None:
                    break
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get("type") == "response" and record.get("id") in expected_ids:
                responses[record["id"]] = record
    finally:
        stderr_tail.clear()
        try:
            while True:
                stderr_tail.append(qerr.get_nowait())
        except queue.Empty:
            pass
        try:
            if proc.stdin is not None:
                proc.stdin.close()
        except OSError:
            pass
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover - reap fallback
            proc.kill()
            proc.wait(timeout=5)

    diag = (
        f"rc={proc.returncode} stderr={''.join(stderr_tail)[:600]!r} "
        f"responses={sorted(responses)}"
    )
    assert responses.keys() == expected_ids, f"pi RPC did not answer commands: {diag}"
    for request_id in expected_ids:
        record = responses[request_id]
        assert record.get("success") is True, f"{request_id} failed: {record}"
    return responses["gs"]["data"], responses["gm"]["data"]["messages"]


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
        expected_endpoint_id="endpoint-one",
        destination_binding="enrolled-one",
        projects={"demo": "/srv/demo"},
    )


def _profile(sessions_dir: Path) -> EnginePersistenceProfile:
    return EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(sessions_dir),
        session_record="pi",
        restart_argv=(
            str(PI_CMD),
            "--mode",
            "rpc",
            "--session-dir",
            "{sessions_dir}",
            "--session",
            "{session_id}",
            "-ne",
            "--no-mcp",
            "-ns",
            "--no-prompt-templates",
            "--no-themes",
            "-nc",
            "--offline",
        ),
        restart_timeout_s=25.0,
    )


def _store_adapter(store: Path, profile: EnginePersistenceProfile, root: Path):
    """One store, one adapter, one restart probe bound to that store."""

    def restart_runner(argv: tuple[str, ...]) -> None:
        pi_cmd = Path(argv[0])
        rest = list(argv[1:])
        assert argv[0] == str(PI_CMD)
        assert "--no-session" not in rest  # it suppresses session loading
        session_id = rest[rest.index("--session") + 1]
        assert Path(rest[rest.index("--session-dir") + 1]) == store
        _COLDLOAD_LOG.append((store.name, session_id))
        state, messages = _rpc_coldload(pi_cmd, rest, root)
        assert state.get("sessionId") == session_id, (
            f"cold-loaded session id {state.get('sessionId')!r} != {session_id!r}"
        )
        session_file = str(state.get("sessionFile", ""))
        assert Path(session_file) == store / f"{session_id}.jsonl", (
            f"cold-loaded sessionFile {session_file!r} is not the declared store"
        )
        assert state.get("messageCount") == 2
        user_texts = [m.get("content") for m in messages if m.get("role") == "user"]
        assert NATIVE_USER_TEXT in user_texts, (
            f"historical user text missing from get_messages: {user_texts!r}"
        )

    return FileBundlePersistenceAdapter(
        engine=ENGINE,
        profile=profile,
        sessions_dir=store,
        restart_runner=restart_runner,
    )


def _receiver_call(
    store: Path,
    profile: EnginePersistenceProfile,
    root: Path,
    *,
    native_root: Path,
    receiver_gate: Path,
):
    """In-process stand-in for the SSH RPC: real protocol + receiver + adapter.

    A real remote receiver runs in its own process with its own adapter
    registry pointing at the destination store; the swap below mirrors that
    boundary inside this single process.
    """
    import base64
    import hashlib

    remote_adapter = _store_adapter(store, profile, root)
    exported: dict[str, tuple[str, str | None, str]] = {}

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
            assert exported[operation_id] == (engine, session_id, digest)
            return {
                "status": "export_completed",
                "operation_id": operation_id,
                "engine": engine,
                "session_id": session_id,
                "package_sha256": digest,
                "endpoint_id": peer.expected_endpoint_id,
            }
        native = NativeReceiver(native_root, max_archive=65536, max_expanded=65536)
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
        previous = native_adapter._ADAPTERS.get(ENGINE)
        native_adapter._ADAPTERS[ENGINE] = lambda: remote_adapter
        try:
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
            os.environ["UNTETHER_SESSION_GATE_ROOT"] = str(receiver_gate)
            try:
                raw = handle_receiver(endpoint, verb=action, payload=frame)
            finally:
                if saved_gate is None:
                    os.environ.pop("UNTETHER_SESSION_GATE_ROOT", None)
                else:
                    os.environ["UNTETHER_SESSION_GATE_ROOT"] = saved_gate
            reply = json.loads(raw)
            if action == "export" and "package_base64" in reply:
                reply["package"] = base64.b64decode(reply["package_base64"])
                exported[operation_id] = (
                    engine,
                    session_id,
                    hashlib.sha256(reply["package"]).hexdigest(),
                )
            return reply
        finally:
            if previous is None:
                native_adapter._ADAPTERS.pop(ENGINE, None)
            else:
                native_adapter._ADAPTERS[ENGINE] = previous

    return call


@pytest.mark.skipif(
    not PI_CMD.is_file(),
    reason=f"installed Pi not found at {PI_CMD}; runtime coldload cannot run",
)
def test_pi_coordinator_push_pull_roundtrip(tmp_path: Path, monkeypatch) -> None:
    # The coordinator refuses cross-store work without an explicit gate root;
    # configure the source owner's gate (push tombstones land here).
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    _COLDLOAD_LOG.clear()

    ids = (str(uuid.uuid4()), str(uuid.uuid4()))
    src_dir, dest_dir, return_dir = (
        tmp_path / "a-sessions",
        tmp_path / "b-sessions",
        tmp_path / "a-return-sessions",
    )
    for store in (src_dir, dest_dir, return_dir):
        store.mkdir(mode=0o700)
    originals = {sid: _write_v3_session(src_dir, sid, tmp_path) for sid in ids}

    profile = _profile(src_dir)
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    ops = tmp_path / "ops"

    # Source archives as the honest baseline; no fences exist yet.
    source_archives = {
        sid: _store_adapter(src_dir, profile, tmp_path).export_exact(sid) for sid in ids
    }
    for sid in ids:
        # Plain v3 journals carry no parentSession headers, so the exported
        # archive holds the session record itself; parent-journal inclusion is
        # a separate behaviour and is deliberately not assumed here.
        assert source_archives[sid].records["session.jsonl"] == originals[sid]
        assert source_archives[sid].native_id == sid

    register_persistence_adapter(
        ENGINE, lambda: _store_adapter(src_dir, profile, tmp_path)
    )
    try:
        push_call = _receiver_call(
            dest_dir,
            profile,
            tmp_path,
            native_root=tmp_path / "push-native",
            receiver_gate=tmp_path / "push-receiver-gate",
        )
        push_results = [
            transfer.perform(
                cfg,
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
        assert push_results[0].operation_id != push_results[1].operation_id
        for result, sid in zip(push_results, ids, strict=True):
            assert result.state == "completed"
            assert result.native_id == sid
            assert result.engine == ENGINE
            assert len(result.receipt_sha256) == 64
        assert len(list((tmp_path / "push-native" / "receipts").glob("*.json"))) == 2

        # Re-export from the destination equals the source archive records.
        with monkeypatch.context() as patch:
            patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "read-gate"))
            for sid in ids:
                at_dest = _store_adapter(dest_dir, profile, tmp_path).export_exact(sid)
                # Destination re-export reproduces the source archive exactly,
                # including whatever record set the exporter declares.
                assert at_dest.records == source_archives[sid].records
                assert at_dest.records["session.jsonl"] == originals[sid]
                assert at_dest.native_id == sid and at_dest.logical_id == sid

        # Return leg: pull both sessions B -> A-return into a THIRD fresh
        # store. Its own gate root models a distinct local owner: the push
        # tombstones under the source gate stay untouched evidence.
        unregister_persistence_adapter(ENGINE)
        register_persistence_adapter(
            ENGINE, lambda: _store_adapter(return_dir, profile, tmp_path)
        )
        pull_call = _receiver_call(
            dest_dir,
            profile,
            tmp_path,
            native_root=tmp_path / "pull-native",
            receiver_gate=tmp_path / "pull-receiver-gate",
        )
        with monkeypatch.context() as patch:
            patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "return-gate"))
            pull_results = [
                transfer.perform(
                    cfg,
                    peer_name="vps",
                    project="demo",
                    session_id=sid,
                    engine=ENGINE,
                    direction="from",
                    root=ops,
                    receiver_call=pull_call,
                )
                for sid in ids
            ]
        for result, sid in zip(pull_results, ids, strict=True):
            assert result.state == "verified"
            assert result.native_id == sid
            assert result.engine == ENGINE
            assert len(result.receipt_sha256) == 64
        operation_ids = {
            result.operation_id for result in (*push_results, *pull_results)
        }
        assert len(operation_ids) == 4
        # The return leg imports locally, so the coordinator records its own
        # receipts; the destination only exported and must not have imported.
        assert len(list((ops / "native" / "receipts").glob("*.json"))) == 2
    finally:
        unregister_persistence_adapter(ENGINE)

    # Byte-exact across all three stores; source bytes were never rewritten.
    for sid in ids:
        for store in (src_dir, dest_dir, return_dir):
            assert (store / f"{sid}.jsonl").read_bytes() == originals[sid]

    # Two import coldloads per direction, one per session, always against the
    # store that received the archive; the export-only destination never
    # loads Pi.
    assert sorted(_COLDLOAD_LOG) == sorted(
        [("b-sessions", sid) for sid in ids]
        + [("a-return-sessions", sid) for sid in ids]
    )
