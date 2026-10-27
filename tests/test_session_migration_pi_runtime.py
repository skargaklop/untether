"""Real installed-Pi RPC coldload after a file-bundle restore (bounded).

Requirement under test: ``FileBundlePersistenceAdapter`` for the declared
``pi-jsonl-v3`` layout exports a real v3 session record and restores it
byte-exactly into a disposable store, after which an *independent installed
Pi subprocess* in RPC mode cold-loads the restored store via the engine's own
exact-id selection (``--session-dir`` + ``--session``) and reports the
historical user text through ``get_state`` / ``get_messages``.

No network, no API keys, no model calls: the child is started offline
(``--offline``, ``PI_OFFLINE=1``) with a temp ``PI_CODING_AGENT_DIR`` and
temp ``HOME``/``USERPROFILE``, extensions/skills/themes/templates/context
discovery disabled. The coldload is the adapter's restart probe
(``restart_runner``), bounded to a hard kill/reap budget; a concrete runtime
failure fails the test with the child's stderr — it is never skipped while
the installed Pi command exists.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import subprocess
import threading
import uuid
from pathlib import Path

import pytest

from untether.session_migration.file_bundle_adapter import (
    FileBundlePersistenceAdapter,
)
from untether.session_migration.native_adapter import NativeArchive
from untether.session_migration.settings import EnginePersistenceProfile

PI_CMD = Path(
    os.environ.get("UNTETHER_TEST_PI_CMD", r"C:\Users\DELL E5570\.pi\agent\bin\pi.cmd")
)
COLDLOAD_BUDGET_S = 20.0
_NATIVE_USER_TEXT = "Historical user request from before migration"
_ASSISTANT_TEXT = "Historical assistant answer retained"


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
                "content": _NATIVE_USER_TEXT,
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
                "content": [{"type": "text", "text": _ASSISTANT_TEXT}],
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
    record = sessions_dir / f"{native_id}.jsonl"
    record.write_bytes(data)
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

    Raises AssertionError (never skip) with the child's stderr on any concrete
    runtime failure. Budget: COLDLOAD_BUDGET_S total, hard kill/reap after.
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

    import time

    deadline = time.monotonic() + COLDLOAD_BUDGET_S
    responses: dict[str, dict] = {}
    stderr_tail: list[str] = []
    try:
        for request_id, command in (("gs", "get_state"), ("gm", "get_messages")):
            assert proc.stdin is not None
            proc.stdin.write(json.dumps({"id": request_id, "type": command}) + "\n")
        proc.stdin.flush()
        while len(responses) < 2 and time.monotonic() < deadline:
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
            if record.get("type") == "response" and record.get("id") in ("gs", "gm"):
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
            proc.wait()

    diag = (
        f"rc={proc.returncode} stderr={''.join(stderr_tail)[:600]!r} "
        f"responses={sorted(responses)}"
    )
    assert len(responses) == 2, f"pi RPC did not answer get_state+get_messages: {diag}"
    for request_id in ("gs", "gm"):
        record = responses[request_id]
        assert record.get("success") is True, f"{request_id} failed: {record}"
    return responses["gs"]["data"], responses["gm"]["data"]["messages"]


@pytest.mark.skipif(
    not PI_CMD.is_file(),
    reason=f"installed Pi not found at {PI_CMD}; runtime coldload cannot run",
)
def test_pi_rpc_coldload_after_file_bundle_restore(tmp_path: Path, monkeypatch) -> None:
    native_id = str(uuid.uuid4())  # actual hyphenated UUID, exact native id

    # -- source store: real v3 fixture, exported through the declared layout --
    src_dir = tmp_path / "src-sessions"
    src_dir.mkdir()
    data = _write_v3_session(src_dir, native_id, tmp_path)

    # export_exact refuses without an explicit gate root: configure one
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate-root"))
    profile = EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(src_dir),
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
    exporter = FileBundlePersistenceAdapter(
        engine="pi", profile=profile, sessions_dir=src_dir
    )
    archive = exporter.export_exact(native_id)
    assert archive.native_id == native_id
    assert archive.records["session.jsonl"] == data

    # -- restore into a second disposable store; the adapter's own restart
    #    probe IS the bounded installed-Pi RPC coldload --
    child_dir = tmp_path / "child-sessions"
    child_dir.mkdir()

    def restart_runner(argv: tuple[str, ...]) -> None:
        pi_cmd = Path(argv[0])
        assert argv[0] == str(PI_CMD)
        rest = list(argv[1:])
        assert "--no-session" not in rest  # it suppresses session loading
        state, messages = _rpc_coldload(pi_cmd, rest, tmp_path)
        # native id confirmed by the engine itself
        assert state.get("sessionId") == native_id, (
            f"cold-loaded session id {state.get('sessionId')!r} != {native_id!r}"
        )
        session_file = str(state.get("sessionFile", ""))
        assert session_file.endswith(f"{native_id}.jsonl"), (
            f"cold-loaded sessionFile {session_file!r} not the restored record"
        )
        roles = [m.get("role") for m in messages]
        assert "user" in roles, f"no user message in history: {roles}"
        user_texts = [m.get("content") for m in messages if m.get("role") == "user"]
        assert _NATIVE_USER_TEXT in user_texts, (
            f"historical user text missing from get_messages: {user_texts!r}"
        )
        assert state.get("messageCount", 0) >= 2, (
            f"messageCount {state.get('messageCount')} shows cold load was empty"
        )

    importer = FileBundlePersistenceAdapter(
        engine="pi",
        profile=profile,
        sessions_dir=child_dir,
        restart_runner=restart_runner,
    )
    proof = importer.import_exact(
        NativeArchive(
            engine="pi",
            logical_id=native_id,
            native_id=native_id,
            generation=0,
            lineage="file-bundle:pi:test",
            records={"session.jsonl": data},
            digest=hashlib.sha256(data).hexdigest(),
        )
    )
    assert proof.native_id == native_id
    assert proof.restarted is True and proof.resumed is True
    # restored record byte-exact on disk, untouched by the coldload probe
    assert (child_dir / f"{native_id}.jsonl").read_bytes() == data
