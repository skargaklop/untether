"""Installed Codex CLI runtime evidence: adapter export/restore + real
app-server resume proof on disposable stores.

Requirement under test: ``CodexRolloutPersistenceAdapter`` over the declared
``codex-rollout-v1`` layout exports a synthetic native-shaped journal and restores it
byte-exactly into a disposable destination store, after which the *installed
``codex`` app-server* (JSON-RPC over stdin/stdout) resumes the restored
thread with the journal's own declared settings and returns the identical
thread id/cwd/model/provider, keeping the restored bytes as an exact prefix
(verified contract: resume is strictly append-only). No credentials exist in
the disposable CODEX_HOME: only the sessions tree is copied, so the probe
cannot touch real accounts. No model/network calls are made (thread/resume
with journal-derived settings, plus the offline native shell request).

A second fresh app-server supplies independent literal history assertions.
Then thread/shellCommand executes only a unique-marker echo with timeoutMs=5000
in the disposable cwd. Completed native output must be journalled, survive an
independent restart, and survive an evolved archive return to a third store.
Synthetic completed-tool history and empty RPC acknowledgements are not proof
of execution or durability; no full model continuation is claimed. Failure is
asserted, never skipped, while the installed command exists.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
import uuid
from contextlib import suppress
from pathlib import Path

import pytest

from untether.session_migration.codex_rollout_adapter import (
    CodexRolloutPersistenceAdapter,
)

CODEX_CMD = Path(
    os.environ.get(
        "UNTETHER_TEST_CODEX_CMD",
        r"C:\Users\DELL E5570\AppData\Roaming\npm\codex.cmd",
    )
)


def _shell_probe(
    home: Path, cwd: Path, session_id: str, marker: str
) -> tuple[dict, dict]:
    """Resume then await native shell completion, not its empty RPC ack.

    The generated protocol warns this API is unsandboxed: never accept an
    arbitrary command here. Only echo a generated alphanumeric sentinel.
    """
    assert marker.isalnum()
    env = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in {"PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT"}
    }
    for key in ("CODEX_HOME", "HOME", "USERPROFILE"):
        env[key] = str(home)
    for key in (
        "APPDATA",
        "LOCALAPPDATA",
        "TEMP",
        "TMP",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_DATA_HOME",
    ):
        location = home / "probe-scratch" / key.lower()
        location.mkdir(parents=True, exist_ok=True)
        env[key] = str(location)
    env.update(PYTHONUTF8="1", OTEL_SDK_DISABLED="true")
    proc = subprocess.Popen(
        [
            str(CODEX_CMD),
            "-c",
            "check_for_update_on_startup=false",
            "-c",
            "analytics.enabled=false",
            "-c",
            "feedback.enabled=false",
            "app-server",
        ],
        cwd=str(cwd),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
    )
    messages: queue.Queue[str | None] = queue.Queue(maxsize=1024)
    assert proc.stdout is not None and proc.stdin is not None
    stdout = proc.stdout

    def reader() -> None:
        # Both individual lines and queued output are bounded; overflow fails
        # the probe rather than retaining arbitrary subprocess output.
        while line := stdout.readline(1024 * 1024):
            try:
                messages.put_nowait(line)
            except queue.Full:
                return
        with suppress(queue.Full):
            messages.put_nowait(None)

    worker = threading.Thread(target=reader, daemon=True)
    worker.start()
    deadline = time.monotonic() + 30.0
    notifications: list[dict] = []

    def receive() -> dict:
        remaining = deadline - time.monotonic()
        assert remaining > 0, "Codex shell completion exceeded 30s budget"
        try:
            line = messages.get(timeout=remaining)
        except queue.Empty as exc:
            raise AssertionError("Codex shell completion timed out") from exc
        assert line is not None, "Codex exited before shell completion"
        response = json.loads(line)
        assert isinstance(response, dict)
        assert "error" not in response, response
        return response

    def rpc(method: str, params: dict, request_id: int) -> dict:
        assert proc.stdin is not None
        proc.stdin.write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
            + "\n"
        )
        proc.stdin.flush()
        while True:
            response = receive()
            if response.get("id") == request_id:
                return response["result"]
            notifications.append(response)
            assert len(notifications) <= 1024, "Codex notification budget exceeded"

    try:
        rpc(
            "initialize",
            {
                "clientInfo": {"name": "untether-shell-test", "version": "1.0"},
            },
            1,
        )
        proc.stdin.write(json.dumps({"method": "initialized", "params": {}}) + "\n")
        proc.stdin.flush()
        resumed = rpc(
            "thread/resume",
            {
                "threadId": session_id,
                "cwd": str(cwd),
                "model": "gpt-5.1-codex",
                "modelProvider": "openai",
            },
            2,
        )
        assert resumed["thread"]["id"] == session_id
        rpc(
            "thread/shellCommand",
            {
                "threadId": session_id,
                "command": f"echo {marker}",
                "timeoutMs": 5000,
            },
            3,
        )
        # Completion can precede the immediate acknowledgement; retain all
        # notifications while waiting for RPC replies.
        completed = None
        completed_turn_id = None
        finished_turn_ids: set[str] = set()
        index = 0
        while completed is None or completed_turn_id not in finished_turn_ids:
            if index == len(notifications):
                notifications.append(receive())
            notification = notifications[index]
            index += 1
            assert index <= 1024, "Codex notification budget exceeded"
            params = notification.get("params", {})
            if params.get("threadId") != session_id:
                continue
            if notification.get("method") == "item/completed":
                item = params["item"]
                if item.get("type") == "commandExecution":
                    completed = item
                    completed_turn_id = params["turnId"]
            if notification.get("method") == "turn/completed":
                finished_turn_ids.add(params["turn"]["id"])
                assert params["turn"]["status"] == "completed", params
        _assert_shell_item(completed, cwd, marker)
        read = rpc("thread/read", {"threadId": session_id, "includeTurns": True}, 4)
        return read, completed
    finally:
        with suppress(OSError):
            proc.stdin.close()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
        worker.join(timeout=2)
        stdout.close()


def _assert_shell_item(item: dict, cwd: Path, marker: str) -> None:
    assert item["type"] == "commandExecution"
    assert item["source"] == "userShell"
    # Codex reports the configured shell invocation, not the raw RPC string.
    assert f"echo {marker}" in item["command"]
    assert Path(item["cwd"]) == cwd
    assert item["status"] == "completed"
    assert item["exitCode"] == 0
    assert item["aggregatedOutput"].strip() == marker


def _persisted_shell(read: dict, item_id: str, cwd: Path, marker: str) -> dict:
    items = [
        item
        for turn in read["thread"]["turns"]
        for item in turn["items"]
        if item["id"] == item_id
    ]
    assert len(items) == 1, "Native shell result absent from durable thread history"
    _assert_shell_item(items[0], cwd, marker)
    return items[0]


def _rollout_bytes(
    session_id: str, cwd: str, model: str, provider: str, *, history: bool = False
) -> bytes:
    """Native-shaped synthetic history; no model or tool is actually run."""
    lines = [
        {
            "timestamp": "2026-10-27T00:00:00.000Z",
            "ordinal": 0,
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "session_id": session_id,
                "timestamp": "2026-10-27T00:00:00.069Z",
                "cwd": cwd,
                "runtime_workspace_roots": [cwd],
                "originator": "untether-runtime-evidence",
                "cli_version": "0.163.0",
                "source": "vscode",
                "model_provider": provider,
                "base_instructions": {
                    "text": "You are a coding agent running in the Codex CLI.",
                    "provenance": {"type": "model", "model": model},
                },
                "history_mode": "paginated",
                "context_window": {"window_id": uuid.uuid4().hex},
            },
        },
        {
            "timestamp": "2026-10-27T00:00:01.000Z",
            "ordinal": 1,
            "type": "event_msg",
            "payload": {
                "type": "thread_settings_applied",
                "thread_settings": {
                    "model": model,
                    "model_provider_id": provider,
                    "approval_policy": "never",
                },
            },
        },
    ]
    if history:
        events = [
            {
                "type": "task_started",
                "turn_id": "turn-probe",
                "model_context_window": 200000,
            },
            *[
                {
                    "type": "item_completed",
                    "thread_id": session_id,
                    "turn_id": "turn-probe",
                    "item": item,
                    "started_at_ms": 1793059200000,
                    "completed_at_ms": 1793059201000,
                }
                for item in [
                    {
                        "type": "UserMessage",
                        "id": "user-probe",
                        "content": [
                            {
                                "type": "text",
                                "text": "History user sentinel",
                                "text_elements": [],
                            }
                        ],
                    },
                    {
                        "type": "AgentMessage",
                        "id": "assistant-probe",
                        "content": [
                            {"type": "Text", "text": "History assistant sentinel"}
                        ],
                        "phase": "final_answer",
                    },
                    {
                        "type": "DynamicToolCall",
                        "id": "tool-probe",
                        "tool": "history_probe",
                        "arguments": {"sentinel": "input"},
                        "status": "completed",
                        "content_items": [
                            {"type": "inputText", "text": "tool-sentinel"}
                        ],
                        "success": True,
                    },
                ]
            ],
            {
                "type": "task_complete",
                "turn_id": "turn-probe",
                "last_agent_message": "History assistant sentinel",
            },
        ]
        lines.extend(
            {
                "timestamp": "2026-10-27T00:00:02.000Z",
                "ordinal": ordinal,
                "type": "event_msg",
                "payload": event,
            }
            for ordinal, event in enumerate(events, 2)
        )
    return "".join(json.dumps(line) + "\n" for line in lines).encode("utf-8")


@pytest.mark.skipif(
    not CODEX_CMD.is_file(),
    reason=f"installed codex not found at {CODEX_CMD}; runtime resume cannot run",
)
@pytest.mark.parametrize("history", [False, True], ids=["metadata-only", "history"])
def test_codex_app_server_resume_after_adapter_restore(
    tmp_path: Path, monkeypatch, history: bool
) -> None:
    session_id = str(uuid.uuid4())
    project_cwd = str(tmp_path)

    # -- source store: synthetic journal with Codex's canonical filename --
    # Paginated rollouts reject rollout-<uuid>.jsonl with RPC -32600; keep
    # the native timestamp component as well as the exact session UUID.
    src_root = tmp_path / "src-home" / "sessions"
    rel = "2026/10/27/rollout-2026-10-27T00-00-00-" + session_id + ".jsonl"
    journal = src_root / rel
    journal.parent.mkdir(parents=True, exist_ok=True)
    model = "gpt-5.1-codex"
    provider = "openai"
    data = _rollout_bytes(session_id, project_cwd, model, provider, history=history)
    journal.write_bytes(data)

    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate-root"))
    exporter = CodexRolloutPersistenceAdapter(
        engine="codex",
        sessions_root=src_root,
        app_server_executable=CODEX_CMD,
        resume_timeout_s=60.0,
    )
    archive = exporter.export_exact(session_id)
    assert archive.native_id == session_id
    stored_name = archive.lineage[len("codex-rollout-v1:") :]
    assert stored_name == rel.replace("\\", "/")
    # canonical record name: session.jsonl carries the journal bytes
    assert archive.records["session.jsonl"] == data

    # -- destination disposable store: restore + real resume proof --
    dst_root = tmp_path / "dst-home" / "sessions"
    dst_root.mkdir(parents=True, exist_ok=True)
    # The disposable CODEX_HOME contains ONLY the sessions tree: no auth.json,
    # no config.toml, no installation_id — resume cannot touch real accounts.
    dst_home = dst_root.parent
    for forbidden in ("auth.json", "config.toml", "installation_id"):
        assert not (dst_home / forbidden).exists()

    importer = CodexRolloutPersistenceAdapter(
        engine="codex",
        sessions_root=dst_root,
        app_server_executable=CODEX_CMD,
        resume_timeout_s=60.0,
    )
    # Use the real default probe inside import_exact (subprocess path).
    proof = importer.import_exact(archive)
    assert proof.native_id == session_id
    assert proof.restarted and proof.resumed
    restored = dst_root / rel
    # Append-only contract: restored bytes are an exact prefix of the journal
    # after the resume (the engine may append thread_settings_applied).
    after = restored.read_bytes()
    assert after[: len(data)] == data
    # A fresh app-server repeats the same strict id/cwd/model/provider echo
    # checks through the production probe, with the same scrubbed environment.
    observed = importer._default_rpc_resume(session_id, restored, 60.0)
    after2 = restored.read_bytes()
    assert after2.startswith(after)  # Preserve even the first probe's append.
    assert isinstance(observed, dict)
    thread = observed["thread"]
    assert thread["id"] == session_id
    if history:
        # Independent literal expectations, not the production visibility check.
        turns = thread["turns"]
        assert len(turns) == 1
        turn = turns[0]
        assert turn["id"] == "turn-probe"
        assert turn["status"] == "completed"
        assert turn["error"] is None
        user, assistant, tool = turn["items"]
        assert (user["type"], user["id"], user["content"]) == (
            "userMessage",
            "user-probe",
            [{"type": "text", "text": "History user sentinel", "text_elements": []}],
        )
        assert (
            assistant["type"],
            assistant["id"],
            assistant["text"],
            assistant["phase"],
        ) == (
            "agentMessage",
            "assistant-probe",
            "History assistant sentinel",
            "final_answer",
        )
        assert (tool["type"], tool["id"], tool["tool"]) == (
            "dynamicToolCall",
            "tool-probe",
            "history_probe",
        )
        assert tool["arguments"] == {"sentinel": "input"}
        assert tool["contentItems"] == [{"type": "inputText", "text": "tool-sentinel"}]
        assert tool["status"] == "completed"
        assert tool["success"] is True
    else:
        assert thread["turns"] == []

    marker = "untetherCodexShell" + uuid.uuid4().hex
    live, completed = _shell_probe(dst_home, tmp_path, session_id, marker)
    live_item = _persisted_shell(live, completed["id"], tmp_path, marker)
    evolved = restored.read_bytes()
    assert evolved.startswith(after2)
    assert marker.encode() in evolved, "Shell output was not durably journalled"

    # A separate app-server must reconstruct the actual completed result.
    cold = importer._default_rpc_resume(session_id, restored, 30.0)
    assert cold["thread"]["id"] == session_id
    assert cold["thread"]["turns"][: len(thread["turns"])] == thread["turns"]
    assert _persisted_shell(cold, completed["id"], tmp_path, marker) == live_item
    assert restored.read_bytes().startswith(evolved)

    returned_archive = importer.export_exact(session_id)
    assert returned_archive.records["session.jsonl"].startswith(evolved)
    return_root = tmp_path / "return-home" / "sessions"
    return_root.mkdir(parents=True)
    receiver = CodexRolloutPersistenceAdapter(
        engine="codex",
        sessions_root=return_root,
        app_server_executable=CODEX_CMD,
        resume_timeout_s=30.0,
    )
    return_proof = receiver.import_exact(returned_archive)
    assert return_proof.native_id == session_id
    assert return_proof.restarted and return_proof.resumed
    returned = return_root / rel
    assert returned.read_bytes().startswith(returned_archive.records["session.jsonl"])
    returned_read = receiver._default_rpc_resume(session_id, returned, 30.0)
    assert returned_read["thread"]["id"] == session_id
    assert returned_read["thread"]["turns"] == cold["thread"]["turns"]
    assert (
        _persisted_shell(returned_read, completed["id"], tmp_path, marker) == live_item
    )
