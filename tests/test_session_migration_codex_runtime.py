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
with journal-derived settings only).

A second fresh app-server uses the existing probe's returned thread/read result
for independent literal history assertions. Synthetic completed-tool history is
not evidence of actual tool execution or full model continuation. Failure is
asserted, never skipped, while the installed command exists.
"""

from __future__ import annotations

import json
import os
import uuid
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
