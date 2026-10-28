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

This mirrors the Pi runtime test: the adapter's own default resume probe IS
the runtime evidence; failure is asserted, never skipped, while the installed
command exists.
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


def _rollout_bytes(session_id: str, cwd: str, model: str, provider: str) -> bytes:
    """Synthetic native-shaped header and settings event; no model turns."""
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
    return "".join(json.dumps(line) + "\n" for line in lines).encode("utf-8")


@pytest.mark.skipif(
    not CODEX_CMD.is_file(),
    reason=f"installed codex not found at {CODEX_CMD}; runtime resume cannot run",
)
def test_codex_app_server_resume_after_adapter_restore(
    tmp_path: Path, monkeypatch
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
    data = _rollout_bytes(session_id, project_cwd, model, provider)
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
    importer._run_resume_probe(session_id, restored, data)
    after2 = restored.read_bytes()
    assert after2[: len(data)] == data
