"""Opt-in installed-engine restore proof; no models, plugins or real user stores.

Run with UNTETHER_TEST_OPENCODE_EXE pointing at the installed native executable.
Only owned serve children are stopped; every store lives under pytest's tmp_path.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
from contextlib import contextmanager
from pathlib import Path

import pytest

from untether.session_migration.opencode_sync_adapter import (
    OpenCodeSyncAdapter,
    _maybe_kill,
)
from untether.session_migration.settings import OpenCodeSyncProfile


@contextmanager
def _running(adapter):
    proc = adapter._serve("runtime")
    try:
        yield proc
    finally:
        _maybe_kill(proc)
        assert proc.poll() is not None


def _ok(adapter, path, method="GET", data=None):
    status, body = adapter._request(path, method, data)
    assert status == 200, (path, status, body)
    return body


def _tool_outputs(messages):
    return [
        part["state"]["output"]
        for message in messages
        for part in message["parts"]
        if part["type"] == "tool" and part["state"]["status"] == "completed"
    ]


def test_installed_opencode_adapter_restore_restart(tmp_path, monkeypatch):
    executable = os.environ.get("UNTETHER_TEST_OPENCODE_EXE")
    if not executable:
        pytest.skip(
            "set UNTETHER_TEST_OPENCODE_EXE for disposable installed-engine proof"
        )
    assert Path(executable).is_file()
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    # Ephemeral loopback port per test; production still has a bind/readiness race.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    def adapter(name):
        root = tmp_path / name
        root.mkdir()
        return OpenCodeSyncAdapter(
            engine="opencode",
            profile=OpenCodeSyncProfile(
                format="opencode-sync-v1",
                executable=executable,
                data_root=str(root),
                port=port,
                request_timeout_s=20,
                serve_start_timeout_s=20,
            ),
        )

    source, destination = adapter("source"), adapter("destination")
    admitted = "disposable adapter admitted input"
    initial = "adapter-original-shell-ok"
    followup = "adapter-followup-shell-ok"
    with _running(source):
        sid = _ok(source, "/session", "POST", {"title": "adapter source"})["id"]
        status, body = source._request(
            f"/api/session/{sid}/prompt",
            "POST",
            {"prompt": {"text": admitted}, "resume": False},
        )
        assert status in (200, 202), body
        _ok(
            source,
            f"/session/{sid}/shell",
            "POST",
            {"agent": "build", "command": f"echo {initial}"},
        )
        messages = _ok(source, f"/session/{sid}/message")
        events = source._collect_events(sid)
        assert admitted in json.dumps(events)
        # resume:false is durable admission, not a projected user message.
        assert any(
            event["type"] == "session.next.prompt.admitted.1" for event in events
        )
        assert any(initial in output for output in _tool_outputs(messages))
    archive = source.export_exact(sid)
    assert json.loads(archive.records["sync-events.json"]) == events

    with _running(destination):
        keep = _ok(destination, "/session", "POST", {"title": "unrelated"})
        keep_events = destination._collect_events(keep["id"])
        keep_messages = _ok(destination, f"/session/{keep['id']}/message")

    # Observe real lifecycle, not fake serve/http seams: import must stop its
    # writer and read the persisted history from an independently started child.
    starts = []
    original_serve = OpenCodeSyncAdapter._serve

    def observed_serve(self, label):
        proc = original_serve(self, label)
        assert isinstance(proc, subprocess.Popen)
        if self is destination:
            if starts:
                assert starts[-1].poll() is not None
            starts.append(proc)
        return proc

    with monkeypatch.context() as patch:
        patch.setattr(OpenCodeSyncAdapter, "_serve", observed_serve)
        proof = destination.import_exact(archive)
    assert len(starts) == 2, "restore proof must include independent restart"
    assert all(proc.poll() is not None for proc in starts)
    assert proof.native_id == sid
    assert proof.restarted and proof.resumed

    with _running(destination):
        assert _ok(destination, f"/session/{sid}")["id"] == sid
        assert destination._collect_events(sid) == events
        assert _ok(destination, f"/session/{sid}/message") == messages
        assert _ok(destination, f"/session/{keep['id']}") == keep
        assert destination._collect_events(keep["id"]) == keep_events
        assert _ok(destination, f"/session/{keep['id']}/message") == keep_messages
        _ok(
            destination,
            f"/session/{sid}/shell",
            "POST",
            {"agent": "build", "command": f"echo {followup}"},
        )
        after = _ok(destination, f"/session/{sid}/message")
        outputs = _tool_outputs(after)
        assert any(initial in output for output in outputs)
        assert any(followup in output for output in outputs)
        assert admitted in json.dumps(destination._collect_events(sid))
        assert destination._collect_events(sid)[: len(events)] == events
    with _running(destination):
        assert _ok(destination, f"/session/{sid}/message") == after
        assert destination._collect_events(keep["id"]) == keep_events
