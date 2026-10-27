"""In-process discovery RPC evidence, not SSH or engine deployment evidence."""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from untether.session_migration import native_adapter, remote
from untether.session_migration.file_bundle_adapter import FileBundlePersistenceAdapter
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    ReceiverProtocolError,
    handle_receiver,
)
from untether.session_migration.settings import (
    EnginePersistenceProfile,
    MigrationPeer,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS, PreflightError


def _snapshot(root: Path):
    """Include directories as well as bytes to detect empty fence/cache writes."""
    return {
        str(path.relative_to(root)): path.read_bytes() if path.is_file() else None
        for path in root.rglob("*")
    }


@pytest.fixture
def discovery(tmp_path, monkeypatch):
    store = tmp_path / "sessions"
    store.mkdir()
    for sid, cwd in (
        ("session-one", "/work/demo"),
        ("session-two", "/work/demo"),
        ("foreign", "/work/other"),
    ):
        (store / f"{sid}.jsonl").write_text(
            json.dumps({"type": "session", "version": 3, "id": sid, "cwd": cwd})
            + '\n{"type":"message","text":"private"}\n',
            encoding="utf-8",
        )
    profile = EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(store),
        session_record="pi",
        restart_argv=("unused", "{session_path}", "{session_id}"),
    )
    adapter = FileBundlePersistenceAdapter("pi", profile, store)
    monkeypatch.setitem(native_adapter._ADAPTERS, "pi", lambda: adapter)

    def unrelated():
        raise AssertionError("discovery must not enumerate unrelated local adapters")

    monkeypatch.setitem(native_adapter._ADAPTERS, "unrelated", unrelated)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gates"))
    endpoint = ReceiverEndpoint(
        endpoint_id="endpoint-one",
        execution_mode="native",
        destination_binding="enrolled",
        project="demo",
        deployment="test",
        arch="x86_64",
        probe_health=lambda: dict.fromkeys(_REQUIRED_CHECKS, True),
        engine="pi",
        project_root="/work/demo",
        native_receiver=NativeReceiver(
            tmp_path / "receiver", max_archive=65536, max_expanded=65536
        ),
    )
    peer = MigrationPeer(
        host="example.invalid",
        user="operator",
        identity_dir=str(tmp_path / "keys"),
        identity_file="selected",
        known_hosts_file=str(tmp_path / "known"),
        host_key_fingerprint="SHA256:" + "A" * 43,
        receiver_path="/usr/local/bin/fixed-receiver",
        execution_mode="native",
        expected_endpoint_id="endpoint-one",
        destination_binding="enrolled",
        projects={"demo": "/work/demo"},
    )
    preflights = []

    def preflight(selected, **kwargs):
        assert selected is peer
        assert kwargs["project"] == "demo"
        assert kwargs["direction"] == "from"
        preflights.append(kwargs)

    monkeypatch.setattr(remote, "inspect_peer", preflight)
    monkeypatch.setattr(
        remote,
        "_identity",
        lambda selected: (tmp_path / "selected", tmp_path / "known"),
    )
    return peer, endpoint, preflights


def _transport(endpoint, frames):
    def run(argv, *, input, capture_output, timeout, check):
        assert argv[0] == "ssh"
        assert argv[-1] == "/usr/local/bin/fixed-receiver inspect"
        assert "StrictHostKeyChecking=yes" in argv
        assert capture_output and not check and timeout > 0
        header, separator, body = input.partition(b"\n")
        assert separator and not body
        frame = json.loads(header)
        assert set(frame) == {
            "protocol",
            "action",
            "operation_id",
            "project",
            "session_id",
        }
        assert frame["protocol"] == 1 and frame["action"] == "inspect"
        frames.append(frame)
        try:
            reply = handle_receiver(endpoint, verb="inspect", payload=input)
        except ReceiverProtocolError as exc:
            assert str(exc) == "SESSION_PROJECT_REFUSED"  # noqa: PT017 -- subprocess exit mapping
            return subprocess.CompletedProcess(argv, 1, b"", b"refused")
        assert isinstance(reply, bytes)
        assert json.loads(reply)["session_id"] == frame["session_id"]
        return subprocess.CompletedProcess(argv, 0, reply, b"")

    return run


def test_two_sessions_discover_through_real_receiver_without_writes(
    tmp_path, discovery
):
    peer, endpoint, preflights = discovery
    before = _snapshot(tmp_path)
    frames = []
    for sid in ("session-one", "session-two"):
        assert (
            remote.inspect_session(peer, "demo", sid, run=_transport(endpoint, frames))
            == "pi"
        )
        assert _snapshot(tmp_path) == before
    assert [frame["session_id"] for frame in frames] == ["session-one", "session-two"]
    assert frames[0]["operation_id"] != frames[1]["operation_id"]
    assert len(preflights) == 2


@pytest.mark.parametrize("mismatch", ["foreign-session", "receiver-root"])
def test_project_mismatch_refuses_without_writes(tmp_path, discovery, mismatch):
    peer, endpoint, preflights = discovery
    sid = "foreign" if mismatch == "foreign-session" else "session-one"
    if mismatch == "receiver-root":
        endpoint = replace(endpoint, project_root="/work/not-demo")
    before = _snapshot(tmp_path)
    frames = []
    with pytest.raises(PreflightError, match=r"^RECEIVER_REFUSED$"):
        remote.inspect_session(peer, "demo", sid, run=_transport(endpoint, frames))
    assert len(frames) == len(preflights) == 1
    assert _snapshot(tmp_path) == before
