"""Tests for the ACP (Agent Client Protocol) client and compact mixin.

These test the ACP JSON-RPC client using the in-memory FakeAcpTransport,
and the AcpCompactMixin's event emission, error handling, and capability
gating.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from untether.model import CompletedEvent, EngineId, ResumeToken
from untether.runners._acp import (
    AcpClient,
    AcpCommandUnavailableError,
    AcpCompactMixin,
    AcpProtocolError,
    FakeAcpTransport,
    SubprocessAcpTransport,
    _acp_client_context,
    _extract_available_commands,
    _parse_message,
)


class TestParseMessage:
    def test_valid_json(self) -> None:
        msg = _parse_message(b'{"jsonrpc": "2.0", "id": 1}')
        assert msg is not None
        assert msg["jsonrpc"] == "2.0"

    def test_empty_bytes(self) -> None:
        assert _parse_message(b"") is None

    def test_whitespace_only(self) -> None:
        assert _parse_message(b"   \n  ") is None

    def test_not_a_dict(self) -> None:
        assert _parse_message(b"[1, 2, 3]") is None

    def test_malformed_json_raises(self) -> None:
        with pytest.raises(AcpProtocolError, match="malformed JSON"):
            _parse_message(b"{invalid json")

    def test_unicode_decode_error(self) -> None:
        assert _parse_message(b"\xff\xfe") is None

    @pytest.mark.parametrize(
        "notification",
        [
            {"method": "session/update", "params": {"update": []}},
            {
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "available_commands_update",
                        "availableCommands": {},
                    }
                },
            },
        ],
    )
    def test_rejects_malformed_available_commands_update(
        self, notification: dict[str, Any]
    ) -> None:
        assert _extract_available_commands(notification) is None


class TestExtractAvailableCommands:
    def test_correct_envelope(self) -> None:
        notification = {
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": [
                        {"name": "compact"},
                        {"name": "chat"},
                        {"name": "compact"},  # dedup
                    ],
                }
            },
        }
        result = _extract_available_commands(notification)
        assert result == {"compact", "chat"}

    def test_wrong_method(self) -> None:
        notification = {"method": "other/method", "params": {}}
        assert _extract_available_commands(notification) is None

    def test_wrong_session_update(self) -> None:
        notification = {
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "other_update"}},
        }
        assert _extract_available_commands(notification) is None

    def test_missing_params(self) -> None:
        notification = {"method": "session/update"}
        assert _extract_available_commands(notification) is None

    def test_non_dict_params(self) -> None:
        notification = {"method": "session/update", "params": "not dict"}
        assert _extract_available_commands(notification) is None

    def test_non_list_commands(self) -> None:
        notification = {
            "method": "session/update",
            "params": {"update": {"availableCommands": "not a list"}},
        }
        assert _extract_available_commands(notification) is None

    def test_filters_empty_names(self) -> None:
        notification = {
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": [
                        {"name": "compact"},
                        {"name": ""},
                        {"no_name": "missing"},
                        "not a dict",
                    ],
                }
            },
        }
        result = _extract_available_commands(notification)
        assert result == {"compact"}


class TestFakeAcpTransport:
    @pytest.mark.anyio
    async def test_start_close_noop(self) -> None:
        transport = FakeAcpTransport()
        await transport.start()
        await transport.close()

    @pytest.mark.anyio
    async def test_send_request_records(self) -> None:
        transport = FakeAcpTransport()
        transport.queue_response("initialize", {"protocolVersion": 1})
        result = await transport.send_request(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        )
        assert result == {"protocolVersion": 1}
        assert len(transport.requests) == 1
        assert transport.requests[0]["method"] == "initialize"

    @pytest.mark.anyio
    async def test_send_request_unknown_method_returns_empty(self) -> None:
        transport = FakeAcpTransport()
        result = await transport.send_request(
            {"jsonrpc": "2.0", "id": 1, "method": "unknown", "params": {}}
        )
        assert result == {}

    @pytest.mark.anyio
    async def test_read_notification(self) -> None:
        transport = FakeAcpTransport()
        transport.emit_notification("session/update", {"key": "value"})
        notification = await transport.read_notification()
        assert notification is not None
        assert notification["method"] == "session/update"


def _initialized_transport() -> FakeAcpTransport:
    transport = FakeAcpTransport()
    transport.queue_response(
        "initialize",
        {
            "protocolVersion": 1,
            "agentCapabilities": {"loadSession": True},
        },
    )
    return transport


@pytest.mark.anyio
async def test_acp_client_initializes_loads_and_prompts() -> None:
    transport = _initialized_transport()
    transport.queue_response("session/load", {})
    transport.queue_response("session/prompt", {"stopReason": "end_turn"})
    client = AcpClient(command="unused", args=[], transport=transport)

    await client.initialize()
    await client.resume_or_load("session")
    updates = [update async for update in client.prompt("session", "hello")]

    assert [request["method"] for request in transport.requests] == [
        "initialize",
        "session/load",
        "session/prompt",
    ]
    assert updates[0].kind == "stop"
    assert updates[0].stop_reason == "end_turn"


@pytest.mark.anyio
async def test_acp_client_rejects_unsupported_protocol() -> None:
    transport = FakeAcpTransport()
    transport.queue_response("initialize", {"protocolVersion": 99})
    client = AcpClient(command="unused", args=[], transport=transport)

    with pytest.raises(AcpProtocolError, match="unsupported protocol"):
        await client.initialize()


@pytest.mark.anyio
async def test_acp_client_uses_resume_capability_when_load_is_unavailable() -> None:
    transport = _initialized_transport()
    transport._responses["initialize"] = {
        "protocolVersion": 1,
        "agentCapabilities": {"sessionCapabilities": {"resume": True}},
    }
    transport.queue_response("session/resume", {})
    client = AcpClient(command="unused", args=[], transport=transport)

    await client.initialize()
    await client.resume_or_load("session")

    assert transport.requests[-1]["method"] == "session/resume"


@pytest.mark.anyio
async def test_acp_client_rejects_missing_resume_capability() -> None:
    transport = _initialized_transport()
    transport._responses["initialize"] = {
        "protocolVersion": 1,
        "agentCapabilities": {},
    }
    client = AcpClient(command="unused", args=[], transport=transport)

    await client.initialize()
    with pytest.raises(AcpCommandUnavailableError, match="cannot load/resume"):
        await client.resume_or_load("session")


@pytest.mark.anyio
async def test_acp_client_waits_for_available_commands_and_requires_them() -> None:
    transport = _initialized_transport()
    transport.emit_notification("session/update", {"unrelated": True})
    transport.emit_notification(
        "session/update",
        {
            "update": {
                "sessionUpdate": "available_commands_update",
                "availableCommands": [{"name": "compact"}],
            }
        },
    )
    client = AcpClient(command="unused", args=[], transport=transport)

    assert await client.wait_for_available_commands() == {"compact"}
    await client.require_command("compact")
    with pytest.raises(AcpCommandUnavailableError, match="advertise 'other'"):
        await client.require_command("other")


@pytest.mark.anyio
async def test_acp_client_rejects_missing_command_advertisement() -> None:
    transport = _initialized_transport()
    client = AcpClient(command="unused", args=[], transport=transport)

    async def no_notification() -> dict[str, Any] | None:
        return None

    cast(Any, transport).read_notification = no_notification
    with pytest.raises(AcpCommandUnavailableError, match="did not advertise"):
        await client.wait_for_available_commands()


@dataclass
class _CompactRunner(AcpCompactMixin):
    engine: EngineId = "codex"
    close_timeout_s: float = 5.0
    startup_timeout_s: float | None = None
    shutdown_timeout_s: float = 5.0
    kill_tree_on_cancel: bool = True
    transport: FakeAcpTransport = field(default_factory=_initialized_transport)

    def command(self) -> str:
        return "unused"

    def create_acp_client(self) -> AcpClient:
        return AcpClient(command=self.command(), args=[], transport=self.transport)


@pytest.mark.anyio
async def test_acp_compact_reports_capability_and_completes() -> None:
    runner = _CompactRunner()
    runner.transport.queue_response("session/load", {})
    runner.transport.queue_response("session/prompt", {})
    runner.transport.emit_notification(
        "session/update",
        {
            "update": {
                "sessionUpdate": "available_commands_update",
                "availableCommands": [{"name": "compact"}],
            }
        },
    )

    events = [
        event
        async for event in runner.compact(
            ResumeToken(engine="codex", value="session"), "focus"
        )
    ]

    assert runner.compact_support().mode == "acp"
    assert [event.type for event in events] == ["started", "completed"]
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].ok is True
    assert runner.transport.requests[-1]["params"]["prompt"] == [
        {"type": "text", "text": "/compact focus"}
    ]


@pytest.mark.anyio
async def test_acp_compact_rejects_resume_for_another_engine() -> None:
    events = [
        event
        async for event in _CompactRunner().compact(
            ResumeToken(engine="pi", value="session")
        )
    ]

    assert len(events) == 1
    assert isinstance(events[0], CompletedEvent)
    assert events[0].ok is False
    assert "!= runner" in (events[0].error or "")


@pytest.mark.anyio
async def test_acp_compact_reports_agent_capability_failures() -> None:
    runner = _CompactRunner()
    events = [
        event
        async for event in runner.compact(ResumeToken(engine="codex", value="session"))
    ]

    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].ok is False
    assert "did not advertise" in (events[-1].error or "")


@pytest.mark.anyio
async def test_acp_client_context_starts_and_closes_transport() -> None:
    class _CountingTransport(FakeAcpTransport):
        def __init__(self) -> None:
            super().__init__()
            self.started = False
            self.closed = False

        async def start(self) -> None:
            self.started = True

        async def close(self) -> None:
            self.closed = True

    transport = _CountingTransport()
    client = AcpClient(command="unused", args=[], transport=transport)
    async with _acp_client_context(client) as active:
        assert active is client
        assert transport.started is True


def _acp_server(body: str) -> list[str]:
    return ["-u", "-c", "import json, sys\n" + body]


@pytest.mark.anyio
async def test_subprocess_acp_transport_handles_server_request_then_response() -> None:
    transport = SubprocessAcpTransport(
        sys.executable,
        _acp_server(
            "request = json.loads(sys.stdin.readline())\n"
            "print(json.dumps({'jsonrpc':'2.0','id':'server','method':'ping'}), flush=True)\n"
            "reply = json.loads(sys.stdin.readline())\n"
            "assert reply['error']['code'] == -32601\n"
            "print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':{'ok':True}}), flush=True)\n"
        ),
    )
    await transport.start()
    try:
        assert await transport.send_request({"id": 7, "method": "test"}) == {"ok": True}
    finally:
        await transport.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            "request = json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':99,'result':{}}), flush=True)\n",
            "id mismatch",
        ),
        (
            "request = json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':request['id'],'error':{'code':1,'message':'bad'}}), flush=True)\n",
            "JSON-RPC error 1",
        ),
        (
            "request = json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':[]}), flush=True)\n",
            "result is not an object",
        ),
        (
            "request = json.loads(sys.stdin.readline())\nprint(json.dumps({'jsonrpc':'2.0','method':'update'}), flush=True)\n",
            "unexpected notification",
        ),
    ],
)
async def test_subprocess_acp_transport_rejects_invalid_responses(
    body: str, message: str
) -> None:
    transport = SubprocessAcpTransport(sys.executable, _acp_server(body))
    await transport.start()
    try:
        with pytest.raises(AcpProtocolError, match=message):
            await transport.send_request({"id": 7, "method": "test"})
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_subprocess_acp_transport_reads_notification_and_times_out() -> None:
    transport = SubprocessAcpTransport(
        sys.executable,
        _acp_server(
            "print(json.dumps({'jsonrpc':'2.0','method':'session/update','params':{}}), flush=True)\nsys.stdin.readline()\n"
        ),
        request_timeout_s=0.5,
    )
    await transport.start()
    try:
        assert await transport.read_notification() == {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {},
        }
        assert await transport.read_notification() is None
    finally:
        await transport.close()
    assert transport._proc is None


@pytest.mark.anyio
async def test_subprocess_acp_transport_rejects_empty_response() -> None:
    transport = SubprocessAcpTransport(
        sys.executable,
        _acp_server("sys.stdin.readline()\nprint('', flush=True)\n"),
    )
    await transport.start()
    try:
        with pytest.raises(AcpProtocolError, match="non-object message"):
            await transport.send_request({"id": 7, "method": "test"})
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_subprocess_acp_transport_discards_response_as_notification() -> None:
    transport = SubprocessAcpTransport(
        sys.executable,
        _acp_server(
            "print(json.dumps({'jsonrpc':'2.0','id':7,'result':{}}), flush=True)\nsys.stdin.readline()\n"
        ),
        request_timeout_s=0.5,
    )
    await transport.start()
    try:
        assert await transport.read_notification() is None
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_subprocess_acp_transport_start_is_idempotent_and_passes_environment(
    tmp_path: Any,
) -> None:
    transport = SubprocessAcpTransport(
        sys.executable,
        _acp_server(
            "import os\n"
            "request = json.loads(sys.stdin.readline())\n"
            "assert os.environ['ACP_TEST_VALUE'] == 'present'\n"
            "assert os.getcwd() == sys.argv[1]\n"
            "print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':{}}), flush=True)\n"
        )
        + [str(tmp_path)],
        cwd=str(tmp_path),
        env={"ACP_TEST_VALUE": "present"},
    )
    await transport.start()
    process = transport._proc
    await transport.start()
    try:
        assert transport._proc is process
        assert await transport.send_request({"id": 7, "method": "test"}) == {}
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_subprocess_acp_transport_close_is_idempotent_before_start() -> None:
    transport = SubprocessAcpTransport("unused", [])

    await transport.close()
    await transport.close()

    assert transport._proc is None
    assert transport._stdout is None


def test_acp_client_creates_configured_subprocess_transport() -> None:
    client = AcpClient(
        command="agent",
        args=["--acp"],
        cwd="worktree",
        env={"KEY": "value"},
        close_timeout_s=1.0,
        request_timeout_s=2.0,
        shutdown_timeout_s=3.0,
        kill_tree_on_cancel=False,
    )

    transport = client._resolve_transport()

    assert isinstance(transport, SubprocessAcpTransport)
    assert transport.command == "agent"
    assert transport.args == ["--acp"]
    assert transport.cwd == "worktree"
    assert transport.env == {"KEY": "value"}
    assert transport.close_timeout_s == 1.0
    assert transport.request_timeout_s == 2.0
    assert transport.shutdown_timeout_s == 3.0
    assert transport.kill_tree_on_cancel is False


@pytest.mark.anyio
async def test_acp_client_wraps_request_timeout() -> None:
    class _SlowTransport(FakeAcpTransport):
        async def send_request(self, request: dict[str, Any]) -> dict[str, Any]:
            await asyncio.sleep(60)
            return {}

    client = AcpClient(
        command="unused", args=[], request_timeout_s=0.01, transport=_SlowTransport()
    )

    with pytest.raises(AcpProtocolError, match="ACP initialize request timed out"):
        await client.initialize()


@pytest.mark.anyio
async def test_acp_client_uses_current_directory_for_resume_without_cwd(
    monkeypatch,
) -> None:
    transport = _initialized_transport()
    transport.queue_response("session/load", {})
    client = AcpClient(command="unused", args=[], transport=transport)
    monkeypatch.setattr("untether.runners._acp.os.getcwd", lambda: "current-directory")

    await client.initialize()
    await client.resume_or_load("session")

    assert transport.requests[-1]["params"]["cwd"] == "current-directory"


def test_acp_compact_default_client_requires_override() -> None:
    with pytest.raises(NotImplementedError, match="must implement"):
        AcpCompactMixin().create_acp_client()
