"""Tests for the ACP compaction mixin on the new AcpPeer stack.

These cover the capability-gated ``/compact`` flow: resume, wait for the
advertised command list, gate on a real ``compact`` command, and issue the
``session/prompt`` compact payload.
"""

from __future__ import annotations

from typing import Any

import pytest

from untether.model import CompletedEvent, ResumeToken
from untether.runners.acp.backend import _BackendRunner
from untether.runners.acp.compact import AcpCompactMixin


class FakeCompactPeer:
    """In-memory peer for compaction: scripted notifications + requests."""

    def __init__(
        self,
        *,
        commands: list[str] | None = None,
        stream_then_commands: bool = False,
        stop_reason: str = "end_turn",
    ):
        self.commands = commands
        self.stream_then_commands = stream_then_commands
        self.stop_reason = stop_reason
        self.closed = False
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.allow_batches = False
        self._emitted = False

    async def start(self) -> None:
        return None

    async def request(self, method: str, params: dict[str, Any], **_: Any):
        self.requests.append((method, params))
        if method == "initialize":
            return {
                "protocolVersion": 1,
                "agentCapabilities": {
                    "loadSession": True,
                    "sessionCapabilities": {"close": True, "resume": True},
                },
            }
        if method in {"session/new", "session/load", "session/resume"}:
            return {"sessionId": params.get("sessionId", "s1")}
        if method == "session/prompt":
            return {"stopReason": self.stop_reason}
        return {}

    async def next_notification(self) -> dict[str, Any]:
        if self.commands is None:
            raise RuntimeError("peer closed")
        if self.stream_then_commands:
            self.stream_then_commands = False
            return {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": "ignored",
                    }
                },
            }
        if not self._emitted:
            self._emitted = True
            return self._commands_notification()
        raise RuntimeError("peer closed")

    def _commands_notification(self) -> dict[str, Any]:
        assert self.commands is not None
        commands = self.commands
        return {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": [{"name": name} for name in commands],
                }
            },
        }

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        self.requests.append((method, params))

    async def close(self) -> None:
        self.closed = True


def _runner(peer: FakeCompactPeer) -> _BackendRunner:
    return _BackendRunner(engine="acp", command="unused", peer_factory=lambda: peer)


@pytest.mark.anyio
async def test_compact_support_declares_acp_mode() -> None:
    runner = _runner(FakeCompactPeer(commands=["compact"]))
    support = runner.compact_support()
    assert support.mode == "acp"
    assert support.accepts_instructions is True
    assert support.true_compaction is True


@pytest.mark.anyio
async def test_compact_gates_on_advertised_command_and_prompts() -> None:
    peer = FakeCompactPeer(commands=["chat", "compact"])
    runner = _runner(peer)
    events = [
        event
        async for event in runner.compact(
            ResumeToken(engine="acp", value="session"), "focus"
        )
    ]

    assert [event.type for event in events] == ["started", "completed"]
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].ok is True
    methods = [request[0] for request in peer.requests]
    assert "session/load" in methods or "session/resume" in methods
    prompt = next(
        request for request in peer.requests if request[0] == "session/prompt"
    )
    assert prompt[1]["prompt"] == [{"type": "text", "text": "/compact focus"}]
    assert peer.closed is True


@pytest.mark.anyio
async def test_compact_without_instructions_uses_bare_prompt() -> None:
    peer = FakeCompactPeer(commands=["compact"])
    runner = _runner(peer)
    events = [
        event
        async for event in runner.compact(ResumeToken(engine="acp", value="s"), None)
    ]
    assert isinstance(events[-1], CompletedEvent) and events[-1].ok is True
    prompt = next(
        request for request in peer.requests if request[0] == "session/prompt"
    )
    assert prompt[1]["prompt"] == [{"type": "text", "text": "/compact"}]


@pytest.mark.anyio
async def test_compact_skips_non_command_notifications_before_advertisement() -> None:
    peer = FakeCompactPeer(commands=["compact"], stream_then_commands=True)
    runner = _runner(peer)
    events = [
        event async for event in runner.compact(ResumeToken(engine="acp", value="s"))
    ]
    assert isinstance(events[-1], CompletedEvent) and events[-1].ok is True


@pytest.mark.anyio
async def test_compact_rejects_resume_for_another_engine() -> None:
    runner = _runner(FakeCompactPeer(commands=["compact"]))
    events = [
        event
        async for event in runner.compact(ResumeToken(engine="pi", value="session"))
    ]
    assert len(events) == 1
    assert isinstance(events[0], CompletedEvent)
    assert events[0].ok is False
    assert "!= runner" in (events[0].error or "")


@pytest.mark.anyio
async def test_compact_reports_agent_capability_failures() -> None:
    runner = _runner(FakeCompactPeer(commands=["chat"]))
    events = [
        event async for event in runner.compact(ResumeToken(engine="acp", value="s"))
    ]
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].ok is False
    assert "did not advertise" in (events[-1].error or "")


@pytest.mark.anyio
async def test_compact_reports_missing_advertisement_stream() -> None:
    runner = _runner(FakeCompactPeer(commands=None))
    events = [
        event async for event in runner.compact(ResumeToken(engine="acp", value="s"))
    ]
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].ok is False


@pytest.mark.anyio
async def test_compact_closes_peer_on_success_and_failure() -> None:
    ok_peer = FakeCompactPeer(commands=["compact"])
    runner = _runner(ok_peer)
    _ = [event async for event in runner.compact(ResumeToken(engine="acp", value="s"))]
    assert ok_peer.closed is True

    fail_peer = FakeCompactPeer(commands=[])
    runner = _runner(fail_peer)
    _ = [event async for event in runner.compact(ResumeToken(engine="acp", value="s"))]
    assert fail_peer.closed is True


def test_mixin_public_surface() -> None:
    assert AcpCompactMixin.compact_accepts_instructions is True
    assert callable(AcpCompactMixin.compact)
    assert callable(AcpCompactMixin.compact_support)
