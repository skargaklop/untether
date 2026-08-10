"""ACP (Agent Client Protocol) JSON-RPC stdio client.

Used for ``/compact`` on ACP-capable runners (grok, omp). The client
communicates over JSON-RPC 2.0, using stdio in production (via
:class:`SubprocessAcpTransport`) and an in-memory transport
(:class:`FakeAcpTransport`) in tests.

Protocol flow for compact::

    initialize -> session/load or session/resume -> require advertised compact
    -> session/prompt with /compact text -> map updates to Untether events

``session/new`` is never used in the compact path.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import os
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, Protocol

import anyio
from anyio.abc import Process, TaskGroup
from anyio.streams.buffered import BufferedByteReceiveStream

from ..events import EventFactory
from ..logging import get_logger
from ..model import EngineId, ResumeToken, UntetherEvent
from ..utils.streams import drain_stderr
from ..utils.subprocess import manage_subprocess

logger = get_logger(__name__)

#: JSON-RPC protocol version tag for every outgoing object.
_JSONRPC_VERSION = "2.0"

#: ACP protocol version this client requests.
_PROTOCOL_VERSION = 1

type JsonObject = dict[str, Any]


class AcpProtocolError(RuntimeError):
    """Raised for invalid framing, EOF, response correlation failures,
    timeouts, protocol-version mismatch, and JSON-RPC errors."""


class AcpCommandUnavailableError(RuntimeError):
    """Raised when the ACP agent does not advertise a required command."""


class AcpTransport(Protocol):
    """Async transport contract for ACP JSON-RPC communication."""

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def send_request(self, request: JsonObject) -> JsonObject: ...

    async def read_notification(self) -> JsonObject | None: ...


@dataclass(slots=True)
class AcpUpdate:
    """A decoded ACP session update, ready for translation to events."""

    kind: str  # "message" | "thought" | "tool" | "plan" | "stop"
    text: str = ""
    stop_reason: str | None = None


# ---------------------------------------------------------------------------
# Subprocess transport (production)
# ---------------------------------------------------------------------------


@dataclass
class SubprocessAcpTransport:
    """Production stdio JSON-RPC transport over a managed subprocess.

    Reuses :func:`manage_subprocess` for cross-platform process-tree
    termination and bounded stream cleanup. Exactly one request is
    in-flight at a time (the ACP compact flow is strictly sequential).
    """

    command: str
    args: list[str]
    cwd: str | None = None
    env: dict[str, str] | None = None
    close_timeout_s: float = 5.0
    request_timeout_s: float = 60.0
    shutdown_timeout_s: float = 5.0
    kill_tree_on_cancel: bool = True

    def __post_init__(self) -> None:
        self._proc: Process | None = None
        self._stdout: BufferedByteReceiveStream | None = None
        self._stdin_lock = anyio.Lock()
        self._stderr_tg: TaskGroup | None = None
        self._stderr_cancel_scope: anyio.CancelScope | None = None
        self._ctx: AbstractAsyncContextManager[Process] | None = None

    async def start(self) -> None:
        if self._proc is not None:
            return
        cmd = [self.command, *self.args]
        kwargs: dict[str, Any] = {}
        if self.cwd is not None:
            kwargs["cwd"] = self.cwd
        kwargs["stdin"] = asyncio.subprocess.PIPE
        kwargs["stdout"] = asyncio.subprocess.PIPE
        kwargs["stderr"] = asyncio.subprocess.PIPE
        if self.env is not None:
            kwargs["env"] = self.env
        self._ctx = manage_subprocess(
            cmd,
            shutdown_timeout_s=self.shutdown_timeout_s,
            kill_tree_on_cancel=self.kill_tree_on_cancel,
            **kwargs,
        )
        proc = await self._ctx.__aenter__()
        self._proc = proc
        if proc.stdout is None or proc.stdin is None or proc.stderr is None:
            await self._shutdown_proc(proc)
            self._proc = None
            self._ctx = None
            raise AcpProtocolError("ACP subprocess did not provide stdio pipes")
        self._stdout = BufferedByteReceiveStream(proc.stdout)
        self._stderr_tg = anyio.create_task_group()
        await self._stderr_tg.__aenter__()
        self._stderr_cancel_scope = anyio.CancelScope()
        self._stderr_tg.start_soon(self._drain_stderr_task)

    async def _drain_stderr_task(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        with self._stderr_cancel_scope or anyio.CancelScope():
            await drain_stderr(self._proc.stderr, logger, tag="acp")

    async def send_request(self, request: JsonObject) -> JsonObject:
        assert self._proc is not None and self._proc.stdin is not None
        assert self._stdout is not None
        line = json.dumps(request, separators=(",", ":")) + "\n"
        async with self._stdin_lock:
            await self._proc.stdin.send(line.encode("utf-8"))
        request_id = request.get("id")
        while True:
            with anyio.fail_after(self.request_timeout_s):
                raw = await self._stdout.receive_until(b"\n", 10 * 1024 * 1024)
            msg = _parse_message(raw)
            if msg is None:
                raise AcpProtocolError("ACP agent sent a non-object message")
            if "method" in msg and "id" in msg:
                # Server request — we don't support any; reply -32601.
                assert self._proc is not None and self._proc.stdin is not None
                error_reply = {
                    "jsonrpc": _JSONRPC_VERSION,
                    "id": msg["id"],
                    "error": {"code": -32601, "message": "Method not found"},
                }
                reply_line = json.dumps(error_reply, separators=(",", ":")) + "\n"
                async with self._stdin_lock:
                    await self._proc.stdin.send(reply_line.encode("utf-8"))
                continue
            if "method" in msg:
                # Notification (no id) — not our response, raise protocol error
                # since we're sequential and didn't expect interleaving at this
                # layer (notifications are handled via read_notification).
                raise AcpProtocolError(
                    "ACP agent sent an unexpected notification during request"
                )
            # It's a response — check id.
            if msg.get("id") != request_id:
                raise AcpProtocolError(
                    f"ACP agent response id mismatch: expected {request_id}, "
                    f"got {msg.get('id')}"
                )
            if "error" in msg:
                err = msg["error"]
                raise AcpProtocolError(
                    f"ACP JSON-RPC error {err.get('code')}: {err.get('message', '')}"
                )
            result = msg.get("result", {})
            if not isinstance(result, dict):
                raise AcpProtocolError("ACP agent response result is not an object")
            return result

    async def read_notification(self) -> JsonObject | None:
        assert self._stdout is not None
        with anyio.move_on_after(self.request_timeout_s) as scope:
            raw = await self._stdout.receive_until(b"\n", 10 * 1024 * 1024)
        if scope.cancel_called:
            return None
        msg = _parse_message(raw)
        if msg is None or "method" not in msg or "id" in msg:
            return None
        return msg

    async def close(self) -> None:
        await self._shutdown()

    async def _shutdown(self) -> None:
        # Cancel stderr drain task first.
        if self._stderr_cancel_scope is not None:
            self._stderr_cancel_scope.cancel()
        if self._stderr_tg is not None:
            with suppress(BaseException):
                await self._stderr_tg.__aexit__(None, None, None)
            self._stderr_tg = None
        if self._proc is not None:
            await self._shutdown_proc(self._proc)
            self._proc = None
        self._stdout = None
        self._ctx = None

    async def _shutdown_proc(self, proc: Process) -> None:
        # Close stdin to signal EOF, then exit the managed-process context
        # which handles process-tree termination + stream cleanup.
        if proc.stdin is not None:
            with anyio.move_on_after(self.close_timeout_s):
                await proc.stdin.aclose()
        if self._ctx is not None:
            with suppress(BaseException):
                await self._ctx.__aexit__(None, None, None)


def _parse_message(raw: bytes) -> JsonObject | None:
    """Parse a newline-delimited JSON object; return None for malformed."""
    try:
        text = raw.decode("utf-8").strip()
    except UnicodeDecodeError:
        return None
    if not text:
        return None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AcpProtocolError("ACP agent sent malformed JSON") from exc
    if not isinstance(obj, dict):
        return None
    return obj


# ---------------------------------------------------------------------------
# Fake transport (tests)
# ---------------------------------------------------------------------------


class FakeAcpTransport:
    """In-memory transport for testing without a real subprocess.

    Records all outgoing JSON-RPC requests and allows tests to queue
    responses and emit notifications.
    """

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self._responses: dict[str, Any] = {}
        self._notifications: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    def queue_response(self, method: str, result: Any) -> None:
        self._responses[method] = result

    def emit_notification(self, method: str, params: dict[str, Any]) -> None:
        self._notifications.put_nowait({"method": method, "params": params})

    async def send_request(self, request: dict[str, Any]) -> JsonObject:
        self.requests.append(request)
        method = request["method"]
        if method in self._responses:
            result = self._responses.pop(method)
            return result if isinstance(result, dict) else {}
        return {}

    async def read_notification(self) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self._notifications.get(), timeout=5.0)
        except TimeoutError:
            return None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


@dataclass
class AcpClient:
    """ACP JSON-RPC stdio client.

    Use as an async context manager::

        async with AcpClient(...) as client:
            await client.initialize()
            ...
    """

    command: str
    args: list[str]
    cwd: str | None = None
    env: dict[str, str] | None = None
    close_timeout_s: float = 5.0
    request_timeout_s: float = 60.0
    shutdown_timeout_s: float = 5.0
    kill_tree_on_cancel: bool = True
    transport: AcpTransport | None = None
    _id_counter: itertools.count[int] = field(
        default_factory=lambda: itertools.count(1), init=False, repr=False
    )
    _available_commands: set[str] = field(default_factory=set, init=False, repr=False)
    _initialized: bool = field(default=False, init=False, repr=False)
    _init_result: JsonObject = field(default_factory=dict, init=False, repr=False)

    def _resolve_transport(self) -> AcpTransport:
        if self.transport is not None:
            return self.transport
        return SubprocessAcpTransport(
            command=self.command,
            args=self.args,
            cwd=self.cwd,
            env=self.env,
            close_timeout_s=self.close_timeout_s,
            request_timeout_s=self.request_timeout_s,
            shutdown_timeout_s=self.shutdown_timeout_s,
            kill_tree_on_cancel=self.kill_tree_on_cancel,
        )

    async def initialize(self) -> None:
        from .. import __version__ as _ver

        result = await self._request(
            "initialize",
            {
                "protocolVersion": _PROTOCOL_VERSION,
                "clientInfo": {"name": "untether", "version": _ver},
                "clientCapabilities": {
                    "fs": {"readTextFile": False, "writeTextFile": False},
                    "terminal": False,
                },
            },
        )
        selected = result.get("protocolVersion")
        if selected != _PROTOCOL_VERSION:
            raise AcpProtocolError(
                f"ACP agent selected unsupported protocol version: {selected}"
            )
        self._init_result = result
        self._initialized = True

    async def resume_or_load(self, session_id: str) -> None:
        caps = self._init_result.get("agentCapabilities", {})
        params: JsonObject = {
            "sessionId": session_id,
            "cwd": self.cwd or os.getcwd(),
            "mcpServers": [],
        }
        if caps.get("loadSession"):
            await self._request("session/load", params)
        else:
            session_caps = caps.get("sessionCapabilities", {})
            if session_caps.get("resume") is not None:
                await self._request("session/resume", params)
            else:
                raise AcpCommandUnavailableError(
                    "ACP agent cannot load/resume sessions"
                )

    async def wait_for_available_commands(self) -> set[str]:
        """Block until ``available_commands_update`` arrives; return names."""
        while True:
            notification = await self._read_notification()
            if notification is None:
                raise AcpCommandUnavailableError(
                    "ACP agent did not advertise available commands"
                )
            update = _extract_available_commands(notification)
            if update is not None:
                self._available_commands = update
                return self._available_commands

    async def require_command(self, name: str) -> None:
        if name not in self._available_commands:
            raise AcpCommandUnavailableError(f"ACP agent did not advertise '{name}'")

    async def prompt(self, session_id: str, text: str) -> AsyncIterator[AcpUpdate]:
        result = await self._request(
            "session/prompt",
            {
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": text}],
            },
        )
        stop_reason = result.get("stopReason") if isinstance(result, dict) else None
        yield AcpUpdate(kind="stop", stop_reason=stop_reason)

    # --- internals ---

    async def _request(self, method: str, params: JsonObject) -> JsonObject:
        request: JsonObject = {
            "jsonrpc": _JSONRPC_VERSION,
            "id": next(self._id_counter),
            "method": method,
            "params": params,
        }
        transport = self._resolve_transport()
        try:
            return await asyncio.wait_for(
                self._send_with_timeout(transport, request),
                timeout=self.request_timeout_s,
            )
        except TimeoutError as exc:
            raise AcpProtocolError(f"ACP {method} request timed out") from exc

    async def _send_with_timeout(
        self, transport: AcpTransport, request: JsonObject
    ) -> JsonObject:
        return await transport.send_request(request)

    async def _read_notification(self) -> JsonObject | None:
        transport = self._resolve_transport()
        return await transport.read_notification()


def _extract_available_commands(
    notification: JsonObject,
) -> set[str] | None:
    """Parse the official nested ``session/update`` envelope for command names.

    Returns ``None`` if *notification* is not an ``available_commands_update``.
    """
    if notification.get("method") != "session/update":
        return None
    params = notification.get("params", {})
    if not isinstance(params, dict):
        return None
    update = params.get("update", {})
    if not isinstance(update, dict):
        return None
    if update.get("sessionUpdate") != "available_commands_update":
        return None
    commands = update.get("availableCommands", [])
    if not isinstance(commands, list):
        return None
    return {
        cmd.get("name", "")
        for cmd in commands
        if isinstance(cmd, dict) and cmd.get("name")
    }


# ---------------------------------------------------------------------------
# Mixin
# ---------------------------------------------------------------------------


class _AcpCompactRunner(Protocol):
    """Structural contract for runners using ACP compaction."""

    engine: EngineId
    compact_accepts_instructions: bool
    close_timeout_s: float
    startup_timeout_s: float | None
    shutdown_timeout_s: float
    kill_tree_on_cancel: bool

    def command(self) -> str: ...

    def create_acp_client(self) -> AcpClient: ...


class AcpCompactMixin:
    """Compact via ACP ``session/prompt`` after capability-gating."""

    compact_accepts_instructions: bool = True
    shutdown_timeout_s: float = 5.0
    kill_tree_on_cancel: bool = True

    def compact_support(self) -> Any:
        from ..compact import CompactSupport

        return CompactSupport(
            mode="acp",
            accepts_instructions=self.compact_accepts_instructions,
            true_compaction=True,
            note="ACP compact requires advertised compact command",
        )

    async def compact(
        self: _AcpCompactRunner,
        resume: ResumeToken,
        instructions: str | None = None,
    ) -> AsyncIterator[UntetherEvent]:
        from ..compact import compact_prompt

        engine = self.engine
        factory = EventFactory(engine)
        if resume.engine != engine:
            yield factory.completed(
                ok=False,
                answer="",
                resume=resume,
                error=f"resume token engine {resume.engine!r} != runner engine {engine!r}",
            )
            return
        yield factory.started(
            resume,
            title=f"{engine} compact",
            meta={"compact": {"mode": "acp", "true_compaction": True}},
        )
        try:
            client = self.create_acp_client()
            try:
                await client.initialize()
                await client.resume_or_load(resume.value)
                await client.wait_for_available_commands()
                await client.require_command("compact")
                async for _update in client.prompt(
                    resume.value, compact_prompt(instructions)
                ):
                    pass
            finally:
                pass
            yield factory.completed_ok(
                answer=f"{engine} compaction completed.",
                resume=resume,
            )
        except Exception as exc:  # noqa: BLE001
            yield factory.completed(
                ok=False,
                answer="",
                resume=resume,
                error=str(exc),
            )

    def create_acp_client(self) -> AcpClient:
        """Create an AcpClient. Override in concrete runners."""
        raise NotImplementedError(
            "AcpCompactMixin subclasses must implement create_acp_client"
        )


@asynccontextmanager
async def _acp_client_context(client: AcpClient) -> AsyncIterator[AcpClient]:
    """Backwards-compatible async context for manually constructed clients."""
    transport = client._resolve_transport()
    await transport.start()
    try:
        yield client
    finally:
        await transport.close()
