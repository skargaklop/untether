from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import os
import re
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any

import anyio
from anyio import get_cancelled_exc_class
from anyio.abc import ObjectReceiveStream, Process
from anyio.lowlevel import checkpoint
from anyio.streams.buffered import BufferedByteReceiveStream
from anyio.streams.memory import MemoryObjectSendStream
from structlog import get_logger

from ...utils.subprocess import manage_subprocess

Json = dict[str, Any]
Handler = Callable[[Json], Json | Awaitable[Json]]

logger = get_logger(__name__)


class AcpProtocolError(RuntimeError):
    """Raised when the ACP JSON-RPC stream violates its contract."""


@dataclass
class AcpPeer:
    command: str
    args: list[str]
    cwd: str | None = None
    env: dict[str, str] | None = None
    request_timeout_s: float = 60.0
    max_frame_bytes: int = 10 * 1024 * 1024
    queue_size: int = 256
    allow_batches: bool = False
    _ctx: Any = field(default=None, init=False, repr=False)
    _proc: Process | None = field(default=None, init=False, repr=False)
    _stdout: BufferedByteReceiveStream | None = field(
        default=None, init=False, repr=False
    )
    _stdin_lock: anyio.Lock = field(default_factory=anyio.Lock, init=False, repr=False)
    _pending: dict[Any, anyio.Event] = field(
        default_factory=dict, init=False, repr=False
    )
    _results: dict[Any, Json] = field(default_factory=dict, init=False, repr=False)
    _handlers: dict[str, Handler] = field(default_factory=dict, init=False, repr=False)
    _notifications: ObjectReceiveStream[Json] | None = field(
        default=None, init=False, repr=False
    )
    _notify_send: MemoryObjectSendStream[Json] | None = field(
        default=None, init=False, repr=False
    )
    _reader_scope: anyio.CancelScope | None = field(
        default=None, init=False, repr=False
    )
    _stderr_task: asyncio.Task[None] | None = field(
        default=None, init=False, repr=False
    )
    _reader_task: asyncio.Task[None] | None = field(
        default=None, init=False, repr=False
    )
    _reverse_tasks: set[asyncio.Task[None]] = field(
        default_factory=set, init=False, repr=False
    )
    closed: bool = field(default=False, init=False)
    _failure: BaseException | None = field(default=None, init=False, repr=False)
    _next_id: int = field(default=1, init=False, repr=False)
    stderr_tail: deque[str] = field(
        default_factory=lambda: deque(maxlen=20), init=False
    )
    _reverse_by_id: dict[Any, asyncio.Task[None]] = field(
        default_factory=dict, init=False, repr=False
    )
    _seq: itertools.count = field(
        default_factory=itertools.count, init=False, repr=False
    )

    async def __aenter__(self) -> AcpPeer:
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    def encode(self, message: Json) -> bytes:
        frame = (
            json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n"
        ).encode()
        if len(frame) > self.max_frame_bytes:
            raise ValueError("ACP frame exceeds configured frame limit")
        return frame

    async def start(self) -> None:
        if self._proc is not None:
            return
        kwargs: dict[str, Any] = {"stdin": -1, "stdout": -1, "stderr": -1}
        if self.cwd is not None:
            kwargs["cwd"] = self.cwd
        if self.env is not None:
            kwargs["env"] = {**os.environ, **self.env}
        self._ctx = manage_subprocess([self.command, *self.args], **kwargs)
        self._proc = await self._ctx.__aenter__()
        if self._proc.stdin is None or self._proc.stdout is None:
            raise AcpProtocolError("ACP subprocess did not provide stdio")
        self._stdout = BufferedByteReceiveStream(self._proc.stdout)
        send, receive = anyio.create_memory_object_stream[Json](self.queue_size)
        self._notify_send, self._notifications = send, receive
        self._reader_task = asyncio.create_task(self._read_loop())
        self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def _drain_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        buffered = BufferedByteReceiveStream(self._proc.stderr)
        try:
            while True:
                line = await buffered.receive_until(b"\n", 1024 * 1024)
                text = line.decode("utf-8", errors="replace")
                text = re.sub(
                    r"/(?:home|Users|tmp|var|private/var)/[^ ]+", "[path]", text
                )
                self.stderr_tail.append(text)
        except (anyio.EndOfStream, anyio.IncompleteRead, anyio.ClosedResourceError):
            return

    def register_handler(self, method: str, handler: Handler) -> None:
        self._handlers[method] = handler

    async def notify(self, method: str, params: Json) -> None:
        """Send a fire-and-forget JSON-RPC notification."""
        if self._proc is None:
            await self.start()
        await self._write({"jsonrpc": "2.0", "method": method, "params": params})

    async def request(
        self, method: str, params: Json, *, timeout_s: float | None = None
    ) -> Json:
        if self._proc is None or self._proc.stdin is None:
            await self.start()
        assert self._proc is not None and self._proc.stdin is not None
        request_id = self._next_id
        self._next_id += 1
        event = anyio.Event()
        self._pending[request_id] = event
        await self._write(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )
        try:
            with anyio.fail_after(
                self.request_timeout_s if timeout_s is None else timeout_s
            ):
                await event.wait()
        except TimeoutError:
            with anyio.CancelScope(shield=True):
                await self.close()
            raise
        finally:
            self._pending.pop(request_id, None)
        if self._failure is not None:
            raise self._failure
        return self._results.pop(request_id)

    async def next_notification(self) -> Json:
        if self._notifications is None:
            raise AcpProtocolError("ACP peer is not started")
        try:
            return await self._notifications.receive()
        except anyio.EndOfStream as exc:
            raise AcpProtocolError("ACP peer reached EOF") from exc

    async def _write(self, message: Json) -> None:
        assert self._proc is not None and self._proc.stdin is not None
        async with self._stdin_lock:
            await self._proc.stdin.send(self.encode(message))

    async def _read_loop(self) -> None:
        assert self._stdout is not None
        buffer = bytearray()
        try:
            while True:
                try:
                    chunk = await self._stdout.receive(64 * 1024)
                except anyio.EndOfStream:
                    raise AcpProtocolError("ACP peer reached EOF") from None
                buffer += chunk
                while True:
                    newline = buffer.find(b"\n")
                    if newline == -1:
                        break
                    raw = bytes(buffer[:newline])
                    del buffer[: newline + 1]
                    await self._handle_frame(raw)
                if len(buffer) > self.max_frame_bytes:
                    raise AcpProtocolError("ACP frame exceeds configured frame limit")
        except Exception as exc:  # noqa: BLE001
            if not self.closed:
                self._failure = exc
                for event in self._pending.values():
                    event.set()
                self._results.update({key: {} for key in self._pending})

    async def _handle_frame(self, raw: bytes) -> None:
        try:
            message = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AcpProtocolError("ACP agent sent malformed JSON") from exc
        if isinstance(message, list) and not self.allow_batches:
            raise AcpProtocolError("JSON-RPC batch received under ACP v1")
        items = message if isinstance(message, list) else [message]
        for item in items:
            if isinstance(item, dict):
                item["_untether_seq"] = next(self._seq)
            await self._dispatch(item)

    async def _dispatch(self, message: Any) -> None:
        if not isinstance(message, dict):
            raise AcpProtocolError("ACP message is not an object")
        if "method" in message:
            if message["method"] == "$/cancel_request":
                self._handle_cancel_request(message.get("params", {}))
                return
            if "id" not in message:
                assert self._notify_send is not None
                try:
                    self._notify_send.send_nowait(message)
                except anyio.WouldBlock as exc:
                    raise AcpProtocolError("ACP notification queue overflow") from exc
                return
            handler = self._handlers.get(message["method"])
            if handler is None:
                await self._write(
                    {
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "error": {"code": -32601, "message": "Method not found"},
                    }
                )
                return
            task = asyncio.create_task(
                self._handle_reverse(message["id"], handler, message.get("params", {}))
            )
            self._reverse_tasks.add(task)
            self._reverse_by_id[message["id"]] = task
            task.add_done_callback(partial(self._reverse_done, message["id"]))
            # Allow the reverse task to actually start before a $/cancel_request
            # (possibly in the same frame burst) cancels it: a pre-start cancel()
            # would inject CancelledError before _handle_reverse's body runs,
            # skipping its -32800 response path.
            await checkpoint()
            return
        request_id = message.get("id")
        if request_id not in self._pending or request_id in self._results:
            raise AcpProtocolError("duplicate or unknown ACP response ID")
        if "error" in message:
            raise AcpProtocolError(f"ACP JSON-RPC error: {message['error']}")
        result = message.get("result")
        if not isinstance(result, dict):
            raise AcpProtocolError("ACP response result is not an object")
        self._results[request_id] = result
        self._pending[request_id].set()

    def _reverse_done(self, reverse_id: Any, task: asyncio.Task[None]) -> None:
        self._reverse_tasks.discard(task)
        self._reverse_by_id.pop(reverse_id, None)

    def _handle_cancel_request(self, params: Json) -> None:
        """Cancel a tracked reverse handler on behalf of the agent."""
        target = params.get("id")
        task = self._reverse_by_id.get(target)
        if task is not None and not task.done():
            task.cancel()

    async def _handle_reverse(
        self, request_id: Any, handler: Handler, params: Json
    ) -> None:
        try:
            result = handler(params)
            if inspect.isawaitable(result):
                result = await result
            if not self.closed:
                await self._write(
                    {"jsonrpc": "2.0", "id": request_id, "result": result}
                )
        except get_cancelled_exc_class():
            if not self.closed:
                with anyio.CancelScope(shield=True):
                    await self._write(
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {"code": -32800, "message": "Cancelled"},
                        }
                    )
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "acp.reverse_handler_failed", request_id=request_id, error=str(exc)
            )
            if not self.closed:
                with anyio.CancelScope(shield=True):
                    await self._write(
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {
                                "code": -32603,
                                "message": repr(exc)[:200],
                            },
                        }
                    )

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        with anyio.CancelScope(shield=True):
            if self._proc is not None and self._proc.stdin is not None:
                with anyio.move_on_after(1):
                    await self._proc.stdin.aclose()
            if self._reader_task is not None:
                self._reader_task.cancel()
                self._reader_task = None
            if self._stderr_task is not None:
                self._stderr_task.cancel()
                self._stderr_task = None
            if self._ctx is not None:
                with anyio.move_on_after(2):
                    await self._ctx.__aexit__(None, None, None)
            self._proc = None
