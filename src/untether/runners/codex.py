from __future__ import annotations

import json
import re
import subprocess
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import anyio
import msgspec
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream

from ..backends import EngineBackend, EngineConfig
from ..config import ConfigError
from ..events import EventFactory
from ..logging import get_logger
from ..model import ActionPhase, EngineId, ResumeToken, UntetherEvent
from ..runner import (
    BaseRunner,
    JsonlSubprocessRunner,
    ResumeTokenMixin,
    Runner,
    _rc_label,
    _session_label,
    _stderr_excerpt,
)
from ..schemas import codex as codex_schema
from ..utils.paths import get_run_base_dir, relativize_command
from ..utils.streams import drain_stderr, iter_bytes_lines
from ..utils.subprocess import (
    close_process_streams,
    kill_process_tree,
    terminate_process,
    wait_for_process,
)
from ._compact_mixin import SlashCompactMixin
from .modes import effective_prompt, run_modes
from .run_options import get_run_options

logger = get_logger(__name__)

ENGINE: EngineId = "codex"
_APP_PENDING_CAP = 64

__all__ = [
    "ENGINE",
    "CodexRunner",
    "find_exec_only_flag",
    "translate_codex_event",
]

_RESUME_RE = re.compile(r"(?im)^\s*`?codex\s+resume\s+(?P<token>[^`\s]+)`?\s*$")
_RECONNECTING_RE = re.compile(
    r"^Reconnecting\.{3}\s*(?P<attempt>\d+)/(?P<max>\d+)\s*$",
    re.IGNORECASE,
)
_EXEC_ONLY_FLAGS = {
    "--ask-for-approval",
    "--skip-git-repo-check",
    "--json",
    "--output-schema",
    "--output-last-message",
    "--color",
    "-o",
}
_EXEC_ONLY_PREFIXES = (
    "--output-schema=",
    "--output-last-message=",
    "--color=",
)


def find_exec_only_flag(extra_args: list[str]) -> str | None:
    for arg in extra_args:
        if arg in _EXEC_ONLY_FLAGS:
            return arg
        for prefix in _EXEC_ONLY_PREFIXES:
            if arg.startswith(prefix):
                return arg
    return None


def _parse_reconnect_message(message: str) -> tuple[int, int] | None:
    match = _RECONNECTING_RE.match(message)
    if not match:
        return None
    try:
        attempt = int(match.group("attempt"))
        max_attempts = int(match.group("max"))
    except (TypeError, ValueError):
        return None
    return (attempt, max_attempts)


def _short_tool_name(server: str | None, tool: str | None) -> str:
    name = ".".join(part for part in (server, tool) if part)
    return name or "tool"


def _summarize_tool_result(result: Any) -> dict[str, Any] | None:
    if isinstance(result, codex_schema.McpToolCallItemResult):
        summary: dict[str, Any] = {}
        content = result.content
        if isinstance(content, list):
            summary["content_blocks"] = len(content)
        elif content is not None:
            summary["content_blocks"] = 1
        summary["has_structured"] = result.structured_content is not None
        return summary or None

    if isinstance(result, dict):
        summary = {}
        content = result.get("content")
        if isinstance(content, list):
            summary["content_blocks"] = len(content)
        elif content is not None:
            summary["content_blocks"] = 1

        structured_key: str | None = None
        if "structured_content" in result:
            structured_key = "structured_content"
        elif "structured" in result:
            structured_key = "structured"

        if structured_key is not None:
            summary["has_structured"] = result.get(structured_key) is not None
        return summary or None

    return None


def _normalize_change_list(changes: list[Any]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for change in changes:
        path: str | None = None
        kind: str | None = None
        if isinstance(change, codex_schema.FileUpdateChange):
            path = change.path
            kind = change.kind
        elif isinstance(change, dict):
            path = change.get("path")
            kind = change.get("kind")
        if not isinstance(path, str) or not path:
            continue
        entry = {"path": path}
        if isinstance(kind, str) and kind:
            entry["kind"] = kind
        normalized.append(entry)
    return normalized


def _format_change_summary(changes: list[Any]) -> str:
    paths: list[str] = []
    for change in changes:
        if isinstance(change, codex_schema.FileUpdateChange):
            if change.path:
                paths.append(change.path)
            continue
        if isinstance(change, dict):
            path = change.get("path")
            if isinstance(path, str) and path:
                paths.append(path)
    if not paths:
        total = len(changes)
        if total <= 0:
            return "files"
        return f"{total} files"
    return ", ".join(str(path) for path in paths)


@dataclass(frozen=True, slots=True)
class _TodoSummary:
    done: int
    total: int
    next_text: str | None


def _summarize_todo_list(items: Any) -> _TodoSummary:
    if not isinstance(items, list):
        return _TodoSummary(done=0, total=0, next_text=None)

    done = 0
    total = 0
    next_text: str | None = None

    for raw_item in items:
        if isinstance(raw_item, codex_schema.TodoItem):
            total += 1
            if raw_item.completed:
                done += 1
                continue
            if next_text is None:
                next_text = raw_item.text
            continue
        if not isinstance(raw_item, dict):
            continue
        total += 1
        completed = raw_item.get("completed") is True
        if completed:
            done += 1
            continue
        if next_text is None:
            text = raw_item.get("text")
            next_text = str(text) if text is not None else None

    return _TodoSummary(done=done, total=total, next_text=next_text)


def _todo_title(summary: _TodoSummary) -> str:
    if summary.total <= 0:
        return "todo"
    if summary.next_text:
        return f"todo {summary.done}/{summary.total}: {summary.next_text}"
    return f"todo {summary.done}/{summary.total}: done"


@dataclass(frozen=True, slots=True)
class _AgentMessageSummary:
    text: str
    phase: str | None


def _select_final_answer(agent_messages: list[_AgentMessageSummary]) -> str | None:
    for message in reversed(agent_messages):
        if message.phase == "final_answer":
            return message.text
    for message in reversed(agent_messages):
        if message.phase in {None, ""}:
            return message.text
    return None


def _translate_item_event(
    phase: ActionPhase, item: codex_schema.ThreadItem, *, factory: EventFactory
) -> list[UntetherEvent]:
    match item:
        case codex_schema.AgentMessageItem(
            id=action_id,
            text=text,
            phase="commentary",
        ):
            detail = {"phase": "commentary"}
            if phase in {"started", "updated"}:
                return [
                    factory.action(
                        phase=phase,
                        action_id=action_id,
                        kind="note",
                        title=text,
                        detail=detail,
                    )
                ]
            if phase == "completed":
                return [
                    factory.action_completed(
                        action_id=action_id,
                        kind="note",
                        title=text,
                        detail=detail,
                        ok=True,
                    )
                ]
            return []
        case codex_schema.AgentMessageItem():
            return []
        case codex_schema.ErrorItem(id=action_id, message=message):
            if phase != "completed":
                return []
            return [
                factory.action_completed(
                    action_id=action_id,
                    kind="warning",
                    title=message,
                    detail={"message": message},
                    ok=False,
                    message=message,
                    level="warning",
                ),
            ]
        case codex_schema.CommandExecutionItem(
            id=action_id,
            command=command,
            exit_code=exit_code,
            status=status,
        ):
            title = relativize_command(command)
            if phase in {"started", "updated"}:
                return [
                    factory.action(
                        phase=phase,
                        action_id=action_id,
                        kind="command",
                        title=title,
                    )
                ]
            if phase == "completed":
                ok = status == "completed"
                if isinstance(exit_code, int):
                    ok = ok and exit_code == 0
                detail = {"exit_code": exit_code, "status": status}
                return [
                    factory.action_completed(
                        action_id=action_id,
                        kind="command",
                        title=title,
                        detail=detail,
                        ok=ok,
                    ),
                ]
        case codex_schema.McpToolCallItem(
            id=action_id,
            server=server,
            tool=tool,
            arguments=arguments,
            status=status,
            result=result,
            error=error,
        ):
            title = _short_tool_name(server, tool)
            detail: dict[str, Any] = {
                "server": server,
                "tool": tool,
                "status": status,
                "arguments": arguments,
            }

            if phase in {"started", "updated"}:
                return [
                    factory.action(
                        phase=phase,
                        action_id=action_id,
                        kind="tool",
                        title=title,
                        detail=detail,
                    )
                ]
            if phase == "completed":
                ok = status == "completed" and error is None
                if error is not None:
                    detail["error_message"] = str(error.message)
                result_summary = _summarize_tool_result(result)
                if result_summary is not None:
                    detail["result_summary"] = result_summary
                return [
                    factory.action_completed(
                        action_id=action_id,
                        kind="tool",
                        title=title,
                        detail=detail,
                        ok=ok,
                    ),
                ]
        case codex_schema.WebSearchItem(id=action_id, query=query):
            detail = {"query": query}
            if phase in {"started", "updated"}:
                return [
                    factory.action(
                        phase=phase,
                        action_id=action_id,
                        kind="web_search",
                        title=query,
                        detail=detail,
                    )
                ]
            if phase == "completed":
                return [
                    factory.action_completed(
                        action_id=action_id,
                        kind="web_search",
                        title=query,
                        detail=detail,
                        ok=True,
                    )
                ]
        case codex_schema.FileChangeItem(id=action_id, changes=changes, status=status):
            if phase != "completed":
                return []
            title = _format_change_summary(changes)
            normalized_changes = _normalize_change_list(changes)
            detail = {
                "changes": normalized_changes,
                "status": status,
                "error": None,
            }
            ok = status == "completed"
            return [
                factory.action_completed(
                    action_id=action_id,
                    kind="file_change",
                    title=title,
                    detail=detail,
                    ok=ok,
                )
            ]
        case codex_schema.TodoListItem(id=action_id, items=items):
            summary = _summarize_todo_list(items)
            title = _todo_title(summary)
            detail = {"done": summary.done, "total": summary.total}
            if phase in {"started", "updated"}:
                return [
                    factory.action(
                        phase=phase,
                        action_id=action_id,
                        kind="note",
                        title=title,
                        detail=detail,
                    )
                ]
            if phase == "completed":
                return [
                    factory.action_completed(
                        action_id=action_id,
                        kind="note",
                        title=title,
                        detail=detail,
                        ok=True,
                    )
                ]
        case codex_schema.ReasoningItem(id=action_id, text=text):
            if phase in {"started", "updated"}:
                return [
                    factory.action(
                        phase=phase,
                        action_id=action_id,
                        kind="note",
                        title=text,
                    )
                ]
            if phase == "completed":
                return [
                    factory.action_completed(
                        action_id=action_id,
                        kind="note",
                        title=text,
                        ok=True,
                    )
                ]
    return []


def translate_codex_event(
    event: codex_schema.ThreadEvent,
    *,
    title: str,
    factory: EventFactory,
    meta: dict[str, Any] | None = None,
) -> list[UntetherEvent]:
    match event:
        case codex_schema.ThreadStarted(thread_id=thread_id):
            logger.info("codex.session.started", session_id=thread_id)
            token = ResumeToken(engine=ENGINE, value=thread_id)
            return [factory.started(token, title=title, meta=meta)]
        case codex_schema.ItemStarted(item=item):
            return _translate_item_event("started", item, factory=factory)
        case codex_schema.ItemUpdated(item=item):
            return _translate_item_event("updated", item, factory=factory)
        case codex_schema.ItemCompleted(item=item):
            return _translate_item_event("completed", item, factory=factory)
        case _:
            logger.debug(
                "codex.event.unrecognised",
                event_type=type(event).__name__,
            )
            return []


@dataclass(slots=True)
class CodexRunState:
    factory: EventFactory
    note_seq: int = 0
    final_answer: str | None = None
    turn_agent_messages: list[_AgentMessageSummary] = field(default_factory=list)
    turn_index: int = 0


class CodexRunner(SlashCompactMixin, ResumeTokenMixin, JsonlSubprocessRunner):
    compact_accepts_instructions = False
    engine: EngineId = ENGINE
    resume_re = _RESUME_RE
    model: str | None = None
    logger = logger

    def __init__(
        self,
        *,
        codex_cmd: str,
        extra_args: list[str],
        title: str = "Codex",
    ) -> None:
        self.codex_cmd = codex_cmd
        self.extra_args = extra_args
        self.session_title = title

    def command(self) -> str:
        return self.codex_cmd

    def build_args(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: Any,
    ) -> list[str]:
        run_options = get_run_options()
        args = [*self.extra_args]
        if run_options is not None:
            if run_options.model:
                args.extend(["--model", str(run_options.model)])
            if run_options.reasoning:
                args.extend(
                    [
                        "-c",
                        f"model_reasoning_effort={run_options.reasoning}",
                    ]
                )
        if run_options is not None and run_options.permission_mode == "safe":
            args.extend(["--ask-for-approval", "untrusted"])
        else:
            args.extend(["--ask-for-approval", "never"])
        args.extend(
            [
                "exec",
                "--json",
                "--skip-git-repo-check",
                "--color=never",
            ]
        )
        if resume:
            if resume.is_continue:
                args.extend(["resume", "--last", "-"])
            else:
                args.extend(["resume", resume.value, "-"])
        else:
            args.append("-")
        return args

    def new_state(self, prompt: str, resume: ResumeToken | None) -> CodexRunState:
        return CodexRunState(factory=EventFactory(ENGINE))

    def start_run(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: CodexRunState,
    ) -> None:
        pass

    def decode_jsonl(self, *, line: bytes) -> codex_schema.ThreadEvent:
        return codex_schema.decode_event(line)

    def decode_error_events(
        self,
        *,
        raw: str,
        line: str,
        error: Exception,
        state: CodexRunState,
    ) -> list[UntetherEvent]:
        if isinstance(error, msgspec.DecodeError):
            self.get_logger().warning(
                "jsonl.msgspec.invalid",
                tag=self.tag(),
                error=str(error),
                error_type=error.__class__.__name__,
            )
            return []
        return super().decode_error_events(
            raw=raw,
            line=line,
            error=error,
            state=state,
        )

    def pipes_error_message(self) -> str:
        return "codex exec failed to open subprocess pipes"

    def translate(
        self,
        data: codex_schema.ThreadEvent,
        *,
        state: CodexRunState,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
    ) -> list[UntetherEvent]:
        factory = state.factory
        match data:
            case codex_schema.StreamError(message=message):
                reconnect = _parse_reconnect_message(message)
                if reconnect is not None:
                    attempt, max_attempts = reconnect
                    phase: ActionPhase = "started" if attempt <= 1 else "updated"
                    return [
                        factory.action(
                            phase=phase,
                            action_id="codex.reconnect",
                            kind="note",
                            title=message,
                            detail={"attempt": attempt, "max": max_attempts},
                            level="info",
                        )
                    ]
                return [self.note_event(message, state=state, ok=False)]
            case codex_schema.TurnFailed(error=error):
                resume_for_completed = found_session or resume
                return [
                    factory.completed_error(
                        error=error.message,
                        answer=state.final_answer or "",
                        resume=resume_for_completed,
                    )
                ]
            case codex_schema.TurnStarted():
                action_id = f"turn_{state.turn_index}"
                state.turn_index += 1
                state.final_answer = None
                state.turn_agent_messages.clear()
                return [
                    factory.action_started(
                        action_id=action_id,
                        kind="turn",
                        title="turn started",
                    )
                ]
            case codex_schema.TurnCompleted(usage=usage):
                resume_for_completed = found_session or resume
                return [
                    factory.completed_ok(
                        answer=state.final_answer or "",
                        resume=resume_for_completed,
                        usage=msgspec.to_builtins(usage),
                    )
                ]
            case codex_schema.ItemCompleted(
                item=codex_schema.AgentMessageItem(text=text, phase=message_phase)
            ):
                state.turn_agent_messages.append(
                    _AgentMessageSummary(text=text, phase=message_phase)
                )
                selected = _select_final_answer(state.turn_agent_messages)
                if selected is not None:
                    state.final_answer = selected
                if len(state.turn_agent_messages) > 1:
                    logger.debug("codex.multiple_agent_messages")
            case _:
                pass

        # Build meta from runner config + run options.
        # Always include a model name — use override, runner config, or CLI default.
        meta: dict[str, Any] | None = None
        model = self.model
        run_options = get_run_options()
        if run_options is not None and run_options.model:
            model = run_options.model
        if model is None:
            model = "codex-mini-latest"
        meta = {"model": str(model)}
        if run_options is not None and run_options.reasoning:
            if meta is None:
                meta = {}
            meta["effort"] = run_options.reasoning
        if run_options is not None and run_options.permission_mode == "safe":
            if meta is None:
                meta = {}
            meta["permissionMode"] = "safe"

        return translate_codex_event(
            data,
            title=self.session_title,
            factory=factory,
            meta=meta,
        )

    def process_error_events(
        self,
        rc: int,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: CodexRunState,
        stderr_lines: list[str] | None = None,
    ) -> list[UntetherEvent]:
        parts = [f"codex exec failed ({_rc_label(rc)})."]
        session = _session_label(found_session, resume)
        if session:
            parts.append(f"session: {session}")
        excerpt = _stderr_excerpt(stderr_lines)
        if excerpt:
            parts.append(excerpt)
        message = "\n".join(parts)
        logger.error(
            "codex.process.failed",
            rc=rc,
            session_id=found_session.value if found_session else None,
        )
        resume_for_completed = found_session or resume
        return [
            self.note_event(
                message,
                state=state,
                ok=False,
            ),
            state.factory.completed_error(
                error=message,
                answer=state.final_answer or "",
                resume=resume_for_completed,
            ),
        ]

    def stream_end_events(
        self,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: CodexRunState,
        stderr_lines: list[str] | None = None,
    ) -> list[UntetherEvent]:
        if not found_session:
            logger.warning("codex.stream.no_session")
            parts = ["codex exec finished but no session_id/thread_id was captured"]
            session = _session_label(None, resume)
            if session:
                parts.append(f"session: {session}")
            message = "\n".join(parts)
            resume_for_completed = resume
            return [
                state.factory.completed_error(
                    error=message,
                    answer=state.final_answer or "",
                    resume=resume_for_completed,
                )
            ]
        logger.info("codex.session.completed", resume=found_session.value)
        return [
            state.factory.completed_ok(
                answer=state.final_answer or "",
                resume=found_session,
            )
        ]


@dataclass(slots=True)
class _AppServerWaiter:
    event: anyio.Event = field(default_factory=anyio.Event)
    result: Any = None
    error: BaseException | None = None


class _BufferedSubscription:
    def __init__(
        self, pending: list[dict[str, Any]], receive: Any, client: Any, turn_id: str
    ) -> None:
        self._pending = iter(pending)
        self._receive = receive
        self._client = client
        self._turn_id = turn_id

    def __aiter__(self) -> _BufferedSubscription:
        return self

    async def __anext__(self) -> dict[str, Any]:
        try:
            return next(self._pending)
        except StopIteration:
            try:
                return await self._receive.receive()
            except anyio.EndOfStream:
                async with self._client._state_lock:
                    if self._turn_id in self._client._pending_overflow:
                        raise RuntimeError(
                            "codex app-server notification buffer overflow"
                        ) from None
                raise

    async def receive(self) -> dict[str, Any]:
        return await self.__anext__()

    async def aclose(self) -> None:
        await self._receive.aclose()


class _AppServerClient:
    def __init__(self, *, codex_cmd: str, extra_args: list[str]) -> None:
        self.codex_cmd, self.extra_args = codex_cmd, extra_args
        self._proc: Any = None
        self._reader_tg: TaskGroup | None = None
        self._waiters: dict[str, _AppServerWaiter] = {}
        self._subscriptions: dict[str, MemoryObjectSendStream[dict[str, Any]]] = {}
        self._pending_by_turn: dict[str, list[dict[str, Any]]] = {}
        self._pending_overflow: set[str] = set()
        self._start_lock = anyio.Lock()
        self._state_lock = anyio.Lock()
        self._write_lock = anyio.Lock()

    async def start(self) -> None:
        async with self._start_lock:
            if self._proc is not None:
                return
            try:
                self._proc = await anyio.open_process(
                    [
                        self.codex_cmd,
                        *self.extra_args,
                        "app-server",
                        "--listen",
                        "stdio://",
                    ],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                if self._proc.stdin is None or self._proc.stdout is None:
                    raise RuntimeError(
                        "codex app-server failed to open subprocess pipes"
                    )
                self._reader_tg = await anyio.create_task_group().__aenter__()
                self._reader_tg.start_soon(self._read_loop)
                if self._proc.stderr is not None:
                    self._reader_tg.start_soon(
                        drain_stderr, self._proc.stderr, logger, "codex-app-server"
                    )
                await self.request(
                    "initialize", {"clientInfo": {"name": "untether", "version": "0"}}
                )
                await self.notify("initialized", {})
            except BaseException:
                await self.close()
                raise

    async def close(self) -> None:
        if self._reader_tg is not None:
            self._reader_tg.cancel_scope.cancel()
            await self._reader_tg.__aexit__(None, None, None)
            self._reader_tg = None
        proc = self._proc
        self._proc = None
        if proc is not None:
            with anyio.CancelScope(shield=True):
                if proc.returncode is None:
                    terminate_process(proc)
                    if await wait_for_process(proc, 2.0):
                        await kill_process_tree(proc)
                        await proc.wait()
                await close_process_streams(proc)
                await self._fail_all(RuntimeError("codex app-server closed"))

    async def _read_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        failure: BaseException | None = None
        try:
            async for raw in iter_bytes_lines(self._proc.stdout):
                message = json.loads(raw)
                if not isinstance(message, dict):
                    continue
                if "method" in message and "id" in message:
                    response = self._handle_server_request(message)
                    await self._write({"id": message["id"], "result": response})
                    continue
                if "method" in message:
                    params = message.get("params", {})
                    turn = params.get("turn", {}) if isinstance(params, dict) else {}
                    turn_id = params.get("turnId") if isinstance(params, dict) else None
                    if not isinstance(turn_id, str) and isinstance(turn, dict):
                        turn_id = turn.get("id")
                    if isinstance(turn_id, str):
                        async with self._state_lock:
                            stream = self._subscriptions.get(turn_id)
                            if stream is None:
                                pending = self._pending_by_turn.setdefault(turn_id, [])
                                if len(pending) < _APP_PENDING_CAP:
                                    pending.append(message)
                                else:
                                    self._pending_overflow.add(turn_id)
                        if stream is not None:
                            try:
                                stream.send_nowait(message)
                            except anyio.WouldBlock:
                                async with self._state_lock:
                                    self._pending_overflow.add(turn_id)
                                    self._subscriptions.pop(turn_id, None)
                                await stream.aclose()
                            except anyio.ClosedResourceError:
                                pass
                    continue
                async with self._state_lock:
                    waiter = self._waiters.pop(str(message["id"]), None)
                if waiter is not None:
                    waiter.error = (
                        RuntimeError("codex app-server request failed")
                        if "error" in message
                        else None
                    )
                    waiter.result = message.get("result")
                    waiter.event.set()
        except BaseException as exc:  # noqa: BLE001 - fail waiters before cancellation propagates
            failure = exc
        await self._fail_all(failure or RuntimeError("codex app-server closed"))

    def _handle_server_request(self, message: dict[str, Any]) -> dict[str, Any]:
        method = message.get("method")
        params = message.get("params")
        if method in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        }:
            return {"decision": "accept"}
        if method == "item/permissions/requestApproval" and isinstance(params, dict):
            permissions = params.get("permissions")
            return {"scope": "turn", "permissions": permissions or {}}
        if method == "mcpServer/elicitation/request":
            return {"action": "decline", "content": None}
        return {}

    async def _fail_all(self, exc: BaseException) -> None:
        async with self._state_lock:
            waiters = list(self._waiters.values())
            self._waiters.clear()
            streams = list(self._subscriptions.values())
            self._subscriptions.clear()
            self._pending_by_turn.clear()
            self._pending_overflow.clear()
        for waiter in waiters:
            waiter.error = RuntimeError(f"codex app-server closed: {exc}")
            waiter.event.set()
        for stream in streams:
            await stream.aclose()

    async def _write(self, payload: dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise RuntimeError("codex app-server is not running")
        async with self._write_lock:
            await self._proc.stdin.send(json.dumps(payload).encode() + b"\n")

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        await self._write({"method": method, "params": params})

    async def request(self, method: str, params: dict[str, Any]) -> Any:
        request_id = str(uuid.uuid4())
        waiter = _AppServerWaiter()
        async with self._state_lock:
            self._waiters[request_id] = waiter
        try:
            await self._write({"id": request_id, "method": method, "params": params})
        except BaseException:
            async with self._state_lock:
                self._waiters.pop(request_id, None)
            raise
        await waiter.event.wait()
        if waiter.error is not None:
            raise waiter.error
        return waiter.result

    async def thread_start(self, params: dict[str, Any]) -> dict[str, Any]:
        result = await self.request("thread/start", params)
        if not isinstance(result, dict) or not isinstance(result.get("thread"), dict):
            raise RuntimeError("thread/start returned no thread")
        return result

    async def ensure_thread_loaded(self, thread_id: str) -> None:
        await self.request("thread/resume", {"threadId": thread_id})

    async def turn_start(
        self, thread_id: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self.request("turn/start", {"threadId": thread_id, **params})
        if not isinstance(result, dict):
            raise RuntimeError("turn/start returned non-object")
        return result

    async def turn_steer(self, thread_id: str, turn_id: str, text: str) -> None:
        result = await self.request(
            "turn/steer",
            {
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "input": [{"type": "text", "text": text}],
            },
        )
        if not isinstance(result, dict) or result.get("turnId") != turn_id:
            raise RuntimeError("turn/steer returned unexpected turn id")

    async def turn_interrupt(self, thread_id: str, turn_id: str) -> bool:
        await self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
        return True

    async def subscribe_turn(self, turn_id: str) -> Any:
        send, receive = anyio.create_memory_object_stream[dict[str, Any]](
            _APP_PENDING_CAP
        )
        async with self._state_lock:
            if turn_id in self._pending_overflow:
                self._pending_overflow.discard(turn_id)
                raise RuntimeError("codex app-server notification buffer overflow")
            pending = self._pending_by_turn.pop(turn_id, [])
            self._subscriptions[turn_id] = send
        return _BufferedSubscription(pending, receive, self, turn_id)

    async def unsubscribe_turn(self, turn_id: str) -> None:
        async with self._state_lock:
            send = self._subscriptions.pop(turn_id, None)
        if send is not None:
            await send.aclose()


@dataclass(frozen=True, slots=True)
class _AppServerTurnControl:
    client: _AppServerClient
    thread_id: str
    turn_id: str

    async def steer(self, text: str) -> None:
        await self.client.turn_steer(self.thread_id, self.turn_id, text)

    async def interrupt(self) -> bool:
        return await self.client.turn_interrupt(self.thread_id, self.turn_id)


class AppServerCodexRunner(SlashCompactMixin, ResumeTokenMixin, BaseRunner):
    compact_accepts_instructions = False
    engine: EngineId = ENGINE
    resume_re = _RESUME_RE

    def __init__(
        self, *, codex_cmd: str, extra_args: list[str], title: str = "Codex"
    ) -> None:
        self.codex_cmd, self.extra_args, self.session_title = (
            codex_cmd,
            extra_args,
            title,
        )
        self._client = _AppServerClient(codex_cmd=codex_cmd, extra_args=extra_args)

    async def run_impl(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]:
        client = self._client
        await client.start()
        options = get_run_options()
        plan, goal = run_modes(options)
        if goal:
            prompt = f"(autonomous goal — work until: {goal})\n\n{prompt.strip()}"
        elif plan:
            prompt = effective_prompt(prompt, soft_plan=True, options=options)
        if resume is None:
            thread_id = str(
                (await client.thread_start({"cwd": str(get_run_base_dir())}))["thread"][
                    "id"
                ]
            )
        else:
            thread_id = resume.value
            await client.ensure_thread_loaded(thread_id)
        result = await client.turn_start(
            thread_id, {"input": [{"type": "text", "text": prompt}]}
        )
        turn = result.get("turn") if isinstance(result, dict) else None
        if not isinstance(turn, dict) or not isinstance(turn.get("id"), str):
            raise RuntimeError("turn/start returned no turn id")
        token = ResumeToken(engine=ENGINE, value=thread_id)
        control = _AppServerTurnControl(client, thread_id, turn["id"])
        yield EventFactory(ENGINE).started(
            token,
            title=self.session_title,
            meta={"turn_id": turn["id"], "control": control},
        )
        try:
            notifications = await client.subscribe_turn(turn["id"])
        except RuntimeError as exc:
            yield EventFactory(ENGINE).completed_error(
                error=str(exc), answer="", resume=token
            )
            await client.close()
            return
        answer = ""
        agent_messages: list[_AgentMessageSummary] = []
        terminal = False
        error = "codex app-server closed before turn completion"
        try:
            async for message in notifications:
                method = message.get("method") if isinstance(message, dict) else None
                params = message.get("params", {}) if isinstance(message, dict) else {}
                if method == "turn/plan/updated":
                    yield EventFactory(ENGINE).action_started(
                        action_id="plan",
                        kind="command",
                        title="Plan updated",
                        detail={"plan": params.get("plan", [])},
                    )
                elif method == "item/completed":
                    item = params.get("item", {}) if isinstance(params, dict) else {}
                    if isinstance(item, dict) and item.get("type") == "agentMessage":
                        text = item.get("text")
                        if isinstance(text, str):
                            agent_messages.append(
                                _AgentMessageSummary(text=text, phase=item.get("phase"))
                            )
                            selected = _select_final_answer(agent_messages)
                            if selected is not None:
                                answer = selected
                elif method == "turn/completed":
                    turn_info = (
                        params.get("turn", {}) if isinstance(params, dict) else {}
                    )
                    status = (
                        turn_info.get("status") if isinstance(turn_info, dict) else None
                    )
                    if status == "completed":
                        terminal = True
                    else:
                        turn_error = (
                            turn_info.get("error")
                            if isinstance(turn_info, dict)
                            else None
                        )
                        if isinstance(turn_error, dict):
                            turn_error = turn_error.get("message") or turn_error.get(
                                "code"
                            )
                        error = str(turn_error or status or "codex turn failed")[:500]
                    break
        except RuntimeError as exc:
            error = str(exc)
        finally:
            await client.unsubscribe_turn(turn["id"])
            close = getattr(client, "close", None)
            if close is not None:
                with anyio.move_on_after(2, shield=True):
                    await close()
        if terminal:
            yield EventFactory(ENGINE).completed_ok(answer=answer, resume=token)
        else:
            yield EventFactory(ENGINE).completed_error(
                error=error, answer=answer, resume=token
            )


def build_runner(config: EngineConfig, config_path: Path) -> Runner:
    codex_cmd = "codex"

    extra_args_value = config.get("extra_args")
    if extra_args_value is None:
        extra_args = ["-c", "notify=[]"]
    elif isinstance(extra_args_value, list) and all(
        isinstance(item, str) for item in extra_args_value
    ):
        extra_args = list(extra_args_value)
    else:
        logger.warning(
            "codex.config.invalid",
            error="extra_args must be a list of strings",
            config_path=str(config_path),
        )
        raise ConfigError(
            f"Invalid `codex.extra_args` in {config_path}; expected a list of strings."
        )

    exec_only_flag = find_exec_only_flag(extra_args)
    if exec_only_flag:
        logger.warning(
            "codex.config.invalid",
            error=f"exec-only flag {exec_only_flag!r} is managed by Untether",
            config_path=str(config_path),
        )
        raise ConfigError(
            f"Invalid `codex.extra_args` in {config_path}; exec-only flag "
            f"{exec_only_flag!r} is managed by Untether."
        )

    title = "Codex"
    profile_value = config.get("profile")
    if profile_value:
        if not isinstance(profile_value, str):
            logger.warning(
                "codex.config.invalid",
                error="profile must be a string",
                config_path=str(config_path),
            )
            raise ConfigError(
                f"Invalid `codex.profile` in {config_path}; expected a string."
            )
        extra_args.extend(["--profile", profile_value])
        title = profile_value

    mode = config.get("mode", "app_server")
    if mode not in {"app_server", "exec"}:
        raise ConfigError(
            f"Invalid `codex.mode` in {config_path}; expected `app_server` or `exec`."
        )
    if mode == "exec":
        runner: Runner = CodexRunner(
            codex_cmd=codex_cmd, extra_args=extra_args, title=title
        )
    else:
        runner = AppServerCodexRunner(
            codex_cmd=codex_cmd, extra_args=extra_args, title=title
        )
    return cast(Runner, runner)


BACKEND = EngineBackend(
    id="codex",
    build_runner=build_runner,
    install_cmd="npm install -g @openai/codex",
)
