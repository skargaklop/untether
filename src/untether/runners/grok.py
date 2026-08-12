from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import msgspec

from ..backends import EngineBackend, EngineConfig
from ..config import ConfigError
from ..events import EventFactory
from ..logging import get_logger
from ..model import ActionKind, EngineId, ResumeToken, UntetherEvent
from ..runner import (
    JsonlSubprocessRunner,
    ResumeTokenMixin,
    Runner,
    _stderr_excerpt,
)
from ..schemas import grok as grok_schema
from ._compact_mixin import HandoffCompactMixin
from .modes import effective_prompt, run_modes
from .run_options import get_run_options
from .tool_actions import tool_kind_and_title

logger = get_logger(__name__)

# Read-only tools allow-list for plan mode. Combined with
# --permission-mode plan, mutating tools are physically ABSENT from the
# agent's toolset, so no approval prompt can fire and no cancellation
# occurs. Proven by Task 16 probes (D2: end_turn, no file, text delivered).
#
# Built-in tools observed in grok captures: run_terminal_command,
# read_file, search_replace, list_dir, grep, todo_write, write, web_search.
# The allow-list keeps read/explore/search tools and drops all mutating
# tools (write, search_replace, run_terminal_command, todo_write).
_PLAN_READONLY_TOOLS = "read_file,list_dir,grep,web_search"

# Salvage note appended when a plan-mode cancellation is converted to a
# soft success because plan content was produced.
_PLAN_CANCEL_SALVAGE_NOTE = "turn ended by plan-mode enforcement; nothing was executed"

# Honest error for a plan-mode cancel with NO salvageable plan text.
_PLAN_CANCEL_ERROR = (
    "plan-mode turn cancelled by the harness "
    "(attempted a forbidden write/execute in read-only mode)"
)

ENGINE: EngineId = "grok"

_RESUME_RE = re.compile(
    r"(?im)^\s*`?grok\s+(?:resume|--resume|-r)\s+(?P<token>[^`\s]+)`?(?:\s|$)"
)
_RESUME_LINE_RE = re.compile(
    r"(?im)^\s*`?grok\s+(?:resume|--resume|-r)\s+(?P<token>[^`\s]+)`?\s*$"
)


@dataclass(slots=True)
class GrokStreamState:
    resume: ResumeToken
    factory: EventFactory = field(default_factory=lambda: EventFactory(ENGINE))
    last_assistant_text: str = ""
    started: bool = False
    note_seq: int = 0
    pending_thought: list[str] = field(default_factory=list)
    # Text segmentation for answer/narration split.
    # current_text accumulates the active text run; text_segments holds
    # closed narration blocks. The trailing text run (after the last thought)
    # becomes the answer; earlier segments become coalesced note actions.
    current_text: str = ""
    text_segments: list[str] = field(default_factory=list)
    # Tool-call tracking: started action ids (prevents duplicate starts).
    seen_tool_calls: set[str] = field(default_factory=set)
    # Maps toolCallId -> (kind, title) from the original tool_call event,
    # so tool_call_update can emit a completed event with the same kind/title.
    tool_call_meta: dict[str, tuple[ActionKind, str]] = field(default_factory=dict)
    # Mid-stream usage events (merged into terminal CompletedEvent; end wins).
    mid_stream_usage: dict[str, Any] | None = None
    # True when the run was launched in plan mode (native read-only).
    plan_mode: bool = False


def _coerce_comma_list(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        parts = [str(item) for item in value if item is not None]
        joined = ",".join(part for part in parts if part)
        return joined or None
    text = str(value).strip()
    return text or None


def _usage_payload(
    event: grok_schema.StreamEndEvent | grok_schema.StreamErrorEvent,
) -> dict[str, Any]:
    usage: dict[str, Any] = {}
    for key in (
        "num_turns",
        "requestId",
        "stopReason",
        "total_cost_usd",
        "total_cost_usd_ticks",
        "cost_is_partial",
        "usage_is_incomplete",
    ):
        value = getattr(event, key, None)
        if value is not None:
            usage[key] = value
    if event.usage is not None:
        usage["usage"] = event.usage
    model_usage = getattr(event, "modelUsage", None)
    if model_usage is not None:
        usage["modelUsage"] = model_usage
    return usage


def _flush_pending_thought(state: GrokStreamState, out: list[UntetherEvent]) -> None:
    """Flush buffered thought chunks as ONE coalesced note action.

    The grok CLI emits ``thought`` events at word/token granularity. Without
    coalescing, each word becomes a separate progress step. This joins all
    pending chunks into a single action before the triggering event.
    """
    if not state.pending_thought:
        return
    title = "".join(state.pending_thought)
    state.pending_thought.clear()
    if not title.strip():
        return
    state.note_seq += 1
    out.append(
        state.factory.action_completed(
            action_id=f"grok.thought.{state.note_seq}",
            kind="note",
            title=title,
            ok=True,
            detail={},
        )
    )


def _close_text_segment(state: GrokStreamState) -> None:
    """Close the current text segment and start a new one.

    When a thought block arrives, the preceding text run is narration (not
    the final answer). It is moved to ``text_segments`` for later flushing
    as a coalesced note action.
    """
    if state.current_text:
        state.text_segments.append(state.current_text)
    state.current_text = ""


def _flush_text_segments(state: GrokStreamState, out: list[UntetherEvent]) -> str:
    """Flush narration text segments as note actions; return the answer.

    The trailing text run (``state.current_text``, not yet closed by a
    thought) is the answer. All segments in ``state.text_segments`` are
    closed narration blocks — they become coalesced note actions (one per
    segment, skipped when empty or whitespace-only, mirroring
    ``_flush_pending_thought`` rules).
    """
    for text in state.text_segments:
        if not text.strip():
            continue
        state.note_seq += 1
        out.append(
            state.factory.action_completed(
                action_id=f"grok.narration.{state.note_seq}",
                kind="note",
                title=text,
                ok=True,
                detail={},
            )
        )
    state.text_segments.clear()
    answer = state.current_text
    state.current_text = ""
    return answer


def _salvage_plan_answer(state: GrokStreamState, out: list[UntetherEvent]) -> str:
    """Collect ALL text content for plan-mode salvage.

    Unlike :func:`_flush_text_segments`, this does NOT emit narration as
    separate note actions. Instead, it combines every text segment
    (narration + trailing answer) into a single plan text, so the user
    receives the full plan even when the agent attempted a write/execute
    tool after producing it (which would otherwise close the plan text as
    narration and leave the trailing answer empty).
    """
    _flush_pending_thought(state, out)
    parts = [seg for seg in state.text_segments if seg.strip()]
    if state.current_text.strip():
        parts.append(state.current_text)
    state.text_segments.clear()
    state.current_text = ""
    return "\n\n".join(parts)


# Path keys for grok tool inputs (field names observed in real CLI captures).
_GROK_PATH_KEYS: tuple[str, ...] = (
    "target_file",
    "target_directory",
    "file_path",
    "path",
)

# Grok tool-name -> canonical name understood by the shared helper.
# Tools not listed here fall through to the generic ("tool", tool_name) tail.
_GROK_TOOL_NAME_MAP: dict[str, str] = {
    "run_terminal_command": "bash",
    "read_file": "read",
    "search_replace": "edit",
    "write": "edit",
    "list_dir": "ls",
    "grep": "grep",
    "todo_write": "todowrite",
    "spawn_subagent": "task",
}

# Grok rawInput field -> normalized field (shared helper looks for these).
_GROK_INPUT_FIELD_MAP: dict[str, str] = {
    "target_file": "file_path",
    "target_directory": "path",
}


def _grok_tool_kind_and_title(
    tool_name: str,
    raw_input: Any,
) -> tuple[ActionKind, str]:
    """Map grok tool names/fields to the shared helper's canonical contract.

    Translates the grok tool name (e.g. ``run_terminal_command``) to the
    canonical name (``bash``) and normalizes input fields (``target_file`` →
    ``file_path``), then delegates to :func:`tool_kind_and_title`.
    """
    canonical = _GROK_TOOL_NAME_MAP.get(tool_name.lower(), tool_name)
    normalized: dict[str, Any] = dict(raw_input) if raw_input else {}
    for grok_key, canon_key in _GROK_INPUT_FIELD_MAP.items():
        if grok_key in normalized:
            normalized[canon_key] = normalized[grok_key]
    return tool_kind_and_title(canonical, normalized, path_keys=_GROK_PATH_KEYS)


def translate_grok_event(
    event: grok_schema.GrokEvent,
    *,
    title: str,
    state: GrokStreamState,
    meta: dict[str, Any] | None = None,
) -> list[UntetherEvent]:
    out: list[UntetherEvent] = []

    if not state.started:
        state.started = True
        out.append(state.factory.started(state.resume, title=title, meta=meta or None))

    match event:
        case grok_schema.StreamTextEvent(data=data):
            if data:
                _flush_pending_thought(state, out)
                state.current_text += data
            return out

        case grok_schema.StreamToolCallEvent():
            # Flush any pending thoughts before the tool action.
            _flush_pending_thought(state, out)
            # Tool calls act as narration delimiters: text accumulated before
            # a tool call is narration, not the final answer.
            _close_text_segment(state)
            call_id = event.toolCallId
            if call_id and call_id not in state.seen_tool_calls:
                state.seen_tool_calls.add(call_id)
                tool_name = str(event.toolName or event.title or "tool")
                kind, action_title = _grok_tool_kind_and_title(
                    tool_name, event.rawInput
                )
                state.tool_call_meta[call_id] = (kind, action_title)
                detail: dict[str, Any] = {"name": tool_name, "input": event.rawInput}
                out.append(
                    state.factory.action_started(
                        action_id=call_id,
                        kind=kind,
                        title=action_title,
                        detail=detail,
                    )
                )
            return out

        case grok_schema.StreamToolCallUpdateEvent():
            _flush_pending_thought(state, out)
            _close_text_segment(state)
            call_id = event.toolCallId
            status = (event.status or "").lower()
            ok = status != "error"
            # Reuse kind/title from the original tool_call; fall back to generic.
            kind_str, title_str = state.tool_call_meta.get(call_id, ("tool", call_id))
            out.append(
                state.factory.action_completed(
                    action_id=call_id,
                    kind=kind_str,
                    title=title_str,
                    ok=ok,
                    detail={"status": event.status},
                )
            )
            return out

        case grok_schema.StreamUsageEvent():
            _flush_pending_thought(state, out)
            state.mid_stream_usage = event.usage
            return out

        case grok_schema.StreamAvailableCommandsEvent():
            # No action needed; suppress.
            return out

        case grok_schema.StreamUnknownEvent():
            # Forward-compat: unrecognized type, DEBUG only, no events.
            logger.debug(
                "grok.stream.unknown_type",
                type_name=event.type_name,
            )
            return out

        case grok_schema.StreamThoughtEvent(data=data):
            # A thought block closes the current text segment (narration).
            _close_text_segment(state)
            if data:
                state.pending_thought.append(data)
            return out

        case grok_schema.StreamEndEvent():
            stop = (event.stopReason or "").lower()
            is_cancel = stop in {"cancelled", "canceled"}
            session_id = event.sessionId or state.resume.value
            resume = ResumeToken(engine=ENGINE, value=session_id)
            # Keep factory resume aligned with the canonical token we started with
            # unless the CLI reports a different session id (should match --session-id).
            if session_id != state.resume.value:
                state.resume = resume
            usage = _usage_payload(event)
            if state.mid_stream_usage is not None:
                usage["mid_stream_usage"] = state.mid_stream_usage

            # Salvage path: plan-mode cancel with text content.
            # When a plan-mode run is cancelled but plan text was produced,
            # deliver the plan as a soft success instead of an opaque error.
            # The plan text may be in narration segments (closed by the
            # tool call that triggered the cancel) OR in the trailing answer.
            if state.plan_mode and is_cancel:
                answer = _salvage_plan_answer(state, out)
                if answer.strip():
                    ok = True
                    error = None
                    answer = f"{answer.rstrip()}\n\n{_PLAN_CANCEL_SALVAGE_NOTE}"
                else:
                    ok = False
                    error = _PLAN_CANCEL_ERROR
            else:
                # Normal path: flush narration as notes, trailing text = answer.
                # Flush narration segments before pending thoughts: the narration
                # was closed by the thought that follows it, so it predates the
                # pending thought block chronologically.
                answer = _flush_text_segments(state, out)
                _flush_pending_thought(state, out)
                ok = stop not in {"error", "aborted", "cancelled", "canceled"}
                error = f"grok run stopped ({event.stopReason})" if not ok else None

            state.last_assistant_text = answer
            out.append(
                state.factory.completed(
                    ok=ok,
                    answer=answer,
                    resume=resume,
                    error=error,
                    usage=usage or None,
                )
            )
            return out

        case grok_schema.StreamErrorEvent():
            answer = _flush_text_segments(state, out)
            _flush_pending_thought(state, out)
            session_id = event.sessionId or state.resume.value
            resume = ResumeToken(engine=ENGINE, value=session_id)
            usage = _usage_payload(event)
            if state.mid_stream_usage is not None:
                usage["mid_stream_usage"] = state.mid_stream_usage
            message = event.message or "grok run failed"
            state.last_assistant_text = answer
            out.append(
                state.factory.completed(
                    ok=False,
                    answer=answer,
                    resume=resume,
                    error=message,
                    usage=usage or None,
                )
            )
            return out

        case _:
            _flush_pending_thought(state, out)
            return out


@dataclass(slots=True)
class GrokRunner(HandoffCompactMixin, ResumeTokenMixin, JsonlSubprocessRunner):
    engine: EngineId = ENGINE
    resume_re: re.Pattern[str] = _RESUME_RE
    # Plan mode keeps native --permission-mode plan AND restricts the
    # toolset to a read-only allow-list (--tools). Mutating tools are
    # physically absent, so no approval prompt fires and no cancellation
    # occurs (Task 16 probe D2: end_turn, no file, text delivered).
    # plan_enforcement is reserved for future tunability.
    plan_enforcement: str = "allowlist"

    grok_cmd: str = "grok"
    model: str | None = None
    yolo: bool = True
    tools: list[str] | str | None = None
    disallowed_tools: list[str] | str | None = None
    reasoning_effort: str | None = None
    max_turns: int | None = None
    extra_args: list[str] = field(default_factory=list)
    session_title: str = "grok"
    logger = logger

    def format_resume(self, token: ResumeToken) -> str:
        if token.engine != ENGINE:
            raise RuntimeError(f"resume token is for engine {token.engine!r}")
        return f"`grok --resume {token.value}`"

    def is_resume_line(self, line: str) -> bool:
        return bool(_RESUME_LINE_RE.match(line))

    def command(self) -> str:
        return self.grok_cmd

    def build_args(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: GrokStreamState,
    ) -> list[str]:
        run_options = get_run_options()
        plan, _goal = run_modes(run_options)
        args: list[str] = [*self.extra_args]
        args.extend(["--output-format", "streaming-json"])

        if plan:
            # Hard enforcement: native --permission-mode plan + read-only
            # tools allow-list. Mutating tools are physically absent so the
            # agent cannot trigger an approval prompt -> no cancellation.
            # The plan_mode flag is still set on state so the salvage net can
            # fire if a cancellation occurs for any other reason (upstream
            # abort, timeout, etc.).
            state.plan_mode = True
            args.extend(["--permission-mode", "plan"])
            args.extend(["--tools", _PLAN_READONLY_TOOLS])
            prompt = effective_prompt(prompt, soft_plan=True, options=run_options)
        elif self.yolo is True:
            args.append("--yolo")

        # effective_prompt handles goal mode (/goal prefix) and plan mode
        # (soft-plan prefix). With soft_plan=True it is a no-op outside plan
        # mode; goal mode is applied regardless.
        if not plan:
            prompt = effective_prompt(prompt, soft_plan=False, options=run_options)

        args.extend(["-p", prompt])

        model = self.model
        if run_options is not None and run_options.model:
            model = run_options.model
        if model is not None:
            args.extend(["-m", str(model)])

        reasoning = self.reasoning_effort
        if run_options is not None and run_options.reasoning:
            reasoning = run_options.reasoning
        if reasoning is not None:
            args.extend(["--effort", str(reasoning)])

        if run_options is not None and run_options.subagent:
            args.extend(["--agent", str(run_options.subagent)])

        # In plan mode the allow-list is fixed to _PLAN_READONLY_TOOLS.
        # Outside plan mode, honor the user-configured tools/disallowed-tools.
        if not plan:
            tools = _coerce_comma_list(self.tools)
            if tools is not None:
                args.extend(["--tools", tools])

            disallowed = _coerce_comma_list(self.disallowed_tools)
            if disallowed is not None:
                args.extend(["--disallowed-tools", disallowed])

        if self.max_turns is not None:
            args.extend(["--max-turns", str(self.max_turns)])

        if resume is not None:
            args.extend(["--resume", resume.value])
        else:
            args.extend(["--session-id", state.resume.value])

        return args

    def stdin_payload(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: GrokStreamState,
    ) -> bytes | None:
        return None

    def env(self, *, state: GrokStreamState) -> dict[str, str] | None:
        env = dict(os.environ)
        env.setdefault("NO_COLOR", "1")
        env.setdefault("GROK_DISABLE_AUTOUPDATER", "1")
        return env

    def new_state(self, prompt: str, resume: ResumeToken | None) -> GrokStreamState:
        if resume is not None:
            token = resume
            if token.engine != ENGINE:
                token = ResumeToken(engine=ENGINE, value=resume.value)
        else:
            token = ResumeToken(engine=ENGINE, value=str(uuid4()))
        return GrokStreamState(resume=token, started=False)

    def decode_jsonl(self, *, line: bytes) -> grok_schema.GrokEvent:
        return grok_schema.decode_event(line)

    def decode_error_events(
        self,
        *,
        raw: str,
        line: str,
        error: Exception,
        state: GrokStreamState,
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

    def invalid_json_events(
        self,
        *,
        raw: str,
        line: str,
        state: GrokStreamState,
    ) -> list[UntetherEvent]:
        return []

    def selected_model(self) -> str | None:
        """Return the explicit model selected for this invocation."""
        run_options = get_run_options()
        if run_options is not None and run_options.model:
            return run_options.model
        return self.model

    def translate(
        self,
        data: grok_schema.GrokEvent,
        *,
        state: GrokStreamState,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
    ) -> list[UntetherEvent]:
        meta: dict[str, Any] = {
            "cwd": os.getcwd(),
            "model": self.selected_model() or "auto",
        }
        return translate_grok_event(
            data,
            title=self.session_title,
            state=state,
            meta=meta or None,
        )

    def process_error_events(
        self,
        rc: int,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: GrokStreamState,
        stderr_lines: list[str] | None = None,
    ) -> list[UntetherEvent]:
        message = f"grok failed (rc={rc})."
        excerpt = _stderr_excerpt(stderr_lines)
        if excerpt:
            message = f"{message}\n{excerpt}"
        resume_for_completed = found_session or resume or state.resume
        out: list[UntetherEvent] = []
        answer = _flush_text_segments(state, out)
        _flush_pending_thought(state, out)
        if state.started or answer:
            out.append(self.note_event(message, state=state, ok=False))
        out.append(
            state.factory.completed_error(
                error=message,
                answer=answer,
                resume=resume_for_completed,
            )
        )
        return out

    def stream_end_events(
        self,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: GrokStreamState,
        stderr_lines: list[str] | None = None,
    ) -> list[UntetherEvent]:
        resume_for_completed = found_session or resume or state.resume
        message = "grok finished without an end event"
        out: list[UntetherEvent] = []
        if not state.started:
            out.append(
                state.factory.started(
                    resume_for_completed or state.resume,
                    title=self.session_title,
                )
            )
        state.started = True
        answer = _flush_text_segments(state, out)
        _flush_pending_thought(state, out)
        out.append(
            state.factory.completed_error(
                error=message,
                answer=answer,
                resume=resume_for_completed,
            )
        )
        return out


def build_runner(config: EngineConfig, config_path: Path) -> Runner:
    grok_cmd = shutil.which("grok") or "grok"

    model = config.get("model")
    if model is not None and not isinstance(model, str):
        raise ConfigError(f"Invalid `grok.model` in {config_path}; expected a string.")

    yolo = True if "yolo" not in config else config.get("yolo") is True

    tools = config.get("tools")
    if tools is not None and not isinstance(tools, (str, list, tuple, set)):
        raise ConfigError(
            f"Invalid `grok.tools` in {config_path}; expected a string or list of strings."
        )

    disallowed_tools = config.get("disallowed_tools")
    if disallowed_tools is not None and not isinstance(
        disallowed_tools, (str, list, tuple, set)
    ):
        raise ConfigError(
            f"Invalid `grok.disallowed_tools` in {config_path}; "
            "expected a string or list of strings."
        )

    reasoning_effort = config.get("reasoning_effort")
    if reasoning_effort is not None and not isinstance(reasoning_effort, str):
        raise ConfigError(
            f"Invalid `grok.reasoning_effort` in {config_path}; expected a string."
        )

    max_turns = config.get("max_turns")
    if max_turns is not None and not isinstance(max_turns, int):
        raise ConfigError(
            f"Invalid `grok.max_turns` in {config_path}; expected an integer."
        )

    extra_args_value = config.get("extra_args")
    if extra_args_value is None:
        extra_args: list[str] = []
    elif isinstance(extra_args_value, list) and all(
        isinstance(x, str) for x in extra_args_value
    ):
        extra_args = list(extra_args_value)
    else:
        raise ConfigError(
            f"Invalid `grok.extra_args` in {config_path}; expected a list of strings."
        )

    title = str(model) if model is not None else "grok"

    return cast(
        Runner,
        GrokRunner(
            grok_cmd=grok_cmd,
            model=model,
            yolo=yolo,
            tools=tools,
            disallowed_tools=disallowed_tools,
            reasoning_effort=reasoning_effort,
            max_turns=max_turns,
            extra_args=extra_args,
            session_title=title,
        ),
    )


BACKEND = EngineBackend(
    id="grok",
    build_runner=build_runner,
    cli_cmd="grok",
    install_cmd="Install Grok Build CLI (grok) and ensure it is on PATH",
)
