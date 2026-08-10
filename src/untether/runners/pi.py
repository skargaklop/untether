from __future__ import annotations

import contextlib
import os
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePath
from typing import Any, cast
from uuid import uuid4

import msgspec

from ..backends import EngineBackend, EngineConfig
from ..config import ConfigError
from ..logging import get_logger
from ..model import (
    Action,
    ActionEvent,
    ActionKind,
    ActionLevel,
    ActionPhase,
    CompletedEvent,
    EngineId,
    ResumeToken,
    StartedEvent,
    UntetherEvent,
)
from ..runner import (
    JsonlSubprocessRunner,
    ResumeTokenMixin,
    Runner,
    _rc_label,
    _session_label,
    _stderr_excerpt,
)
from ..schemas import pi as pi_schema
from ..utils.paths import get_run_base_dir
from .modes import apply_soft_plan_prompt, run_modes
from .run_options import get_run_options
from .tool_actions import tool_kind_and_title

logger = get_logger(__name__)

ENGINE: EngineId = "pi"

_RESUME_RE = re.compile(r"(?im)^\s*`?pi\s+--session\s+(?P<token>.+?)`?\s*$")

_SESSION_ID_PREFIX_LEN = 8


def _load_env_extras() -> tuple[tuple[str, ...], tuple[str, ...]]:
    """#409: read [security] env_extra_allow / env_extra_prefix_allow.

    Best-effort — config errors must never block a run, so we swallow
    them and fall back to the built-in defaults. Returns
    ``(extra_exact, extra_prefix)``.
    """
    from ..settings import load_settings_if_exists

    try:
        result = load_settings_if_exists()
        if result is None:
            return ((), ())
        settings, _ = result
        return (
            tuple(settings.security.env_extra_allow),
            tuple(settings.security.env_extra_prefix_allow),
        )
    except Exception:  # noqa: BLE001 — never let config errors block a run
        return ((), ())


@dataclass(slots=True)
class PiStreamState:
    resume: ResumeToken
    allow_id_promotion: bool = False
    pending_actions: dict[str, Action] = field(default_factory=dict)
    last_assistant_text: str | None = None
    last_assistant_error: str | None = None
    last_usage: dict[str, Any] | None = None
    started: bool = False
    note_seq: int = 0
    compaction_seq: int = 0
    compaction_action_id: str | None = None
    # #460: stable action id across an AutoRetryStart/AutoRetryEnd pair so the
    # progress UI updates the same action rather than stacking a new one.
    retry_seq: int = 0
    retry_action_id: str | None = None
    # #225: latch once the runner has emitted the JSONL-derived model into
    # meta. Prevents us from re-emitting supplementary StartedEvents on every
    # subsequent message_end when the default-config model path is in use.
    jsonl_model_emitted: bool = False
    shorten_session_id: bool = True


def _looks_like_session_path(token: str) -> bool:
    if not token:
        return False
    if token.endswith(".jsonl"):
        return True
    if "/" in token or "\\" in token:
        return True
    return token.startswith("~")


def _short_session_id(session_id: str) -> str:
    if not session_id:
        return session_id
    if "-" in session_id:
        return session_id.split("-", 1)[0]
    if len(session_id) > _SESSION_ID_PREFIX_LEN:
        return session_id[:_SESSION_ID_PREFIX_LEN]
    return session_id


def _maybe_promote_session_id(state: PiStreamState, session_id: str | None) -> None:
    if not session_id:
        return
    if state.started:
        return
    if not state.allow_id_promotion:
        return
    # For /continue runs the resume value is empty; for fresh runs it's a
    # session path — either way, promotion is allowed when the flag is set.
    if state.resume.value and not _looks_like_session_path(state.resume.value):
        return
    old_value = state.resume.value
    value = _short_session_id(session_id) if state.shorten_session_id else session_id
    state.resume = ResumeToken(engine=ENGINE, value=value)
    state.allow_id_promotion = False
    logger.info("pi.session.promoted", old=old_value, new=state.resume.value)


def _action_event(
    *,
    phase: ActionPhase,
    action: Action,
    ok: bool | None = None,
    message: str | None = None,
    level: ActionLevel | None = None,
) -> ActionEvent:
    return ActionEvent(
        engine=ENGINE,
        action=action,
        phase=phase,
        ok=ok,
        message=message,
        level=level,
    )


def _format_retry_delay(delay_ms: int) -> str:
    """Render an AutoRetry ``delayMs`` as a compact human string (#460)."""
    if delay_ms >= 1000:
        seconds = delay_ms / 1000
        return f"{seconds:.0f}s" if delay_ms % 1000 == 0 else f"{seconds:.1f}s"
    return f"{delay_ms}ms"


def _extract_text_blocks(content: Any) -> str | None:
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "text":
            continue
        text = item.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    if not parts:
        return None
    return "".join(parts).strip() or None


def _assistant_error(message: dict[str, Any]) -> str | None:
    stop_reason = message.get("stopReason")
    if stop_reason in {"error", "aborted"}:
        error = message.get("errorMessage")
        if isinstance(error, str) and error:
            return error
        return f"pi run {stop_reason}"
    return None


def _tool_kind_and_title(
    name: str,
    args: dict[str, Any],
) -> tuple[ActionKind, str]:
    return tool_kind_and_title(name, args, path_keys=("path",))


def _last_assistant_message(messages: Any) -> dict[str, Any] | None:
    if not isinstance(messages, list):
        return None
    for item in reversed(messages):
        if isinstance(item, dict) and item.get("role") == "assistant":
            return item
    return None


def translate_pi_event(
    event: pi_schema.PiEvent,
    *,
    title: str,
    meta: dict[str, Any] | None,
    state: PiStreamState,
) -> list[UntetherEvent]:
    out: list[UntetherEvent] = []
    if isinstance(event, pi_schema.SessionHeader):
        _maybe_promote_session_id(state, event.id)
        if not state.started:
            logger.info("pi.session.started", resume=state.resume.value, title=title)
            out.append(
                StartedEvent(
                    engine=ENGINE,
                    resume=state.resume,
                    title=title,
                    meta=meta or None,
                )
            )
            state.started = True
        return out

    if not state.started:
        logger.info(
            "pi.session.started.implicit",
            resume=state.resume.value,
            event_type=type(event).__name__,
        )
        out.append(
            StartedEvent(
                engine=ENGINE,
                resume=state.resume,
                title=title,
                meta=meta or None,
            )
        )
        state.started = True

    match event:
        case pi_schema.ToolExecutionStart(
            toolCallId=tool_id, toolName=tool_name, args=args
        ):
            if not isinstance(args, dict):
                args = {}
            if isinstance(tool_id, str) and tool_id:
                name = str(tool_name or "tool")
                kind, title_str = _tool_kind_and_title(name, args)
                detail: dict[str, Any] = {"tool_name": name, "args": args}
                if kind == "file_change":
                    path = args.get("path")
                    if path:
                        detail["changes"] = [{"path": str(path), "kind": "update"}]
                action = Action(id=tool_id, kind=kind, title=title_str, detail=detail)
                state.pending_actions[action.id] = action
                out.append(_action_event(phase="started", action=action))
            return out

        case pi_schema.ToolExecutionEnd(
            toolCallId=tool_id, toolName=tool_name, result=result, isError=is_error
        ):
            if isinstance(tool_id, str) and tool_id:
                action = state.pending_actions.pop(tool_id, None)
                name = str(tool_name or "tool")
                if action is None:
                    action = Action(id=tool_id, kind="tool", title=name, detail={})
                detail = dict(action.detail)
                detail["result"] = result
                detail["is_error"] = is_error
                out.append(
                    _action_event(
                        phase="completed",
                        action=Action(
                            id=action.id,
                            kind=action.kind,
                            title=action.title,
                            detail=detail,
                        ),
                        ok=not is_error,
                    )
                )
            return out

        case pi_schema.MessageEnd(message=message):
            if isinstance(message, dict) and message.get("role") == "assistant":
                text = _extract_text_blocks(message.get("content"))
                if text:
                    state.last_assistant_text = text
                usage = message.get("usage")
                if isinstance(usage, dict):
                    state.last_usage = usage
                error = _assistant_error(message)
                if error:
                    state.last_assistant_error = error
                # #225: when the user relies on Pi's default config model (no
                # /model override and no pi.model in untether.toml), SessionHeader
                # was emitted without a model in meta. Pi's message_end carries
                # the actual model used (e.g. "gpt-5.4"); extract it once per
                # session and emit a supplementary StartedEvent so the footer
                # picks it up via ProgressTracker.note_event's meta merge.
                if not state.jsonl_model_emitted and (
                    meta is None or not meta.get("model")
                ):
                    jsonl_model = message.get("model")
                    if isinstance(jsonl_model, str) and jsonl_model:
                        jsonl_meta: dict[str, Any] = {"model": jsonl_model}
                        jsonl_provider = message.get("provider")
                        if isinstance(jsonl_provider, str) and jsonl_provider:
                            jsonl_meta.setdefault("provider", jsonl_provider)
                        out.append(
                            StartedEvent(
                                engine=ENGINE,
                                resume=state.resume,
                                title=title,
                                meta=jsonl_meta,
                            )
                        )
                        state.jsonl_model_emitted = True
            return out

        case pi_schema.AgentEnd(messages=messages):
            assistant = _last_assistant_message(messages)
            if assistant:
                text = _extract_text_blocks(assistant.get("content"))
                if text:
                    state.last_assistant_text = text
                usage = assistant.get("usage")
                if isinstance(usage, dict):
                    state.last_usage = usage
                error = _assistant_error(assistant)
                if error:
                    state.last_assistant_error = error

            ok = state.last_assistant_error is None
            error = state.last_assistant_error
            answer = state.last_assistant_text or ""

            logger.info(
                "pi.completed",
                ok=ok,
                error=error,
                resume=state.resume.value,
                answer_len=len(answer),
            )
            out.append(
                CompletedEvent(
                    engine=ENGINE,
                    ok=ok,
                    answer=answer,
                    resume=state.resume,
                    error=error,
                    usage=state.last_usage,
                )
            )
            return out

        case pi_schema.AutoCompactionStart(reason=reason):
            state.compaction_seq += 1
            action_id = f"compaction_{state.compaction_seq}"
            state.compaction_action_id = action_id
            reason_str = f" ({reason})" if reason else ""
            out.append(
                _action_event(
                    phase="started",
                    action=Action(
                        id=action_id,
                        kind="note",
                        title=f"compacting context…{reason_str}",
                        detail={},
                    ),
                )
            )
            return out

        case pi_schema.AutoCompactionEnd(result=result, aborted=aborted):
            action_id = (
                state.compaction_action_id or f"compaction_{state.compaction_seq}"
            )
            state.compaction_action_id = None
            tokens_before = None
            if isinstance(result, dict):
                tokens_before = result.get("tokensBefore")
            if aborted:
                title = "context compaction aborted"
            elif tokens_before:
                title = f"context compacted ({tokens_before:,} tokens)"
            else:
                title = "context compacted"
            out.append(
                _action_event(
                    phase="completed",
                    action=Action(
                        id=action_id,
                        kind="note",
                        title=title,
                        detail={"result": result} if result else {},
                    ),
                    ok=not aborted,
                )
            )
            return out

        case pi_schema.AutoRetryStart(
            attempt=attempt,
            maxAttempts=max_attempts,
            delayMs=delay_ms,
            errorMessage=error_message,
        ):
            # #460: surface transient provider retries so they're visible in
            # Telegram and the liveness watchdog sees event activity at each
            # retry boundary (rather than mistaking the backoff gap for a stall).
            state.retry_seq += 1
            action_id = f"retry_{state.retry_seq}"
            state.retry_action_id = action_id
            bits: list[str] = []
            if attempt is not None:
                if max_attempts:
                    bits.append(f"attempt {attempt}/{max_attempts}")
                else:
                    bits.append(f"attempt {attempt}")
            if delay_ms:
                bits.append(f"~{_format_retry_delay(int(delay_ms))} delay")
            suffix = f" ({', '.join(bits)})" if bits else ""
            retry_detail: dict[str, Any] = {}
            if error_message:
                retry_detail["error"] = error_message
            out.append(
                _action_event(
                    phase="started",
                    action=Action(
                        id=action_id,
                        kind="note",
                        title=f"retrying provider{suffix}",
                        detail=retry_detail,
                    ),
                )
            )
            return out

        case pi_schema.AutoRetryEnd(success=success, finalError=final_error):
            action_id = state.retry_action_id or f"retry_{state.retry_seq}"
            state.retry_action_id = None
            if success:
                retry_title = "retry succeeded"
                retry_ok = True
            else:
                retry_title = (
                    f"retry exhausted: {final_error}"
                    if final_error
                    else "retry exhausted"
                )
                retry_ok = False
            out.append(
                _action_event(
                    phase="completed",
                    action=Action(
                        id=action_id,
                        kind="note",
                        title=retry_title,
                        detail={"final_error": final_error} if final_error else {},
                    ),
                    ok=retry_ok,
                )
            )
            return out

        case _:
            logger.debug(
                "pi.event.unrecognised",
                event_type=type(event).__name__,
            )
            return out


class PiRunner(ResumeTokenMixin, JsonlSubprocessRunner):
    engine: EngineId = ENGINE
    resume_re: re.Pattern[str] = _RESUME_RE
    session_title: str = "pi"
    logger = logger

    def __init__(
        self,
        *,
        extra_args: list[str],
        model: str | None,
        provider: str | None,
        plan_mode_extension: bool = False,
        goal_list_extension: bool = False,
    ) -> None:
        self.extra_args = extra_args
        self.model = model
        self.provider = provider
        self.plan_mode_extension = plan_mode_extension
        self.goal_list_extension = goal_list_extension
        self._plan_warning_logged = False

    def format_resume(self, token: ResumeToken) -> str:
        if token.engine != ENGINE:
            raise RuntimeError(f"resume token is for engine {token.engine!r}")
        return f"`pi --session {self._quote_token(token.value)}`"

    def run(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]:
        return super().run(prompt, resume)

    def extract_resume(self, text: str | None) -> ResumeToken | None:
        if not text:
            return None
        found: str | None = None
        for match in self.resume_re.finditer(text):
            token = match.group("token")
            if not token:
                continue
            token = token.strip()
            if len(token) >= 2 and token[0] == token[-1] and token[0] in {'"', "'"}:
                token = token[1:-1]
            found = token
        if not found:
            return None
        return ResumeToken(engine=self.engine, value=found)

    def _final_prompt(self, prompt: str, *, resume: ResumeToken | None = None) -> str:
        """Apply goal/plan mode mutations to the prompt.

        - Goal mode + goal-list extension + fresh session: seeds
          ``<task-goal>{escaped goal}</task-goal>`` as the first message.
        - Goal mode + no extension (or resumed session): injects the
          autonomous-goal prefix.
        - Plan mode + extension: delegates to the extension via ``--plan``,
          no prompt mutation.
        - Plan mode + no extension: applies the soft-plan prompt prefix
          (graceful fallback) and logs a one-time warning.
        """
        run_options = get_run_options()
        plan, goal = run_modes(run_options)
        if goal is not None:
            body = prompt.strip()
            is_fresh = resume is None
            if self.goal_list_extension and is_fresh:
                escaped = _escape_goal_xml(goal)
                directive = f"<task-goal>{escaped}</task-goal>"
                return f"{directive}\n\n{body}" if body else directive
            note = f"(autonomous goal — work until: {goal})"
            return f"{note}\n\n{body}" if body else note
        if plan and not self.plan_mode_extension:
            if not self._plan_warning_logged:
                logger.warning("pi.plan_mode_extension_missing")
                self._plan_warning_logged = True
            return apply_soft_plan_prompt(prompt)
        return prompt

    @staticmethod
    def _prompt_needs_stdin(prompt: str) -> bool:
        """True when the prompt must be sent via stdin instead of a CLI arg.

        ``pi.cmd`` (the Windows batch wrapper) rejects argv elements containing
        newlines with "batch file arguments are invalid" (rc=126). The
        autonomous-goal prefix and any multi-line user prompt inject newlines,
        so they are piped through stdin; single-line prompts keep the argv
        path to match the existing session/attachment ordering contract.
        """
        return "\n" in prompt

    def build_args(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: PiStreamState,
    ) -> list[str]:
        run_options = get_run_options()
        plan, _goal = run_modes(run_options)
        final_prompt = self._final_prompt(prompt, resume=resume)
        args: list[str] = [*self.extra_args, "--print", "--mode", "json"]
        if self.provider:
            args.extend(["--provider", self.provider])
        model = self.model
        if run_options is not None and run_options.model:
            model = run_options.model
        if model:
            args.extend(["--model", model])
        if plan and self.plan_mode_extension:
            args.append("--plan")
        if state.resume.is_continue:
            args.append("--continue")
        else:
            args.extend(["--session", state.resume.value])
        # Layer B: pi accepts @file references in the initial message list.
        if run_options is not None:
            args.extend(
                f"@{attachment.rel_path}"
                for attachment in run_options.attachments
                if attachment.kind == "image" and attachment.rel_path
            )
        if not self._prompt_needs_stdin(final_prompt):
            args.append(self.sanitize_prompt(final_prompt))
        return args

    def stdin_payload(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: PiStreamState,
    ) -> bytes | None:
        final_prompt = self._final_prompt(prompt, resume=resume)
        if not self._prompt_needs_stdin(final_prompt):
            return None
        # Newline-terminated UTF-8 bytes for the multi-line/Windows-safe path.
        return (final_prompt + "\n").encode()

    def env(self, *, state: PiStreamState) -> dict[str, str] | None:
        # #198: allowlist filter — Pi subprocess no longer inherits the
        # parent's full environment. See `utils/env_policy.py` for the
        # canonical list + extension notes. #409: thread per-deployment
        # extras from [security] env_extra_allow / env_extra_prefix_allow.
        from ..utils.env_policy import filtered_env, log_user_extensions_once

        extra_exact, extra_prefix = _load_env_extras()
        log_user_extensions_once(extra_exact, extra_prefix)
        env = filtered_env(extra_allow=extra_exact, extra_prefix=extra_prefix)
        env.setdefault("NO_COLOR", "1")
        env.setdefault("CI", "1")
        return env

    def new_state(self, prompt: str, resume: ResumeToken | None) -> PiStreamState:
        if resume is None:
            session_path = self._new_session_path()
            token = ResumeToken(engine=ENGINE, value=session_path)
            return PiStreamState(
                resume=token,
                allow_id_promotion=True,
            )
        return PiStreamState(resume=resume, allow_id_promotion=resume.is_continue)

    def translate(
        self,
        data: pi_schema.PiEvent,
        *,
        state: PiStreamState,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
    ) -> list[UntetherEvent]:
        meta: dict[str, Any] = {"cwd": os.getcwd()}
        model = self.model
        run_options = get_run_options()
        if run_options is not None and run_options.model:
            model = run_options.model
        if model:
            meta["model"] = model
        if self.provider:
            meta["provider"] = self.provider
        return translate_pi_event(
            data,
            title=self.session_title,
            meta=meta or None,
            state=state,
        )

    def decode_jsonl(
        self,
        *,
        line: bytes,
    ) -> pi_schema.PiEvent:
        return pi_schema.decode_event(line)

    def decode_error_events(
        self,
        *,
        raw: str,
        line: str,
        error: Exception,
        state: PiStreamState,
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

    def process_error_events(
        self,
        rc: int,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: PiStreamState,
        stderr_lines: list[str] | None = None,
    ) -> list[UntetherEvent]:
        parts = [f"pi failed ({_rc_label(rc)})."]
        session = _session_label(found_session, resume)
        if session:
            parts.append(f"session: {session}")
        excerpt = _stderr_excerpt(stderr_lines)
        if excerpt:
            parts.append(excerpt)
        message = "\n".join(parts)
        logger.error("pi.process.failed", rc=rc, resume=state.resume.value)
        resume_for_completed = found_session or resume or state.resume
        return [
            self.note_event(message, state=state),
            CompletedEvent(
                engine=ENGINE,
                ok=False,
                answer=state.last_assistant_text or "",
                resume=resume_for_completed,
                error=message,
                usage=state.last_usage,
            ),
        ]

    def stream_end_events(
        self,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: PiStreamState,
        stderr_lines: list[str] | None = None,
    ) -> list[UntetherEvent]:
        resume_for_completed = found_session or resume or state.resume
        resumed = resume is not None
        # #565: Untether was blind to *why* Pi exited on this path — the old
        # hardcoded "finished without an agent_end event" message hid both the
        # failure category and Pi's own stderr. Distinguish two cases:
        #   * zero translated events (rc=0, ~1.5s) → a startup/early-exit crash,
        #     e.g. MCP servers still cold while a resumed session rehydrates
        #     tool state (the real, transient cause behind the original report).
        #   * events seen but no agent_end → a genuinely truncated stream.
        # ``state.started`` flips True on the first translated event, so it is a
        # reliable "did Pi produce anything?" signal without threading the raw
        # JSONL line count through the polymorphic call.
        if not state.started:
            opener = (
                "pi exited cleanly (rc=0) but produced no events — likely a "
                "startup/early-exit failure (e.g. MCP servers still connecting)"
            )
            if resumed:
                opener += "; the session may have failed to load on resume"
            parts = [opener + "."]
        else:
            parts = ["pi finished without an agent_end event"]
        session = _session_label(found_session, resume)
        if session:
            parts.append(f"session: {session}")
        excerpt = _stderr_excerpt(stderr_lines)
        if excerpt:
            parts.append(excerpt)
        message = "\n".join(parts)
        logger.warning(
            "pi.stream.no_agent_end",
            resume=state.resume.value,
            had_events=state.started,
            resumed=resumed,
        )
        return [
            CompletedEvent(
                engine=ENGINE,
                ok=False,
                answer=state.last_assistant_text or "",
                resume=resume_for_completed,
                error=message,
                usage=state.last_usage,
            )
        ]

    def _new_session_path(self) -> str:
        cwd = get_run_base_dir() or Path.cwd()
        session_dir = _default_session_dir(cwd)
        # #207: 0o700 keeps Pi session JSONL out of reach of other users on
        # shared hosts. mkdir's mode arg is ignored for existing dirs, so
        # chmod the directory after to also tighten any pre-existing one.
        session_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        with contextlib.suppress(OSError):
            session_dir.chmod(0o700)
        timestamp = datetime.now(UTC).isoformat()
        safe_timestamp = timestamp.replace(":", "-").replace(".", "-")
        token = uuid4().hex
        filename = f"{safe_timestamp}_{token}.jsonl"
        return str(session_dir / filename)

    def _quote_token(self, token: str) -> str:
        if not token:
            return token
        needs_quotes = any(ch.isspace() for ch in token)
        if not needs_quotes and '"' not in token:
            return token
        escaped = token.replace('"', '\\"')
        return f'"{escaped}"'


def _default_session_dir(cwd: PurePath) -> Path:
    agent_dir = os.environ.get("PI_CODING_AGENT_DIR")
    base = Path(agent_dir).expanduser() if agent_dir else Path.home() / ".pi" / "agent"
    cwd_str = str(cwd).lstrip("/\\")
    safe_path_part = cwd_str.translate(str.maketrans({"/": "-", "\\": "-", ":": "-"}))
    safe_path = f"--{safe_path_part}--"
    return base / "sessions" / safe_path


def _escape_goal_xml(goal: str) -> str:
    """Escape ``&``, ``<``, ``>`` so user content cannot close the directive."""
    return goal.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_PLAN_MODE_EXTENSION_PACKAGE = "@narumitw/pi-plan-mode"
_GOAL_LIST_EXTENSION_PACKAGE = "pi-goal-list-loop-audit"


def detect_goal_list_extension(root: Path | None = None) -> bool:
    """True when the ``pi-goal-list-loop-audit`` extension is installed.

    Checks ``<root>/pi-goal-list-loop-audit`` (directory existence). The
    default root mirrors :func:`detect_plan_mode_extension`.
    """
    if root is None:
        agent_dir = os.environ.get("PI_CODING_AGENT_DIR")
        base = (
            Path(agent_dir).expanduser() if agent_dir else Path.home() / ".pi" / "agent"
        )
        root = base / "npm" / "node_modules"
    return (root / _GOAL_LIST_EXTENSION_PACKAGE).is_dir()


def detect_plan_mode_extension(root: Path | None = None) -> bool:
    """True when the ``@narumitw/pi-plan-mode`` extension is installed.

    Checks ``<root>/@narumitw/pi-plan-mode`` (directory existence). The
    default root is the conventional pi extension install path
    ``~/.pi/agent/npm/node_modules``. Injectable ``root`` for tests; no
    config key because the path is a pi-ecosystem convention.
    """
    if root is None:
        agent_dir = os.environ.get("PI_CODING_AGENT_DIR")
        base = (
            Path(agent_dir).expanduser() if agent_dir else Path.home() / ".pi" / "agent"
        )
        root = base / "npm" / "node_modules"
    return (root / _PLAN_MODE_EXTENSION_PACKAGE).is_dir()


def build_runner(config: EngineConfig, config_path: Path) -> Runner:
    extra_args_value = config.get("extra_args")
    if extra_args_value is None:
        extra_args = []
    elif isinstance(extra_args_value, list) and all(
        isinstance(x, str) for x in extra_args_value
    ):
        extra_args = list(extra_args_value)
    else:
        logger.warning(
            "pi.config.invalid",
            error="extra_args must be a list of strings",
            config_path=str(config_path),
        )
        raise ConfigError(
            f"Invalid `pi.extra_args` in {config_path}; expected a list of strings."
        )

    model = config.get("model")
    if model is not None and not isinstance(model, str):
        logger.warning(
            "pi.config.invalid",
            error="model must be a string",
            config_path=str(config_path),
        )
        raise ConfigError(f"Invalid `pi.model` in {config_path}; expected a string.")

    provider = config.get("provider")
    if provider is not None and not isinstance(provider, str):
        logger.warning(
            "pi.config.invalid",
            error="provider must be a string",
            config_path=str(config_path),
        )
        raise ConfigError(f"Invalid `pi.provider` in {config_path}; expected a string.")

    return cast(
        Runner,
        PiRunner(
            extra_args=extra_args,
            model=model,
            provider=provider,
            plan_mode_extension=detect_plan_mode_extension(),
            goal_list_extension=detect_goal_list_extension(),
        ),
    )


BACKEND = EngineBackend(
    id="pi",
    build_runner=build_runner,
    cli_cmd="pi",
    install_cmd="npm install -g @mariozechner/pi-coding-agent",
)
