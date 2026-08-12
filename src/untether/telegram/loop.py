from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import anyio
from anyio.abc import TaskGroup

from ..commands import list_command_ids
from ..config import ConfigError
from ..config_watch import ConfigReload
from ..config_watch import watch_config as watch_config_changes
from ..context import RunContext
from ..directives import DirectiveError
from ..ids import RESERVED_CHAT_COMMANDS
from ..logging import get_logger
from ..model import EngineId, ResumeToken
from ..progress import ProgressTracker
from ..runner_bridge import RunOutcome
from ..runners.run_options import EngineRunOptions
from ..scheduler import ThreadJob, ThreadScheduler
from ..settings import TelegramTransportSettings
from ..transport import MessageRef, RenderedMessage, SendOptions, Transport
from ..transport_runtime import ResolvedMessage
from ..utils.error_display import user_safe_error
from .bridge import (
    CANCEL_CALLBACK_DATA,
    STEER_CALLBACK_DATA,
    TelegramBridgeConfig,
    send_plain,
)
from .chat_prefs import ChatPrefsStore, resolve_prefs_path
from .chat_sessions import ChatSessionStore, resolve_sessions_path
from .client import poll_incoming
from .commands.cancel import (
    handle_callback_cancel,
    handle_callback_steer,
    handle_cancel,
)
from .commands.compact import CompactConfirmRecord, handle_compact_command
from .commands.file_transfer import FILE_PUT_USAGE
from .commands.handlers import (
    dispatch_callback,
    dispatch_command,
    get_reserved_commands,
    handle_agent_command,
    handle_chat_ctx_command,
    handle_chat_new_command,
    handle_ctx_command,
    handle_file_command,
    handle_file_put_default,
    handle_listen_command,
    handle_media_group,
    handle_model_command,
    handle_new_command,
    handle_reasoning_command,
    handle_topic_command,
    parse_callback_data,
    parse_slash_command,
    run_engine,
    save_file_put,
    set_command_menu,
    should_show_resume_line,
)
from .commands.parse import is_cancel_command, parse_dot_typo
from .commands.reply import make_reply
from .context import _merge_topic_context, _usage_ctx_set, _usage_topic
from .engine_defaults import resolve_engine_for_message
from .engine_overrides import merge_overrides
from .files import format_image_prompt_annotation, is_image_document, split_command_args
from .listen_mode import resolve_listen_mode, should_trigger_run
from .prompt_batch import (
    PromptBatchPart,
    PromptBatchSeparator,
    PromptBatchSettings,
    join_prompt_parts,
    should_batch_text,
)
from .topic_state import TopicStateStore, resolve_state_path
from .topics import (
    _maybe_rename_topic,
    _resolve_topics_scope,
    _topic_key,
    _topics_chat_allowed,
    _topics_chat_project,
    _validate_topics_setup,
)
from .types import (
    TelegramCallbackQuery,
    TelegramIncomingMessage,
    TelegramIncomingUpdate,
)
from .voice import transcribe_voice

logger = get_logger(__name__)

__all__ = ["poll_updates", "run_main_loop", "send_with_resume"]

ForwardKey = tuple[int, int, int]
MessageKey = tuple[int, int]
_SEEN_MESSAGES_LIMIT = 2048
_SEEN_UPDATES_LIMIT = 4096

_handle_file_put_default = handle_file_put_default

# #528: AskUserQuestion text-reply echo. Earlier versions hard-sliced at
# [:100] which truncated mid-word with no ellipsis; the agent always
# received the full reply but the user couldn't see it in the chat.
_ANSWERED_ECHO_MAX = 300


def _format_answered_echo(text: str) -> str:
    if len(text) <= _ANSWERED_ECHO_MAX:
        return f"↩️ Answered: {text}"
    return f"↩️ Answered: {text[: _ANSWERED_ECHO_MAX - 1]}…"


def _chat_session_key(
    msg: TelegramIncomingMessage, *, store: ChatSessionStore | None
) -> tuple[int, int | None] | None:
    if store is None or msg.thread_id is not None:
        return None
    if msg.chat_type == "private":
        return (msg.chat_id, None)
    if msg.sender_id is None:
        return None
    return (msg.chat_id, msg.sender_id)


async def _resolve_engine_run_options(
    chat_id: int,
    thread_id: int | None,
    engine: EngineId,
    chat_prefs: ChatPrefsStore | None,
    topic_store: TopicStateStore | None,
) -> EngineRunOptions | None:
    topic_override = None
    if topic_store is not None and thread_id is not None:
        topic_override = await topic_store.get_engine_override(
            chat_id, thread_id, engine
        )
    chat_override = None
    if chat_prefs is not None:
        chat_override = await chat_prefs.get_engine_override(chat_id, engine)
    merged = merge_overrides(topic_override, chat_override)

    # Resolve sticky plan preference: topic > chat > default off.
    sticky_plan = False
    if topic_store is not None and thread_id is not None:
        tp = await topic_store.get_plan_mode(chat_id, thread_id)
        if tp is not None:
            sticky_plan = tp
        elif chat_prefs is not None:
            cp = await chat_prefs.get_plan_mode(chat_id)
            if cp is not None:
                sticky_plan = cp
    elif chat_prefs is not None:
        cp = await chat_prefs.get_plan_mode(chat_id)
        if cp is not None:
            sticky_plan = cp

    # Resolve sticky subagent: chat-scoped only.
    sticky_subagent: str | None = None
    if chat_prefs is not None:
        sticky_subagent = await chat_prefs.get_subagent(chat_id)

    if merged is None and not sticky_plan and sticky_subagent is None:
        return None
    return EngineRunOptions(
        model=merged.model if merged else None,
        reasoning=merged.reasoning if merged else None,
        permission_mode=merged.permission_mode if merged else None,
        ask_questions=merged.ask_questions if merged else None,
        diff_preview=merged.diff_preview if merged else None,
        show_api_cost=merged.show_api_cost if merged else None,
        show_subscription_usage=merged.show_subscription_usage if merged else None,
        show_resume_line=merged.show_resume_line if merged else None,
        budget_enabled=merged.budget_enabled if merged else None,
        budget_auto_cancel=merged.budget_auto_cancel if merged else None,
        loop_enabled=merged.loop_enabled if merged else None,
        plan=sticky_plan,
        subagent=sticky_subagent,
    )


async def _restore_handoff_route(
    store: TopicStateStore | ChatSessionStore,
    key: tuple[int, int | None],
    previous: ResumeToken | None,
    engine: EngineId,
) -> None:
    if isinstance(store, TopicStateStore):
        thread_id = key[1]
        if thread_id is None:
            return
        if previous is None:
            await store.clear_engine_session(key[0], thread_id, engine)
        else:
            await store.set_session_resume(key[0], thread_id, previous)
    elif previous is None:
        await store.clear_engine_session(key[0], key[1], engine)
    else:
        await store.set_session_resume(key[0], key[1], previous)


async def _commit_handoff_routing(
    *,
    topic_store: TopicStateStore | None,
    topic_key: tuple[int, int] | None,
    chat_session_store: ChatSessionStore | None,
    chat_session_key: tuple[int, int | None] | None,
    destination: ResumeToken,
) -> None:
    """Persist a successful destination route, restoring any partial commit."""
    writes: list[
        tuple[
            TopicStateStore | ChatSessionStore,
            tuple[int, int | None],
            ResumeToken | None,
        ]
    ] = []
    if topic_store is not None and topic_key is not None:
        previous = await topic_store.get_session_resume(*topic_key, destination.engine)
        writes.append((topic_store, topic_key, previous))
    if chat_session_store is not None and chat_session_key is not None:
        previous = await chat_session_store.get_session_resume(
            *chat_session_key, destination.engine
        )
        writes.append((chat_session_store, chat_session_key, previous))
    committed: list[
        tuple[
            TopicStateStore | ChatSessionStore,
            tuple[int, int | None],
            ResumeToken | None,
        ]
    ] = []
    try:
        for store, key, previous in writes:
            if isinstance(store, TopicStateStore):
                thread_id = key[1]
                if thread_id is None:
                    continue
                await store.set_session_resume(key[0], thread_id, destination)
            else:
                await store.set_session_resume(key[0], key[1], destination)
            committed.append((store, key, previous))
    except BaseException:
        for store, key, previous in reversed(committed):
            with anyio.CancelScope(shield=True):
                await _restore_handoff_route(store, key, previous, destination.engine)
        raise


def _directive_options(resolved: ResolvedMessage) -> EngineRunOptions | None:
    """Build a one-shot ``EngineRunOptions`` from directive-derived fields.

    Returns ``None`` when no directive is present so callers can avoid
    constructing an empty object. Goal-over-plan precedence is enforced
    at runner consumption time, not here.
    """
    if (
        not resolved.plan
        and resolved.goal is None
        and resolved.skill is None
        and resolved.subagent is None
        and resolved.model is None
    ):
        return None
    return EngineRunOptions(
        plan=resolved.plan,
        goal=resolved.goal,
        skill=resolved.skill,
        subagent=resolved.subagent,
        model=resolved.model,
    )


@runtime_checkable
class _ModelCatalogRuntime(Protocol):
    """Structural subset of TransportRuntime needed for model validation."""

    def list_models(self, engine: EngineId | None) -> tuple[str, ...] | None: ...

    def supports_model_on_resume(self, engine: EngineId | None) -> bool: ...


class _ModelValidationResult:
    """Outcome of validating a one-message model directive before enqueue.

    ``allow`` — pass the model through unchanged.
    ``reject`` — stop with ``message``; create no job.
    ``fallback`` — drop the model override (use engine default); send ``message``.
    """

    __slots__ = ("action", "message", "model")

    def __init__(
        self,
        action: str,
        *,
        message: str | None = None,
        model: str | None = None,
    ) -> None:
        self.action = action
        self.message = message
        self.model = model


def _validate_model_override(
    model: str,
    engine: EngineId,
    *,
    runtime: _ModelCatalogRuntime,
    fallback_enabled: bool,
) -> _ModelValidationResult:
    """Validate a one-message model against the engine's catalog before enqueue.

    Precedence:
    - Catalog hit → allow.
    - Catalog-confirmed miss, fallback off → reject with ``Unknown model``.
    - Catalog-confirmed miss, fallback on → drop override, visible notice.
    - Catalog unavailable (None) → pass through; the engine is authoritative.
    """
    catalog = runtime.list_models(engine)
    if catalog is None:
        return _ModelValidationResult("allow", model=model)
    if model in catalog:
        return _ModelValidationResult("allow", model=model)
    if fallback_enabled:
        return _ModelValidationResult(
            "fallback",
            message=(
                f"Unknown model `{model}` for `{engine}`; using the engine's default."
            ),
        )
    return _ModelValidationResult(
        "reject",
        message=f"Unknown model `{model}` for `{engine}`.",
    )


def _check_resume_model_capability(
    *,
    engine: EngineId,
    runtime: _ModelCatalogRuntime,
    model: str,
) -> str | None:
    """Return a limitation message if the engine can't change model on resume.

    None means the override is allowed. A non-None message means: send the
    message, perform zero runner starts, and create no fresh session.
    """
    if runtime.supports_model_on_resume(engine):
        return None
    return (
        f"`{engine}` does not support changing the model while resuming a session; "
        f"the model override (`{model}`) was not applied."
    )


def _apply_trigger_permission_override(
    run_options: EngineRunOptions | None,
    context: RunContext | None,
    *,
    engine: EngineId | None = None,
) -> EngineRunOptions | None:
    """#330: apply a trigger-level `permission_mode` on top of resolved run_options.

    Dispatchers populate ``RunContext.permission_mode`` from
    ``CronConfig.permission_mode``; this helper overrides the resolved
    per-chat/topic ``EngineRunOptions.permission_mode`` when a trigger
    override is present. Logs once when the override actually changes the
    effective value so staging debug is greppable.
    """
    if context is None or context.permission_mode is None:
        return run_options
    previous_mode = run_options.permission_mode if run_options is not None else None
    if run_options is None:
        new_options = EngineRunOptions(permission_mode=context.permission_mode)
    else:
        from dataclasses import replace

        new_options = replace(run_options, permission_mode=context.permission_mode)
    if previous_mode != context.permission_mode:
        logger.info(
            "trigger.cron.permission_mode_override",
            trigger_source=context.trigger_source,
            chat_permission_mode=previous_mode,
            trigger_permission_mode=context.permission_mode,
            engine=engine,
        )
    return new_options


def _allowed_chat_ids(cfg: TelegramBridgeConfig) -> set[int]:
    allowed = set(cfg.chat_ids or ())
    allowed.add(cfg.chat_id)
    allowed.update(cfg.runtime.project_chat_ids())
    allowed.update(cfg.allowed_user_ids)
    return allowed


async def _send_startup(cfg: TelegramBridgeConfig) -> None:
    from ..markdown import MarkdownParts
    from ..transport import RenderedMessage
    from .render import prepare_telegram

    logger.debug("startup.message", text=cfg.startup_msg)
    parts = MarkdownParts(header=cfg.startup_msg)
    text, entities = prepare_telegram(parts)
    message = RenderedMessage(text=text, extra={"entities": entities})
    sent = await cfg.exec_cfg.transport.send(
        channel_id=cfg.chat_id,
        message=message,
    )
    if sent is not None:
        logger.info("startup.sent", chat_id=cfg.chat_id)


async def _notify_restart_required(cfg: TelegramBridgeConfig, keys: list[str]) -> None:
    """#318 follow-up: broadcast restart-required warning to project chats + admin DMs.

    PR #336 wired the warning to ``cfg.chat_id`` alone; in project-routed
    deployments that value is the placeholder sentinel and every send
    fails with "chat not found". This helper instead targets every active
    project chat plus any ``allowed_user_ids`` admin DM, falling back to
    ``cfg.chat_id`` only when no routed targets exist. Per-chat failures
    are logged and skipped so one bad chat can't mask the warning from
    the rest.
    """
    keys_text = ", ".join(f"`{k}`" for k in keys)
    text = (
        "\N{CLOCKWISE GAPPED CIRCLE ARROW} "
        f"Setting {keys_text} changed — restart required to take effect.\n"
        "Run: `systemctl --user restart untether`"
    )
    targets: set[int] = set()
    targets.update(cfg.runtime.project_chat_ids())
    targets.update(cfg.allowed_user_ids or ())
    if not targets:
        targets.add(cfg.chat_id)
    sent_count = 0
    for chat_id in sorted(targets):
        try:
            sent = await cfg.exec_cfg.transport.send(
                channel_id=chat_id,
                message=RenderedMessage(
                    text=text,
                    extra={"parse_mode": "Markdown"},
                ),
                options=SendOptions(notify=True),
            )
            if sent is not None:
                sent_count += 1
        except Exception as exc:  # noqa: BLE001 — logged then continue
            logger.warning(
                "config.reload.restart_notify.failed",
                chat_id=chat_id,
                error=str(exc),
            )
    logger.info(
        "config.reload.restart_notify.sent",
        keys=keys,
        targets=sorted(targets),
        sent_count=sent_count,
    )


async def _notify_reload_applied(
    cfg: TelegramBridgeConfig,
    *,
    path: Path,
    hot_keys: list[str],
    restart_keys: list[str],
) -> None:
    """#547 axis 2 / #548: broadcast a hot-reload confirmation message so
    agents and users see "did my edit work?" answered in-chat (instead of
    having to switch to ``journalctl``). The headline framing ("No restart
    needed.") flips the trained-in agent reflex to ``systemctl restart``
    after editing config.

    Reuses the same broadcast pattern as ``_notify_restart_required``: send
    to every active project chat + admin DMs, falling back to
    ``cfg.chat_id`` if no routed targets exist. Per-chat failures are
    logged and skipped — one bad chat can't mask the affirmation from
    the rest.
    """
    if not hot_keys and not restart_keys:
        return
    from ..config_reload_notification import format_reload_notification

    text = format_reload_notification(
        path=path, hot_keys=hot_keys, restart_keys=restart_keys
    )
    targets: set[int] = set()
    targets.update(cfg.runtime.project_chat_ids())
    targets.update(cfg.allowed_user_ids or ())
    if not targets:
        targets.add(cfg.chat_id)
    sent_count = 0
    for chat_id in sorted(targets):
        try:
            sent = await cfg.exec_cfg.transport.send(
                channel_id=chat_id,
                message=RenderedMessage(
                    text=text,
                    extra={"parse_mode": "Markdown"},
                ),
                options=SendOptions(notify=False),  # non-disruptive
            )
            if sent is not None:
                sent_count += 1
        except Exception as exc:  # noqa: BLE001 — logged then continue
            logger.warning(
                "config.reload.applied_notify.failed",
                chat_id=chat_id,
                error=str(exc),
            )
    logger.info(
        "config.reload.applied_notify.sent",
        hot_keys=hot_keys,
        restart_keys=restart_keys,
        targets=sorted(targets),
        sent_count=sent_count,
    )


def _dispatch_builtin_command(
    *,
    ctx: TelegramCommandContext,
    command_id: str,
) -> bool:
    cfg = ctx.cfg
    msg = ctx.msg
    args_text = ctx.args_text
    ambient_context = ctx.ambient_context
    topic_store = ctx.topic_store
    chat_prefs = ctx.chat_prefs
    resolved_scope = ctx.resolved_scope
    scope_chat_ids = ctx.scope_chat_ids
    reply = ctx.reply
    task_group = ctx.task_group
    if command_id == "file":
        if not cfg.files.enabled:
            handler = partial(
                reply,
                text="file transfer disabled; enable `[transports.telegram.files]`.",
            )
        else:
            if topic_store is None:
                raise RuntimeError("topic store required")
            handler = partial(
                handle_file_command,
                cfg,
                msg,
                args_text,
                ambient_context,
                topic_store,
            )
        task_group.start_soon(cast(Callable[..., Awaitable[Any]], handler))
        return True
        topic_key = (
            _topic_key(msg, cfg, scope_chat_ids=scope_chat_ids)
            if cfg.topics.enabled and topic_store is not None
            else None
        )
        if topic_key is not None:
            handler = partial(
                handle_ctx_command,
                cfg,
                msg,
                args_text,
                topic_store,
                resolved_scope=resolved_scope,
                scope_chat_ids=scope_chat_ids,
            )
        else:
            handler = partial(
                handle_chat_ctx_command,
                cfg,
                msg,
                args_text,
                chat_prefs,
            )
        task_group.start_soon(handler)
        return True

    if command_id == "new":
        topic_key = (
            _topic_key(msg, cfg, scope_chat_ids=scope_chat_ids)
            if cfg.topics.enabled and topic_store is not None
            else None
        )
        if topic_key is not None and topic_store is not None:
            handler: Callable[..., Awaitable[None]] = partial(
                handle_new_command,
                cfg,
                msg,
                topic_store,
                resolved_scope=resolved_scope,
                scope_chat_ids=scope_chat_ids,
                running_tasks=ctx.running_tasks,
            )
        elif ctx.chat_session_store is not None:
            handler = partial(
                handle_chat_new_command,
                cfg,
                msg,
                ctx.chat_session_store,
                ctx.chat_session_key,
                running_tasks=ctx.running_tasks,
            )
        else:
            # Stateless mode: just cancel running tasks and reply
            async def _stateless_new() -> None:
                from .commands.topics import _cancel_chat_tasks

                cancelled = _cancel_chat_tasks(msg.chat_id, ctx.running_tasks)
                label = "cancelled run" if cancelled else "no stored sessions to clear"
                await reply(text=f"{label} for this chat.")

            handler = _stateless_new
        task_group.start_soon(handler)
        return True

    if cfg.topics.enabled and topic_store is not None:
        if command_id == "topic":
            handler = partial(
                handle_topic_command,
                cfg,
                msg,
                args_text,
                topic_store,
                resolved_scope=resolved_scope,
                scope_chat_ids=scope_chat_ids,
            )
        else:
            handler = None
        if handler is not None:
            task_group.start_soon(handler)
            return True

    if command_id == "model":
        handler = partial(
            handle_model_command,
            cfg,
            msg,
            args_text,
            ambient_context,
            topic_store,
            chat_prefs,
            resolved_scope=resolved_scope,
            scope_chat_ids=scope_chat_ids,
        )
        task_group.start_soon(handler)
        return True

    if command_id == "agent":
        handler = partial(
            handle_agent_command,
            cfg,
            msg,
            args_text,
            ambient_context,
            topic_store,
            chat_prefs,
            resolved_scope=resolved_scope,
            scope_chat_ids=scope_chat_ids,
        )
        task_group.start_soon(handler)
        return True

    if command_id == "reasoning":
        handler = partial(
            handle_reasoning_command,
            cfg,
            msg,
            args_text,
            ambient_context,
            topic_store,
            chat_prefs,
            resolved_scope=resolved_scope,
            scope_chat_ids=scope_chat_ids,
        )
        task_group.start_soon(handler)
        return True

    if command_id in {"listen", "trigger"}:
        # #297: /trigger is a deprecated alias for /listen. The handler
        # prepends a deprecation notice when invoked_as="trigger".
        handler = partial(
            handle_listen_command,
            cfg,
            msg,
            args_text,
            ambient_context,
            topic_store,
            chat_prefs,
            resolved_scope=resolved_scope,
            scope_chat_ids=scope_chat_ids,
            invoked_as=command_id,
        )
        task_group.start_soon(handler)
        return True

    if command_id == "queue":
        from .commands.queue_cmd import handle_queue_command

        task_group.start_soon(
            partial(
                handle_queue_command,
                cfg,
                msg,
                scheduler=ctx.scheduler,
                running_tasks=ctx.running_tasks,
                reply=reply,
            )
        )
        return True

    if command_id in {"plan", "goal", "subagent"}:
        from .commands.meta_cmd import handle_meta_command

        task_group.start_soon(
            partial(
                handle_meta_command,
                cfg,
                msg,
                args_text,
                command_id,
                topic_store,
                chat_prefs,
                resolved_scope=resolved_scope,
                scope_chat_ids=scope_chat_ids,
                reply=reply,
            )
        )
        return True

    return False


async def _drain_backlog(cfg: TelegramBridgeConfig, offset: int | None) -> int | None:
    drained = 0
    while True:
        updates = await cfg.bot.get_updates(
            offset=offset,
            timeout_s=0,
            allowed_updates=["message", "callback_query"],
        )
        if updates is None:
            logger.info("startup.backlog.failed")
            return offset
        logger.debug("startup.backlog.updates", updates=updates)
        if not updates:
            if drained:
                logger.info("startup.backlog.drained", count=drained)
            return offset
        offset = updates[-1].update_id + 1
        drained += len(updates)


async def _cleanup_orphan_progress(cfg: TelegramBridgeConfig) -> None:
    """Edit orphan progress messages from a prior instance to show interrupted."""
    config_path = cfg.runtime.config_path
    if config_path is None:
        return
    from .progress_persistence import (
        clear_all_progress,
        load_active_progress,
        resolve_progress_path,
    )

    progress_path = resolve_progress_path(config_path)
    entries = load_active_progress(progress_path)
    if not entries:
        return
    logger.info("startup.orphan_cleanup", count=len(entries))
    for entry in entries.values():
        chat_id = entry.get("chat_id")
        message_id = entry.get("message_id")
        if chat_id is None or message_id is None:
            continue
        try:
            await cfg.bot.edit_message_text(
                chat_id=int(chat_id),
                message_id=int(message_id),
                text="\u26a0\ufe0f interrupted by restart",
            )
            logger.debug(
                "startup.orphan_cleanup.edited",
                chat_id=chat_id,
                message_id=message_id,
            )
        except Exception:  # noqa: BLE001
            logger.debug(
                "startup.orphan_cleanup.edit_failed",
                chat_id=chat_id,
                message_id=message_id,
                exc_info=True,
            )
    clear_all_progress(progress_path)


def _init_quarantine_store(config_path: Path) -> None:
    """#631 (T6): eagerly initialise the process-wide ``QuarantineStore``
    singleton from the ACTUAL loaded config path, once, before polling
    starts — mirrors the offset-persistence init just above.

    Without this, ``get_quarantine_store()`` lazily resolves the path from
    ``UNTETHER_CONFIG_PATH``/HOME default on first use, which is wrong
    when settings were loaded from an explicit non-env path. Doing it
    eagerly at startup also surfaces a corrupt state file in startup logs
    instead of mid-run.

    ``QuarantineStore.load()`` already survives corrupt JSON internally
    (it logs and falls back to an empty store) — the except below only
    guards against truly unexpected errors. Startup must never fail
    because of this file; the lazy accessor remains the fallback.
    """
    try:
        from ..session_quarantine import (
            QuarantineStore,
            resolve_quarantine_path,
            set_quarantine_store,
        )

        set_quarantine_store(QuarantineStore.load(resolve_quarantine_path(config_path)))
    except Exception:  # noqa: BLE001 — startup must never fail because of
        # this file; the lazy accessor (get_quarantine_store()) remains
        # the fallback.
        logger.warning("quarantine.startup_init_failed", exc_info=True)


async def poll_updates(
    cfg: TelegramBridgeConfig,
    *,
    sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
) -> AsyncIterator[TelegramIncomingUpdate]:
    from .. import sdnotify
    from .offset_persistence import (
        DebouncedOffsetWriter,
        load_last_update_id,
        resolve_offset_path,
    )

    config_path = cfg.runtime.config_path
    offset: int | None = None
    offset_writer: DebouncedOffsetWriter | None = None
    if config_path is not None:
        offset_path = resolve_offset_path(config_path)
        saved = load_last_update_id(offset_path)
        if saved is not None:
            offset = saved + 1
            logger.info(
                "startup.offset.resumed",
                last_update_id=saved,
                path=str(offset_path),
            )
        offset_writer = DebouncedOffsetWriter(offset_path)
        _init_quarantine_store(config_path)

    offset = await _drain_backlog(cfg, offset)
    await _cleanup_orphan_progress(cfg)
    await _send_startup(cfg)

    # Signal systemd that Untether is ready to receive traffic. No-op on
    # non-systemd runs (NOTIFY_SOCKET absent). See #287.
    if sdnotify.notify("READY=1"):
        logger.debug("sdnotify.ready")

    try:
        async for msg in poll_incoming(
            cfg.bot,
            chat_ids=lambda: _allowed_chat_ids(cfg),
            offset=offset,
            sleep=sleep,
            on_offset_advanced=(
                offset_writer.note if offset_writer is not None else None
            ),
        ):
            yield msg
    finally:
        if offset_writer is not None:
            offset_writer.flush()


@dataclass(slots=True)
class _MediaGroupState:
    messages: list[TelegramIncomingMessage]
    token: int = 0


@dataclass(slots=True)
class _PendingPrompt:
    msg: TelegramIncomingMessage
    text: str
    ambient_context: RunContext | None
    chat_project: str | None
    topic_key: tuple[int, int] | None
    chat_session_key: tuple[int, int | None] | None
    reply_ref: MessageRef | None
    reply_id: int | None
    is_voice_transcribed: bool
    forwards: list[tuple[int, str]]
    cancel_scope: anyio.CancelScope | None = None


@dataclass(frozen=True, slots=True)
class TelegramMsgContext:
    chat_id: int
    thread_id: int | None
    reply_id: int | None
    reply_ref: MessageRef | None
    topic_key: tuple[int, int] | None
    chat_session_key: tuple[int, int | None] | None
    stateful_mode: bool
    chat_project: str | None
    ambient_context: RunContext | None


@dataclass(frozen=True, slots=True)
class MessageClassification:
    text: str
    command_id: str | None
    args_text: str
    is_cancel: bool
    is_forward_candidate: bool
    is_media_group_document: bool


@dataclass(frozen=True, slots=True)
class TelegramCommandContext:
    cfg: TelegramBridgeConfig
    msg: TelegramIncomingMessage
    args_text: str
    ambient_context: RunContext | None
    topic_store: TopicStateStore | None
    chat_prefs: ChatPrefsStore | None
    resolved_scope: str | None
    scope_chat_ids: frozenset[int]
    reply: Callable[..., Awaitable[None]]
    task_group: TaskGroup
    running_tasks: RunningTasks | None = None
    chat_session_store: ChatSessionStore | None = None
    chat_session_key: tuple[int, int | None] | None = None
    scheduler: ThreadScheduler | None = None


def _classify_message(
    msg: TelegramIncomingMessage, *, files_enabled: bool
) -> MessageClassification:
    text = msg.text
    command_id, args_text = parse_slash_command(text)
    is_forward_candidate = (
        _is_forwarded(msg.raw)
        and msg.document is None
        and msg.voice is None
        and msg.media_group_id is None
    )
    is_media_group_document = (
        files_enabled and msg.document is not None and msg.media_group_id is not None
    )
    return MessageClassification(
        text=text,
        command_id=command_id,
        args_text=args_text,
        is_cancel=is_cancel_command(text),
        is_forward_candidate=is_forward_candidate,
        is_media_group_document=is_media_group_document,
    )


@dataclass(slots=True)
class TelegramLoopState:
    running_tasks: RunningTasks
    pending_prompts: dict[ForwardKey, _PendingPrompt]
    media_groups: dict[tuple[int, str], _MediaGroupState]
    prompt_batches: dict[PromptBatchKey, PromptBatchState]
    command_ids: set[str]
    reserved_commands: set[str]
    reserved_chat_commands: set[str]
    transport_snapshot: dict[str, object] | None
    topic_store: TopicStateStore | None
    chat_session_store: ChatSessionStore | None
    chat_prefs: ChatPrefsStore | None
    resolved_topics_scope: str | None
    topics_chat_ids: frozenset[int]
    bot_username: str | None
    forward_coalesce_s: float
    media_group_debounce_s: float
    prompt_batch_enabled: bool
    prompt_batch_debounce_s: float
    prompt_batch_max_messages: int
    prompt_batch_max_chars: int
    prompt_batch_separator: PromptBatchSeparator
    transport_id: str | None
    seen_update_ids: set[int]
    seen_update_order: deque[int]
    seen_message_keys: set[MessageKey]
    seen_messages_order: deque[MessageKey]
    pending_confirms: dict[str, CompactConfirmRecord]


if TYPE_CHECKING:
    from ..runner_bridge import RunningTasks
    from ..triggers.manager import TriggerManager


_FORWARD_FIELDS = (
    "forward_origin",
    "forward_from",
    "forward_from_chat",
    "forward_from_message_id",
    "forward_sender_name",
    "forward_signature",
    "forward_date",
    "is_automatic_forward",
)


def _forward_key(msg: TelegramIncomingMessage) -> ForwardKey:
    return (msg.chat_id, msg.thread_id or 0, msg.sender_id or 0)


def _is_forwarded(raw: dict[str, object] | None) -> bool:
    if not isinstance(raw, dict):
        return False
    return any(raw.get(field) is not None for field in _FORWARD_FIELDS)


def _forward_fields_present(raw: dict[str, object] | None) -> list[str]:
    if not isinstance(raw, dict):
        return []
    return [field for field in _FORWARD_FIELDS if raw.get(field) is not None]


def _format_forwarded_prompt(forwarded: list[str], prompt: str) -> str:
    if not forwarded:
        return prompt
    separator = "\n\n"
    forward_block = separator.join(forwarded)
    if prompt.strip():
        return f"{prompt}{separator}{forward_block}"
    return forward_block


@dataclass(frozen=True, slots=True)
class PromptBatchKey:
    chat_id: int
    thread_id: int | None
    sender_id: int
    reply_id: int | None
    topic_key: tuple[int, int] | None
    chat_session_key: tuple[int, int | None] | None


@dataclass(slots=True)
class PromptBatchState:
    pending: _PendingPrompt
    parts: list[PromptBatchPart]
    token: int = 0


class PromptInputBatcher:
    """Group consecutive qualifying Telegram text messages into one prompt.

    Messages join a batch when they share chat, topic/thread, sender, reply
    target, topic, and session scope, arrive inside the quiet window, and pass
    :func:`should_batch_text`. One flush assembles exactly one
    ``_PendingPrompt``; the existing dispatcher then decides what the assembled
    prompt means (directives, resume, queue).

    Debounce uses token invalidation instead of stored cancel scopes: a newer
    schedule for the same key makes older debounce tasks no-ops, so no task
    ever exits a cancel scope entered by another task.
    """

    def __init__(
        self,
        *,
        task_group: TaskGroup,
        debounce_s: float,
        sleep: Callable[[float], Awaitable[None]],
        dispatch: Callable[[_PendingPrompt], Awaitable[None]],
        pending: dict[PromptBatchKey, PromptBatchState],
        max_messages: int,
        max_chars: int,
        separator: PromptBatchSeparator,
    ) -> None:
        self._task_group = task_group
        self._debounce_s = debounce_s
        self._sleep = sleep
        self._dispatch = dispatch
        self._pending = pending
        self._max_messages = max_messages
        self._max_chars = max_chars
        self._separator = separator

    def key_for_message(
        self,
        msg: TelegramIncomingMessage,
        *,
        topic_key: tuple[int, int] | None,
        chat_session_key: tuple[int, int | None] | None,
    ) -> PromptBatchKey | None:
        if msg.sender_id is None:
            return None
        return PromptBatchKey(
            chat_id=msg.chat_id,
            thread_id=msg.thread_id,
            sender_id=msg.sender_id,
            reply_id=msg.reply_to_message_id,
            topic_key=topic_key,
            chat_session_key=chat_session_key,
        )

    def key_for(self, pending: _PendingPrompt) -> PromptBatchKey | None:
        msg = pending.msg
        if msg.sender_id is None:
            return None
        if (
            msg.document is not None
            or msg.voice is not None
            or msg.media_group_id is not None
        ):
            return None
        return self.key_for_message(
            msg,
            topic_key=pending.topic_key,
            chat_session_key=pending.chat_session_key,
        )

    def cancel(self, key: PromptBatchKey | None) -> None:
        if key is None:
            return
        state = self._pending.pop(key, None)
        if state is not None:
            # Invalidate any in-flight debounce task for this state.
            state.token += 1

    def attach_forward(self, msg: TelegramIncomingMessage) -> bool:
        """Attach a forwarded message to a pending batch, if any.

        Forwarded content is kept separate from text chunks: it lands on the
        pending prompt's ``forwards`` list and is joined by the existing
        forward formatting in the dispatcher. Returns ``False`` when no batch
        is pending for this message, so the caller can fall back to
        :class:`ForwardCoalescer`.
        """
        if msg.sender_id is None:
            return False
        text = msg.text
        if not text.strip():
            return False
        for key, state in list(self._pending.items()):
            if (
                key.chat_id == msg.chat_id
                and key.thread_id == msg.thread_id
                and key.sender_id == msg.sender_id
            ):
                state.pending.forwards.append((msg.message_id, text))
                self._reschedule(key, state)
                return True
        return False

    def schedule(self, pending: _PendingPrompt) -> bool:
        key = self.key_for(pending)
        if key is None:
            return False
        text = pending.text
        settings = PromptBatchSettings(
            enabled=self._debounce_s > 0,
            max_messages=self._max_messages,
            max_chars=self._max_chars,
            separator=self._separator,
        )
        if not settings.enabled or not should_batch_text(text, settings=settings):
            return False

        part = PromptBatchPart(message_id=pending.msg.message_id, text=text)
        state = self._pending.get(key)
        if state is None:
            state = PromptBatchState(pending=pending, parts=[part])
            self._pending[key] = state
            self._reschedule(key, state)
            return True

        if self._joined_len([*state.parts, part], self._separator) > self._max_chars:
            # Flush the existing batch first, then start a new batch with the
            # current chunk; the assembled prompt stays within max_chars.
            self._pending.pop(key, None)
            state.token += 1
            self._task_group.start_soon(self._dispatch_state, state)
            new_state = PromptBatchState(pending=pending, parts=[part])
            self._pending[key] = new_state
            self._reschedule(key, new_state)
            return True

        state.parts.append(part)
        if len(state.parts) >= self._max_messages:
            self._task_group.start_soon(self.flush, key)
            return True
        self._reschedule(key, state)
        return True

    @staticmethod
    def _joined_len(parts: list[PromptBatchPart], separator: str) -> int:
        sep_len = 1 if separator == "newline" else 2
        return sum(len(part.text) for part in parts) + sep_len * max(0, len(parts) - 1)

    def _reschedule(self, key: PromptBatchKey, state: PromptBatchState) -> None:
        state.token += 1
        token = state.token
        self._task_group.start_soon(self._debounce_flush, key, state, token)

    async def _debounce_flush(
        self,
        key: PromptBatchKey,
        state: PromptBatchState,
        token: int,
    ) -> None:
        await self._sleep(self._debounce_s)
        current = self._pending.get(key)
        if current is not state or current.token != token:
            return
        await self.flush(key)

    async def flush(self, key: PromptBatchKey) -> None:
        state = self._pending.pop(key, None)
        if state is None:
            return
        await self._dispatch_state(state)

    async def _dispatch_state(self, state: PromptBatchState) -> None:
        first = state.pending
        assembled = join_prompt_parts(state.parts, separator=self._separator)
        await self._dispatch(
            _PendingPrompt(
                msg=first.msg,
                text=assembled,
                ambient_context=first.ambient_context,
                chat_project=first.chat_project,
                topic_key=first.topic_key,
                chat_session_key=first.chat_session_key,
                reply_ref=first.reply_ref,
                reply_id=first.reply_id,
                is_voice_transcribed=first.is_voice_transcribed,
                forwards=first.forwards,
            )
        )


class ForwardCoalescer:
    def __init__(
        self,
        *,
        task_group: TaskGroup,
        debounce_s: float,
        sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
        dispatch: Callable[[_PendingPrompt], Awaitable[None]],
        pending: dict[ForwardKey, _PendingPrompt],
    ) -> None:
        self._task_group = task_group
        self._debounce_s = debounce_s
        self._sleep = sleep
        self._dispatch = dispatch
        self._pending = pending

    def cancel(self, key: ForwardKey) -> None:
        pending = self._pending.pop(key, None)
        if pending is None:
            return
        if pending.cancel_scope is not None:
            pending.cancel_scope.cancel()
        logger.debug(
            "forward.prompt.cancelled",
            chat_id=pending.msg.chat_id,
            thread_id=pending.msg.thread_id,
            sender_id=pending.msg.sender_id,
            message_id=pending.msg.message_id,
            forward_count=len(pending.forwards),
        )

    def schedule(self, pending: _PendingPrompt) -> None:
        if pending.msg.sender_id is None:
            logger.debug(
                "forward.prompt.bypass",
                chat_id=pending.msg.chat_id,
                thread_id=pending.msg.thread_id,
                sender_id=pending.msg.sender_id,
                message_id=pending.msg.message_id,
                reason="missing_sender",
            )
            self._task_group.start_soon(self._dispatch, pending)
            return
        if self._debounce_s <= 0:
            logger.debug(
                "forward.prompt.bypass",
                chat_id=pending.msg.chat_id,
                thread_id=pending.msg.thread_id,
                sender_id=pending.msg.sender_id,
                message_id=pending.msg.message_id,
                reason="disabled",
            )
            self._task_group.start_soon(self._dispatch, pending)
            return
        key = _forward_key(pending.msg)
        existing = self._pending.get(key)
        if existing is not None:
            if existing.cancel_scope is not None:
                existing.cancel_scope.cancel()
            if existing.forwards:
                pending.forwards = list(existing.forwards)
            logger.debug(
                "forward.prompt.replace",
                chat_id=pending.msg.chat_id,
                thread_id=pending.msg.thread_id,
                sender_id=pending.msg.sender_id,
                old_message_id=existing.msg.message_id,
                new_message_id=pending.msg.message_id,
                forward_count=len(pending.forwards),
            )
        self._pending[key] = pending
        logger.debug(
            "forward.prompt.schedule",
            chat_id=pending.msg.chat_id,
            thread_id=pending.msg.thread_id,
            sender_id=pending.msg.sender_id,
            message_id=pending.msg.message_id,
            debounce_s=self._debounce_s,
        )
        self._reschedule(key, pending)

    def attach_forward(self, msg: TelegramIncomingMessage) -> None:
        if msg.sender_id is None:
            logger.debug(
                "forward.message.ignored",
                chat_id=msg.chat_id,
                thread_id=msg.thread_id,
                sender_id=msg.sender_id,
                message_id=msg.message_id,
                reason="missing_sender",
            )
            return
        key = _forward_key(msg)
        pending = self._pending.get(key)
        if pending is None:
            logger.debug(
                "forward.message.ignored",
                chat_id=msg.chat_id,
                thread_id=msg.thread_id,
                sender_id=msg.sender_id,
                message_id=msg.message_id,
                reason="no_pending_prompt",
            )
            return
        text = msg.text
        if not text.strip():
            logger.debug(
                "forward.message.ignored",
                chat_id=msg.chat_id,
                thread_id=msg.thread_id,
                sender_id=msg.sender_id,
                message_id=msg.message_id,
                reason="empty_text",
            )
            return
        pending.forwards.append((msg.message_id, text))
        logger.debug(
            "forward.message.attached",
            chat_id=msg.chat_id,
            thread_id=msg.thread_id,
            sender_id=msg.sender_id,
            message_id=msg.message_id,
            prompt_message_id=pending.msg.message_id,
            forward_count=len(pending.forwards),
            forward_fields=_forward_fields_present(msg.raw),
            forward_date=msg.raw.get("forward_date") if msg.raw else None,
            message_date=msg.raw.get("date") if msg.raw else None,
            text_len=len(text),
        )
        self._reschedule(key, pending)

    def _reschedule(self, key: ForwardKey, pending: _PendingPrompt) -> None:
        if pending.cancel_scope is not None:
            pending.cancel_scope.cancel()
        pending.cancel_scope = None
        self._task_group.start_soon(self._debounce_prompt_run, key, pending)

    async def _debounce_prompt_run(
        self,
        key: ForwardKey,
        pending: _PendingPrompt,
    ) -> None:
        try:
            with anyio.CancelScope() as scope:
                pending.cancel_scope = scope
                await self._sleep(self._debounce_s)
        except anyio.get_cancelled_exc_class():
            return
        if self._pending.get(key) is not pending:
            return
        self._pending.pop(key, None)
        logger.debug(
            "forward.prompt.run",
            chat_id=pending.msg.chat_id,
            thread_id=pending.msg.thread_id,
            sender_id=pending.msg.sender_id,
            message_id=pending.msg.message_id,
            forward_count=len(pending.forwards),
            debounce_s=self._debounce_s,
        )
        await self._dispatch(pending)


@dataclass(frozen=True, slots=True)
class ResumeDecision:
    resume_token: ResumeToken | None
    handled_by_running_task: bool


class ResumeResolver:
    def __init__(
        self,
        *,
        cfg: TelegramBridgeConfig,
        task_group: TaskGroup,
        running_tasks: Mapping[MessageRef, object],
        enqueue_resume: Callable[..., Awaitable[Any]],
        topic_store: TopicStateStore | None,
        chat_session_store: ChatSessionStore | None,
    ) -> None:
        self._cfg = cfg
        self._task_group = task_group
        self._running_tasks = running_tasks
        self._enqueue_resume = enqueue_resume
        self._topic_store = topic_store
        self._chat_session_store = chat_session_store

    async def resolve(
        self,
        *,
        resume_token: ResumeToken | None,
        reply_id: int | None,
        chat_id: int,
        user_msg_id: int,
        thread_id: int | None,
        chat_session_key: tuple[int, int | None] | None,
        topic_key: tuple[int, int] | None,
        engine_for_session: EngineId,
        prompt_text: str,
    ) -> ResumeDecision:
        if resume_token is not None:
            return ResumeDecision(
                resume_token=resume_token, handled_by_running_task=False
            )
        if reply_id is not None:
            running_task = self._running_tasks.get(
                MessageRef(channel_id=chat_id, message_id=reply_id)
            )
            if running_task is not None:
                self._task_group.start_soon(
                    send_with_resume,
                    self._cfg,
                    self._enqueue_resume,
                    running_task,
                    chat_id,
                    user_msg_id,
                    thread_id,
                    chat_session_key,
                    prompt_text,
                )
                return ResumeDecision(resume_token=None, handled_by_running_task=True)
        if self._topic_store is not None and topic_key is not None:
            stored = await self._topic_store.get_session_resume(
                topic_key[0],
                topic_key[1],
                engine_for_session,
            )
            if stored is not None:
                resume_token = stored
        if (
            resume_token is None
            and self._chat_session_store is not None
            and chat_session_key is not None
        ):
            stored = await self._chat_session_store.get_session_resume(
                chat_session_key[0],
                chat_session_key[1],
                engine_for_session,
            )
            if stored is not None:
                resume_token = stored
        return ResumeDecision(resume_token=resume_token, handled_by_running_task=False)


class MediaGroupBuffer:
    def __init__(
        self,
        *,
        task_group: TaskGroup,
        debounce_s: float,
        sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
        cfg: TelegramBridgeConfig,
        chat_prefs: ChatPrefsStore | None,
        topic_store: TopicStateStore | None,
        bot_username: str | None,
        command_ids: Callable[[], set[str]],
        reserved_chat_commands: set[str],
        groups: dict[tuple[int, str], _MediaGroupState],
        run_prompt_from_upload: Callable[
            [TelegramIncomingMessage, str, ResolvedMessage], Awaitable[None]
        ],
        resolve_prompt_message: Callable[
            [TelegramIncomingMessage, str, RunContext | None],
            Awaitable[ResolvedMessage | None],
        ],
    ) -> None:
        self._task_group = task_group
        self._debounce_s = debounce_s
        self._sleep = sleep
        self._cfg = cfg
        self._chat_prefs = chat_prefs
        self._topic_store = topic_store
        self._bot_username = bot_username
        self._command_ids = command_ids
        self._reserved_chat_commands = reserved_chat_commands
        self._groups = groups
        self._run_prompt_from_upload = run_prompt_from_upload
        self._resolve_prompt_message = resolve_prompt_message

    def add(self, msg: TelegramIncomingMessage) -> None:
        if msg.media_group_id is None:
            return
        key = (msg.chat_id, msg.media_group_id)
        state = self._groups.get(key)
        if state is None:
            state = _MediaGroupState(messages=[])
            self._groups[key] = state
            self._task_group.start_soon(self._flush_media_group, key)
        state.messages.append(msg)
        state.token += 1

    async def _flush_media_group(self, key: tuple[int, str]) -> None:
        while True:
            state = self._groups.get(key)
            if state is None:
                return
            token = state.token
            await self._sleep(self._debounce_s)
            state = self._groups.get(key)
            if state is None:
                return
            if state.token != token:
                continue
            messages = list(state.messages)
            del self._groups[key]
            if not messages:
                return
            listen_mode = await resolve_listen_mode(
                chat_id=messages[0].chat_id,
                thread_id=messages[0].thread_id,
                chat_prefs=self._chat_prefs,
                topic_store=self._topic_store,
            )
            command_ids = self._command_ids()
            if listen_mode == "mentions" and not any(
                should_trigger_run(
                    msg,
                    bot_username=self._bot_username,
                    runtime=self._cfg.runtime,
                    command_ids=command_ids,
                    reserved_chat_commands=self._reserved_chat_commands,
                )
                for msg in messages
            ):
                return
            try:
                await handle_media_group(
                    self._cfg,
                    messages,
                    self._topic_store,
                    self._run_prompt_from_upload,
                    self._resolve_prompt_message,
                    chat_prefs=self._chat_prefs,
                )
                logger.debug(
                    "media_group.flush.ok",
                    chat_id=key[0],
                    media_group_id=key[1],
                    message_count=len(messages),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "media_group.flush.failed",
                    error=str(exc),
                    error_type=exc.__class__.__name__,
                    chat_id=key[0],
                    media_group_id=key[1],
                    message_count=len(messages),
                )
                try:
                    reply = make_reply(self._cfg, messages[0])
                    await reply(
                        text="Couldn't process that upload group — please try again."
                    )
                except Exception:  # noqa: BLE001
                    logger.debug("media_group.flush.notify_failed")
            return


def _diff_keys(old: dict[str, object], new: dict[str, object]) -> list[str]:
    keys = set(old) | set(new)
    return sorted(key for key in keys if old.get(key) != new.get(key))


async def _wait_for_resume(running_task) -> ResumeToken | None:
    if running_task.resume is not None:
        return running_task.resume
    resume: ResumeToken | None = None

    async with anyio.create_task_group() as tg:

        async def wait_resume() -> None:
            nonlocal resume
            await running_task.resume_ready.wait()
            resume = running_task.resume
            tg.cancel_scope.cancel()

        async def wait_done() -> None:
            await running_task.done.wait()
            tg.cancel_scope.cancel()

        tg.start_soon(wait_resume)
        tg.start_soon(wait_done)

    return resume


def _queued_wait_note(resume_token: ResumeToken) -> str | None:
    """Explain WHY a queued follow-up is waiting, when knowable (#654).

    A follow-up that queues behind a Claude session lingering post-result
    (finishing background work under the liveness-aware ceiling, #646/#647)
    previously sat on a bare "queued" progress message for the whole hold —
    observed at 5m36s, permitted up to 30 min — with nothing telling the
    user why, or that /cancel was available. Returns ``None`` for non-Claude
    engines, unknown sessions, and normal mid-run queues (where the active
    progress message above already explains itself).
    """
    if resume_token.engine != "claude":
        return None
    try:
        from ..runners.claude import session_linger_info

        info = session_linger_info(resume_token.value)
    except Exception:  # noqa: BLE001 — the note is best-effort decoration;
        # a registry hiccup must never break the queued send itself.
        logger.debug("queued_note.linger_info_failed", exc_info=True)
        return None
    if info is None:
        return None
    post_result, bg_count = info
    if not post_result:
        return None
    if bg_count > 0:
        plural = "s" if bg_count != 1 else ""
        return (
            f"⏳ Queued behind the previous run's {bg_count} background "
            f"task{plural} — starts when they finish, with context carried "
            f"over. /cancel to drop it."
        )
    return (
        "⏳ Queued — the previous run is still finishing up. Starts "
        "when it completes, with context carried over. /cancel to drop it."
    )


async def _send_queued_progress(
    cfg: TelegramBridgeConfig,
    *,
    chat_id: int,
    user_msg_id: int,
    thread_id: int | None,
    resume_token: ResumeToken,
    context: RunContext | None = None,
    run_options: EngineRunOptions | None = None,
    steerable: bool = False,
) -> MessageRef | None:
    tracker = ProgressTracker(engine=resume_token.engine)
    tracker.set_resume(resume_token)
    context_line = cfg.runtime.format_context_line(
        context,
        plan=run_options.plan if run_options is not None else False,
        goal=run_options.goal if run_options is not None else None,
    )
    state = tracker.snapshot(context_line=context_line)
    message = cfg.exec_cfg.presenter.render_progress(
        state,
        elapsed_s=0.0,
        label="queued",
        steerable=steerable,
    )
    # #654: when the queue reason is knowable (Claude session lingering
    # post-result over background work), say so instead of a bare "queued".
    note = _queued_wait_note(resume_token)
    if note is not None:
        message = RenderedMessage(
            text=f"{message.text}\n\n{note}",
            extra=message.extra,
        )
    reply_ref = MessageRef(
        channel_id=chat_id,
        message_id=user_msg_id,
        thread_id=thread_id,
    )
    return await cfg.exec_cfg.transport.send(
        channel_id=chat_id,
        message=message,
        options=SendOptions(reply_to=reply_ref, notify=False, thread_id=thread_id),
    )


def _thread_turn_steerable(
    running_tasks: Mapping[MessageRef, object],
    resume_token: ResumeToken,
) -> bool:
    for task in running_tasks.values():
        control = getattr(task, "control", None)
        resume = getattr(task, "resume", None)
        if control is not None and resume == resume_token:
            return True
    return False


async def send_with_resume(
    cfg: TelegramBridgeConfig,
    enqueue: Callable[
        [
            int,
            int,
            str,
            ResumeToken,
            RunContext | None,
            int | None,
            tuple[int, int | None] | None,
            MessageRef | None,
        ],
        Awaitable[None],
    ],
    running_task,
    chat_id: int,
    user_msg_id: int,
    thread_id: int | None,
    session_key: tuple[int, int | None] | None,
    text: str,
) -> None:
    reply = partial(
        send_plain,
        cfg.exec_cfg.transport,
        chat_id=chat_id,
        user_msg_id=user_msg_id,
        thread_id=thread_id,
    )
    resume = await _wait_for_resume(running_task)
    if resume is None:
        await reply(
            text="resume token not ready yet; try replying to the final message.",
            notify=False,
        )
        return
    progress_ref = await _send_queued_progress(
        cfg,
        chat_id=chat_id,
        user_msg_id=user_msg_id,
        thread_id=thread_id,
        resume_token=resume,
        context=running_task.context,
    )
    await enqueue(
        chat_id,
        user_msg_id,
        text,
        resume,
        running_task.context,
        thread_id,
        session_key,
        progress_ref,
    )


async def _notify_drain_start(
    transport: Transport,
    running_tasks: Mapping[MessageRef, object],
) -> None:
    msg = RenderedMessage(
        text="\U0001f504 Restarting \N{EM DASH} waiting for your run to finish\N{HORIZONTAL ELLIPSIS}",
        extra={},
    )
    notified: set[int | str] = set()
    for ref in list(running_tasks):
        if ref.channel_id not in notified:
            notified.add(ref.channel_id)
            try:
                await transport.send(channel_id=ref.channel_id, message=msg)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                logger.debug("shutdown.drain_notify_failed", channel_id=ref.channel_id)


async def _notify_drain_timeout(
    transport: Transport,
    running_tasks: Mapping[MessageRef, object],
    remaining: int,
) -> None:
    from ..transport import RenderedMessage

    hint = (
        "Untether was restarted. Your session is saved"
        " \N{EM DASH} resume by sending a new message"
        " or starting /claude."
    )
    msg = RenderedMessage(
        text=(
            f"\N{WARNING SIGN} Restart timed out \N{EM DASH}"
            f" {remaining} run(s) interrupted."
            f"\n\n\N{ELECTRIC LIGHT BULB} {hint}"
        ),
        extra={},
    )
    notified: set[int | str] = set()
    for ref in list(running_tasks):
        if ref.channel_id not in notified:
            notified.add(ref.channel_id)
            try:
                await transport.send(channel_id=ref.channel_id, message=msg)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                logger.debug(
                    "shutdown.timeout_notify_failed", channel_id=ref.channel_id
                )


async def run_main_loop(
    cfg: TelegramBridgeConfig,
    poller: Callable[
        [TelegramBridgeConfig], AsyncIterator[TelegramIncomingUpdate]
    ] = poll_updates,
    *,
    watch_config: bool | None = None,
    default_engine_override: str | None = None,
    transport_id: str | None = None,
    transport_config: TelegramTransportSettings | None = None,
    sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
) -> None:
    state = TelegramLoopState(
        running_tasks={},
        pending_prompts={},
        media_groups={},
        prompt_batches={},
        command_ids={
            command_id.lower()
            for command_id in list_command_ids(allowlist=cfg.runtime.allowlist)
        },
        reserved_commands=get_reserved_commands(cfg.runtime),
        reserved_chat_commands=set(RESERVED_CHAT_COMMANDS),
        transport_snapshot=(
            transport_config.model_dump() if transport_config is not None else None
        ),
        topic_store=None,
        chat_session_store=None,
        chat_prefs=None,
        resolved_topics_scope=None,
        topics_chat_ids=frozenset(),
        bot_username=None,
        forward_coalesce_s=max(0.0, float(cfg.forward_coalesce_s)),
        media_group_debounce_s=max(0.0, float(cfg.media_group_debounce_s)),
        prompt_batch_enabled=bool(cfg.prompt_batch_enabled),
        prompt_batch_debounce_s=max(0.0, float(cfg.prompt_batch_debounce_s)),
        prompt_batch_max_messages=max(2, int(cfg.prompt_batch_max_messages)),
        prompt_batch_max_chars=max(4096, int(cfg.prompt_batch_max_chars)),
        prompt_batch_separator=cfg.prompt_batch_separator,
        transport_id=transport_id,
        seen_update_ids=set(),
        seen_update_order=deque(),
        seen_message_keys=set(),
        seen_messages_order=deque(),
        pending_confirms={},
    )

    def refresh_topics_scope() -> None:
        if cfg.topics.enabled:
            (
                state.resolved_topics_scope,
                state.topics_chat_ids,
            ) = _resolve_topics_scope(cfg)
        else:
            state.resolved_topics_scope = None
            state.topics_chat_ids = frozenset()

    def refresh_commands() -> None:
        allowlist = cfg.runtime.allowlist
        state.command_ids = {
            command_id.lower() for command_id in list_command_ids(allowlist=allowlist)
        }
        state.reserved_commands = get_reserved_commands(cfg.runtime)

    import signal as _signal

    from ..shutdown import (
        get_shutdown_origin_chat_id,
        is_shutting_down,
        request_shutdown,
        reset_shutdown,
        select_drain_timeout,
    )

    _prev_sigterm = _signal.getsignal(_signal.SIGTERM)
    _prev_sigint = _signal.getsignal(_signal.SIGINT)

    try:
        config_path = cfg.runtime.config_path
        if config_path is not None:
            from ..runner_bridge import set_progress_persistence_path
            from .progress_persistence import resolve_progress_path

            set_progress_persistence_path(resolve_progress_path(config_path))

            state.chat_prefs = ChatPrefsStore(resolve_prefs_path(config_path))
            logger.info(
                "chat_prefs.enabled",
                state_path=str(resolve_prefs_path(config_path)),
            )
            from ..session_stats import init_stats
            from ..triggers.history import init_history

            init_stats(config_path)
            init_history(config_path)
        if cfg.session_mode == "chat":
            if config_path is None:
                raise ConfigError(
                    "session_mode=chat but config path is not set; cannot locate state file."
                )
            state.chat_session_store = ChatSessionStore(
                resolve_sessions_path(config_path)
            )
            cleared = await state.chat_session_store.sync_startup_cwd(Path.cwd())
            if cleared:
                logger.info(
                    "chat_sessions.cleared",
                    reason="startup_cwd_changed",
                    cwd=str(Path.cwd()),
                    state_path=str(resolve_sessions_path(config_path)),
                )
            logger.info(
                "chat_sessions.enabled",
                state_path=str(resolve_sessions_path(config_path)),
            )
        if cfg.topics.enabled:
            if config_path is None:
                raise ConfigError(
                    "topics enabled but config path is not set; cannot locate state file."
                )
            state.topic_store = TopicStateStore(resolve_state_path(config_path))
            await _validate_topics_setup(cfg)
            refresh_topics_scope()
            logger.info(
                "topics.enabled",
                scope=cfg.topics.scope,
                resolved_scope=state.resolved_topics_scope,
                state_path=str(resolve_state_path(config_path)),
            )
        await set_command_menu(cfg)
        try:
            me = await cfg.bot.get_me()
        except Exception as exc:  # noqa: BLE001
            logger.info(
                "listen_mode.bot_username.failed",
                error=str(exc),
                error_type=exc.__class__.__name__,
            )
            me = None
        if me is not None and me.username:
            state.bot_username = me.username.lower()
        else:
            logger.info("listen_mode.bot_username.unavailable")
        # Install graceful shutdown signal handlers

        def _shutdown_handler(signum: int, frame: object) -> None:
            request_shutdown()

        _signal.signal(_signal.SIGTERM, _shutdown_handler)
        _signal.signal(_signal.SIGINT, _shutdown_handler)
        logger.info("signal.handler.installed", signals=["SIGTERM", "SIGINT"])

        # Reset uptime counter so /ping reports time since this start, not
        # since the module was first imported (#234).
        from .commands.ping import reset_uptime

        reset_uptime()

        async with anyio.create_task_group() as tg:
            poller_fn: Callable[
                [TelegramBridgeConfig], AsyncIterator[TelegramIncomingUpdate]
            ]
            if poller is poll_updates:
                poller_fn = cast(
                    Callable[
                        [TelegramBridgeConfig], AsyncIterator[TelegramIncomingUpdate]
                    ],
                    partial(poll_updates, sleep=sleep),
                )
            else:
                poller_fn = poller
            config_path = cfg.runtime.config_path
            watch_enabled = bool(watch_config) and config_path is not None

            async def handle_reload(reload: ConfigReload) -> None:
                refresh_commands()
                refresh_topics_scope()
                await set_command_menu(cfg)
                # #547 axis 2 / #548: accumulate the keys that actually
                # changed in this reload so the broadcast at the end of
                # handle_reload can tell the user (and any agent reading
                # in next-turn context) whether a restart is required.
                _reload_hot_keys: list[str] = []
                _reload_restart_keys: list[str] = []
                if state.transport_snapshot is not None:
                    new_snapshot = reload.settings.transports.telegram.model_dump()
                    changed = _diff_keys(state.transport_snapshot, new_snapshot)
                    if changed:
                        # rc4 (#286): unfrozen TelegramBridgeConfig allows most
                        # settings to hot-reload. Only a handful still require a
                        # restart — everything else is applied via update_from().
                        # #318: authoritative set lives on the settings model
                        # so /config, docs, and this reload path agree.
                        restart_only = TelegramTransportSettings.RESTART_REQUIRED_FIELDS
                        restart_keys = [k for k in changed if k in restart_only]
                        hot_keys = [k for k in changed if k not in restart_only]
                        _reload_hot_keys.extend(hot_keys)
                        _reload_restart_keys.extend(restart_keys)
                        if restart_keys:
                            logger.warning(
                                "config.reload.transport_config_changed",
                                transport="telegram",
                                keys=restart_keys,
                                restart_required=True,
                            )
                            # #318 (follow-up): PR #336 sent to cfg.chat_id,
                            # but in project-routed deployments that is the
                            # placeholder sentinel and every send fails with
                            # "chat not found". Broadcast to every project
                            # chat plus admin DMs so the warning actually
                            # reaches whoever's driving the bot. Per-chat
                            # failures are logged and skipped.
                            await _notify_restart_required(cfg, restart_keys)
                            state.forward_coalesce_s = max(
                                0.0, float(cfg.forward_coalesce_s)
                            )
                            state.media_group_debounce_s = max(
                                0.0, float(cfg.media_group_debounce_s)
                            )
                            state.prompt_batch_enabled = bool(cfg.prompt_batch_enabled)
                            state.prompt_batch_debounce_s = max(
                                0.0, float(cfg.prompt_batch_debounce_s)
                            )
                            state.prompt_batch_max_messages = max(
                                2, int(cfg.prompt_batch_max_messages)
                            )
                            state.prompt_batch_max_chars = max(
                                4096, int(cfg.prompt_batch_max_chars)
                            )
                            state.prompt_batch_separator = cfg.prompt_batch_separator
                            logger.info(
                                "config.reload.transport_config_hot_reloaded",
                                transport="telegram",
                                keys=hot_keys,
                            )
                        if hot_keys:
                            cfg.update_from(reload.settings.transports.telegram)
                        state.transport_snapshot = new_snapshot
                if (
                    state.transport_id is not None
                    and reload.settings.transport != state.transport_id
                ):
                    logger.warning(
                        "config.reload.transport_changed",
                        old=state.transport_id,
                        new=reload.settings.transport,
                        restart_required=True,
                    )
                    state.transport_id = reload.settings.transport

                # #547 axis 2 / #548: broadcast the affirmative
                # "Hot-reloaded — No restart needed." (or, if any
                # restart-only key was edited, the matching
                # "Restart required" / "Partial reload" message). The
                # headline framing flips the trained-in agent reflex to
                # ``systemctl restart`` after editing config; agents read
                # this message in next-turn context and adapt.
                if _reload_hot_keys or _reload_restart_keys:
                    try:
                        await _notify_reload_applied(
                            cfg,
                            path=reload.config_path,
                            hot_keys=_reload_hot_keys,
                            restart_keys=_reload_restart_keys,
                        )
                    except Exception:  # noqa: BLE001 — never break reload
                        logger.warning(
                            "config.reload.applied_notify.crashed", exc_info=True
                        )

                # --- Hot-reload trigger configuration ---
                if trigger_manager is not None:
                    try:
                        from ..config import read_config
                        from ..triggers.settings import (
                            TriggersSettings,
                            parse_trigger_config,
                        )

                        raw_toml = read_config(reload.config_path)
                        raw_triggers = raw_toml.get("triggers")
                        if isinstance(raw_triggers, dict) and raw_triggers.get(
                            "enabled"
                        ):
                            new_settings = parse_trigger_config(raw_triggers)
                            trigger_manager.update(new_settings)
                        else:
                            # Triggers disabled or removed — clear all.
                            trigger_manager.update(TriggersSettings())
                    except (ValueError, TypeError, OSError) as exc:
                        logger.warning(
                            "config.reload.triggers_failed",
                            error=str(exc),
                        )

            if watch_enabled and config_path is not None:

                async def run_config_watch() -> None:
                    await watch_config_changes(
                        config_path=config_path,
                        runtime=cfg.runtime,
                        default_engine_override=default_engine_override,
                        on_reload=handle_reload,
                    )

                tg.start_soon(run_config_watch)

            # Graceful drain-then-exit task
            async def _drain_and_exit() -> None:
                # Poll the threading.Event since signal handlers can't use anyio
                while not is_shutting_down():
                    await sleep(0.5)

                # Signal systemd that we've entered drain (Deactivating state).
                from .. import sdnotify

                if sdnotify.notify("STOPPING=1"):
                    logger.debug("sdnotify.stopping")

                active = len(state.running_tasks)
                pending_at = at_scheduler.active_count()
                # #289: include loop fires in the shutdown summary so ops
                # can see how many were pending at drain time.  Pending
                # loops are persisted to disk; the task-group cancel below
                # cancels their in-flight `_arm_timer` sleeps cleanly.
                from .. import loop_scheduler

                pending_loops = loop_scheduler.active_count()
                logger.info(
                    "shutdown.draining",
                    active_runs=active,
                    pending_at=pending_at,
                    pending_loops=pending_loops,
                )

                if active > 0:
                    # #559: when the sole active run is (almost certainly) the
                    # session that triggered the restart — the self-restart
                    # deadlock from #547 — the full 120s drain is dead time the
                    # lone run can never satisfy. Use a short drain instead so we
                    # reach the clean-exit + outbox-flush path promptly.
                    origin_chat_id = get_shutdown_origin_chat_id()
                    self_restart = active == 1
                    drain_timeout = select_drain_timeout(active)
                    if self_restart:
                        sole_chat = next(
                            (ref.channel_id for ref in state.running_tasks), None
                        )
                        logger.info(
                            "shutdown.drain.self_restart",
                            drain_timeout_s=drain_timeout,
                            sole_chat_id=sole_chat,
                            origin_chat_id=origin_chat_id,
                            origin_matches=origin_chat_id is not None
                            and origin_chat_id == sole_chat,
                        )

                    await _notify_drain_start(
                        cfg.exec_cfg.transport, state.running_tasks
                    )

                    # Wait for all runs to complete (up to drain timeout).
                    # Pending /at delays that have not yet fired are cancelled
                    # via the task-group cancel below; no need to wait on them.
                    _drain_tick = 0
                    with anyio.move_on_after(drain_timeout):
                        while state.running_tasks:
                            await sleep(1.0)
                            _drain_tick += 1
                            if _drain_tick % 10 == 0:
                                logger.info(
                                    "shutdown.drain.progress",
                                    remaining=len(state.running_tasks),
                                )

                    remaining = len(state.running_tasks)
                    if remaining > 0:
                        logger.warning(
                            "shutdown.drain_timeout",
                            remaining=remaining,
                            timeout_s=drain_timeout,
                        )
                        await _notify_drain_timeout(
                            cfg.exec_cfg.transport,
                            state.running_tasks,
                            remaining,
                        )

                tg.cancel_scope.cancel()

            tg.start_soon(_drain_and_exit)

            def wrap_on_thread_known(
                base_cb: Callable[[ResumeToken, anyio.Event], Awaitable[None]] | None,
                topic_key: tuple[int, int] | None,
                chat_session_key: tuple[int, int | None] | None,
            ) -> Callable[[ResumeToken, anyio.Event], Awaitable[None]] | None:
                if base_cb is None and topic_key is None and chat_session_key is None:
                    return None

                async def _wrapped(token: ResumeToken, done: anyio.Event) -> None:
                    if base_cb is not None:
                        await base_cb(token, done)
                    if state.topic_store is not None and topic_key is not None:
                        await state.topic_store.set_session_resume(
                            topic_key[0], topic_key[1], token
                        )
                    if (
                        state.chat_session_store is not None
                        and chat_session_key is not None
                    ):
                        await state.chat_session_store.set_session_resume(
                            chat_session_key[0], chat_session_key[1], token
                        )

                return _wrapped

            def wrap_on_resume_failed(
                topic_key: tuple[int, int] | None,
                chat_session_key: tuple[int, int | None] | None,
            ) -> Callable[[ResumeToken], Awaitable[None]] | None:
                if topic_key is None and chat_session_key is None:
                    return None

                async def _wrapped(token: ResumeToken) -> None:
                    if state.topic_store is not None and topic_key is not None:
                        await state.topic_store.clear_engine_session(
                            topic_key[0], topic_key[1], token.engine
                        )
                    if (
                        state.chat_session_store is not None
                        and chat_session_key is not None
                    ):
                        await state.chat_session_store.clear_engine_session(
                            chat_session_key[0], chat_session_key[1], token.engine
                        )

                return _wrapped

            async def run_job(
                chat_id: int,
                user_msg_id: int,
                text: str,
                resume_token: ResumeToken | None,
                context: RunContext | None,
                thread_id: int | None = None,
                chat_session_key: tuple[int, int | None] | None = None,
                reply_ref: MessageRef | None = None,
                on_thread_known: Callable[[ResumeToken, anyio.Event], Awaitable[None]]
                | None = None,
                engine_override: EngineId | None = None,
                progress_ref: MessageRef | None = None,
                directive_options: EngineRunOptions | None = None,
                persist_sessions: bool = True,
            ) -> RunOutcome | None:
                topic_key = (
                    (chat_id, thread_id)
                    if state.topic_store is not None
                    and thread_id is not None
                    and _topics_chat_allowed(
                        cfg, chat_id, scope_chat_ids=state.topics_chat_ids
                    )
                    else None
                )
                stateful_mode = topic_key is not None or chat_session_key is not None
                show_resume_line = should_show_resume_line(
                    show_resume_line=cfg.show_resume_line,
                    stateful_mode=stateful_mode,
                    context=context,
                )
                engine_for_overrides = (
                    resume_token.engine
                    if resume_token is not None
                    else engine_override
                    if engine_override is not None
                    else cfg.runtime.resolve_engine(
                        engine_override=None,
                        context=context,
                    )
                )
                overrides_thread_id = topic_key[1] if topic_key is not None else None
                run_options = await _resolve_engine_run_options(
                    chat_id,
                    overrides_thread_id,
                    engine_for_overrides,
                    chat_prefs=state.chat_prefs,
                    topic_store=state.topic_store,
                )
                # #330: cron-level permission_mode override wins over the
                # resolved chat/topic preference. Dispatchers populate
                # RunContext.permission_mode from CronConfig.permission_mode;
                # here we apply it to the per-run EngineRunOptions so the
                # runner's _effective_permission_mode() picks it up.
                run_options = _apply_trigger_permission_override(
                    run_options, context, engine=engine_for_overrides
                )
                # Directive-derived options override the resolved chat/topic
                # preferences for this run. Merge by field so one object can
                # override only the fields it sets (goal>plan precedence).
                if directive_options is not None:
                    from dataclasses import replace as _replace_options

                    base = (
                        run_options if run_options is not None else EngineRunOptions()
                    )
                    run_options = _replace_options(
                        base,
                        plan=directive_options.plan or base.plan,
                        goal=(
                            directive_options.goal
                            if directive_options.goal is not None
                            else base.goal
                        ),
                        skill=(
                            directive_options.skill
                            if directive_options.skill is not None
                            else base.skill
                        ),
                        subagent=(
                            directive_options.subagent
                            if directive_options.subagent is not None
                            else base.subagent
                        ),
                        model=(
                            directive_options.model
                            if directive_options.model is not None
                            else base.model
                        ),
                    )
                return await run_engine(
                    exec_cfg=cfg.exec_cfg,
                    runtime=cfg.runtime,
                    running_tasks=state.running_tasks,
                    chat_id=chat_id,
                    user_msg_id=user_msg_id,
                    text=text,
                    resume_token=resume_token,
                    context=context,
                    reply_ref=reply_ref,
                    on_thread_known=(
                        wrap_on_thread_known(
                            on_thread_known, topic_key, chat_session_key
                        )
                        if persist_sessions
                        else on_thread_known
                    ),
                    on_resume_failed=(
                        wrap_on_resume_failed(topic_key, chat_session_key)
                        if persist_sessions
                        else None
                    ),
                    engine_override=engine_override,
                    show_resume_line=show_resume_line,
                    thread_id=thread_id,
                    progress_ref=progress_ref,
                    run_options=run_options,
                )

            async def _edit_operation_card(job: ThreadJob, text: str) -> None:
                if job.progress_ref is None:
                    return
                await cfg.exec_cfg.transport.edit(
                    ref=job.progress_ref,
                    message=RenderedMessage(
                        text=text,
                        extra={"reply_markup": {"inline_keyboard": []}},
                    ),
                )

            async def run_compact_job(job: ThreadJob) -> None:
                """Run native compaction while retaining one operation card."""
                from ..compact import get_compact_support
                from ..model import CompletedEvent

                await _edit_operation_card(job, "running summary…")
                entry = cfg.runtime.resolve_runner(
                    resume_token=job.resume_token,
                    engine_override=job.resume_token.engine,
                )
                final_event: CompletedEvent | None = None
                async for event in entry.runner.compact(
                    job.resume_token, job.compact_instructions
                ):
                    if isinstance(event, CompletedEvent):
                        final_event = event
                if final_event is None or not final_event.ok:
                    raise RuntimeError(
                        final_event.error
                        if final_event is not None
                        else "compaction did not complete"
                    )
                support = get_compact_support(entry.runner)
                status = (
                    "completed — compaction finished."
                    if support.true_compaction
                    else "completed — handoff summary finished."
                )
                await _edit_operation_card(job, status)

            async def run_handoff_job(job: ThreadJob) -> None:
                """Create a source summary then seed a new destination session."""
                from ..compact import handoff_seed_prompt
                from ..markdown import MarkdownParts
                from ..model import CompletedEvent
                from .render import MAX_BODY_CHARS, prepare_telegram_multi

                chat_id = cast(int, job.chat_id)
                user_msg_id = cast(int, job.user_msg_id)
                thread_id = cast(int | None, job.thread_id)
                await _edit_operation_card(job, "running summary…")
                entry = cfg.runtime.resolve_runner(
                    resume_token=job.resume_token,
                    engine_override=job.resume_token.engine,
                )
                final_event: CompletedEvent | None = None
                async for event in entry.runner.compact(
                    job.resume_token, job.compact_instructions
                ):
                    if isinstance(event, CompletedEvent):
                        final_event = event
                if final_event is None or not final_event.ok:
                    raise RuntimeError(
                        final_event.error
                        if final_event is not None
                        else "summary failed"
                    )
                summary = (final_event.answer or "").strip()
                if not summary:
                    raise RuntimeError("summary was empty")

                target_engine = job.handoff_target or job.resume_token.engine
                await _edit_operation_card(
                    job, f"seeding destination — new {target_engine} session…"
                )
                destination = await run_job(
                    chat_id,
                    user_msg_id,
                    handoff_seed_prompt(summary),
                    None,
                    None,
                    thread_id=thread_id,
                    engine_override=target_engine,
                    persist_sessions=False,
                )
                if (
                    destination is None
                    or destination.cancelled
                    or destination.completed is None
                    or not destination.completed.ok
                    or destination.resume is None
                    or destination.resume.engine != target_engine
                ):
                    raise RuntimeError("destination session did not start")
                await _commit_handoff_routing(
                    topic_store=state.topic_store,
                    topic_key=(chat_id, cast(int, job.thread_id))
                    if state.topic_store is not None and job.thread_id is not None
                    else None,
                    chat_session_store=state.chat_session_store,
                    chat_session_key=job.session_key,
                    destination=destination.resume,
                )
                await _edit_operation_card(
                    job,
                    "completed — new session started with the summary.\n"
                    "Send your next message to continue; old-message replies retain "
                    "their old session.",
                )

                parts = MarkdownParts(header="handoff summary", body=summary)
                for rendered_text, entities in prepare_telegram_multi(
                    parts, max_body_chars=MAX_BODY_CHARS
                ):
                    await cfg.exec_cfg.transport.send(
                        channel_id=chat_id,
                        message=RenderedMessage(
                            text=rendered_text, extra={"entities": entities}
                        ),
                        options=SendOptions(
                            reply_to=MessageRef(
                                channel_id=chat_id, message_id=user_msg_id
                            ),
                            notify=False,
                            thread_id=thread_id,
                        ),
                    )

            async def run_thread_job(job: ThreadJob) -> None:
                try:
                    if job.kind == "compact":
                        await run_compact_job(job)
                        return
                    if job.kind == "handoff":
                        await run_handoff_job(job)
                        return
                    await run_job(
                        cast(int, job.chat_id),
                        cast(int, job.user_msg_id),
                        job.text,
                        job.resume_token,
                        job.context,
                        cast(int | None, job.thread_id),
                        job.session_key,
                        None,
                        scheduler.note_thread_known,
                        None,
                        job.progress_ref,
                        job.run_options,
                    )
                except anyio.get_cancelled_exc_class():
                    if job.kind in {"compact", "handoff"}:
                        with anyio.CancelScope(shield=True):
                            await _edit_operation_card(job, "cancelled")
                    raise

            async def _on_job_claimed(job: ThreadJob) -> None:
                if job.progress_ref is None:
                    return
                try:
                    if job.kind in {"compact", "handoff"}:
                        await _edit_operation_card(job, "claimed")
                        return
                    tracker = ProgressTracker(engine=job.resume_token.engine)
                    tracker.set_resume(job.resume_token)
                    options = job.run_options
                    context_line = cfg.runtime.format_context_line(
                        job.context,
                        plan=options.plan if options is not None else False,
                        goal=options.goal if options is not None else None,
                    )
                    st = tracker.snapshot(context_line=context_line)
                    msg = cfg.exec_cfg.presenter.render_progress(
                        st, elapsed_s=0.0, label="starting"
                    )
                    await cfg.exec_cfg.transport.edit(ref=job.progress_ref, message=msg)
                except Exception:  # noqa: BLE001 — observer must never break a run
                    logger.debug("scheduler.observer.claim.edit_failed", exc_info=True)

            async def _on_job_failed(job: ThreadJob, exc: BaseException) -> None:
                if job.progress_ref is None:
                    return
                try:
                    detail = user_safe_error(exc, fallback="run failed")
                    if job.kind in {"compact", "handoff"}:
                        await _edit_operation_card(job, f"failed — {detail}")
                        return
                    preview = job.text[:80].replace("\n", " ")
                    text = f"`failed`\n{detail}\n\n> {preview}"
                    msg = RenderedMessage(text=text)
                    await cfg.exec_cfg.transport.edit(ref=job.progress_ref, message=msg)
                except Exception:  # noqa: BLE001 — observer must never break a run
                    logger.debug("scheduler.observer.fail.edit_failed", exc_info=True)

            scheduler = ThreadScheduler(
                task_group=tg,
                run_job=run_thread_job,
                on_job_claimed=_on_job_claimed,
                on_job_failed=_on_job_failed,
            )

            # --- /at one-shot delayed runs (#288) ---
            from . import at_scheduler

            at_scheduler.install(
                tg,
                run_job,
                cfg.exec_cfg.transport,
                cfg.chat_id,
            )

            # --- /loop and ScheduleWakeup observation (#289) ---
            from .. import loop_scheduler

            loop_state_path = None
            config_path_for_loops = cfg.runtime.config_path
            if config_path_for_loops is not None:
                loop_state_path = config_path_for_loops.with_name(
                    loop_scheduler.STATE_FILENAME
                )

            def _is_chat_busy(chat_id_in: int) -> bool:
                """Drop a loop fire if the chat already has a run in flight
                — mirrors upstream's "no catch-up" semantic."""
                for ref in state.running_tasks:
                    if getattr(ref, "channel_id", None) == chat_id_in:
                        return True
                return False

            loop_scheduler.install(
                tg,
                run_job,
                cfg.exec_cfg.transport,
                cfg.chat_id,
                state_path=loop_state_path,
                is_chat_busy=_is_chat_busy,
            )

            # --- Trigger system (webhooks + cron) ---
            trigger_manager: TriggerManager | None = None
            if cfg.trigger_config and cfg.trigger_config.get("enabled"):
                from ..triggers.cron import run_cron_scheduler
                from ..triggers.dispatcher import TriggerDispatcher
                from ..triggers.manager import TriggerManager
                from ..triggers.server import run_webhook_server
                from ..triggers.settings import parse_trigger_config

                try:
                    trigger_settings = parse_trigger_config(cfg.trigger_config)
                    # #317: pass config_path so the manager can load/save
                    # the run_once fired-state alongside untether.toml.
                    trigger_manager = TriggerManager(
                        trigger_settings,
                        config_path=cfg.runtime.config_path,
                    )
                    # rc4 (#271): expose trigger_manager to commands via cfg so
                    # /ping and /config can render per-chat trigger indicators.
                    cfg.trigger_manager = trigger_manager
                    trigger_dispatcher = TriggerDispatcher(
                        run_job=cast(Callable[..., Awaitable[None]], run_job),
                        transport=cfg.exec_cfg.transport,
                        default_chat_id=cfg.chat_id,
                        task_group=tg,
                    )
                    # Always start the cron scheduler — it idles when the
                    # cron list is empty and picks up new crons on reload.
                    tg.start_soon(
                        run_cron_scheduler, trigger_manager, trigger_dispatcher
                    )
                    if trigger_settings.webhooks or trigger_settings.server:
                        tg.start_soon(
                            run_webhook_server,
                            trigger_settings,
                            trigger_dispatcher,
                            trigger_manager,
                        )
                    # #601: report BOTH counts. ``crons`` previously counted
                    # raw ``[[triggers.crons]]`` TOML entries while the
                    # manager/scheduler logs count active crons (raw minus
                    # run_once entries already spent per the persisted
                    # fired-state), which read as "3 crons failed to load"
                    # during triage. ``crons`` now matches the manager;
                    # ``crons_configured`` preserves the raw entry count.
                    logger.info(
                        "triggers.enabled",
                        webhooks=len(trigger_settings.webhooks),
                        crons=len(trigger_manager.crons),
                        crons_configured=len(trigger_settings.crons),
                    )
                except (ValueError, TypeError, OSError) as exc:
                    logger.error(
                        "triggers.init_failed",
                        error=str(exc),
                        error_type=exc.__class__.__name__,
                    )

            def resolve_topic_key(
                msg: TelegramIncomingMessage,
            ) -> tuple[int, int] | None:
                if state.topic_store is None:
                    return None
                return _topic_key(msg, cfg, scope_chat_ids=state.topics_chat_ids)

            def _build_upload_prompt(base: str, annotation: str) -> str:
                if base and base.strip():
                    return f"{base}\n\n{annotation}"
                return annotation

            async def resolve_prompt_message(
                msg: TelegramIncomingMessage,
                text: str,
                ambient_context: RunContext | None,
            ) -> ResolvedMessage | None:
                reply = make_reply(cfg, msg)
                try:
                    resolved = cfg.runtime.resolve_message(
                        text=text,
                        reply_text=msg.reply_to_text,
                        ambient_context=ambient_context,
                        chat_id=msg.chat_id,
                    )
                except DirectiveError as exc:
                    await reply(text=f"error:\n{exc}")
                    return None
                topic_key = resolve_topic_key(msg)
                chat_project = (
                    _topics_chat_project(cfg, msg.chat_id)
                    if cfg.topics.enabled
                    else None
                )
                _, ok = await ensure_topic_context(
                    resolved=resolved,
                    ambient_context=ambient_context,
                    topic_key=topic_key,
                    chat_project=chat_project,
                    reply=reply,
                )
                if not ok:
                    return None
                return resolved

            async def resolve_engine_defaults(
                *,
                explicit_engine: EngineId | None,
                context: RunContext | None,
                chat_id: int,
                topic_key: tuple[int, int] | None,
            ):
                return await resolve_engine_for_message(
                    runtime=cfg.runtime,
                    context=context,
                    explicit_engine=explicit_engine,
                    chat_id=chat_id,
                    topic_key=topic_key,
                    topic_store=state.topic_store,
                    chat_prefs=state.chat_prefs,
                )

            async def ensure_topic_context(
                *,
                resolved: ResolvedMessage,
                ambient_context: RunContext | None,
                topic_key: tuple[int, int] | None,
                chat_project: str | None,
                reply: Callable[..., Awaitable[None]],
            ) -> tuple[RunContext | None, bool]:
                effective_context = ambient_context
                if (
                    state.topic_store is not None
                    and topic_key is not None
                    and resolved.context is not None
                    and resolved.context_source == "directives"
                ):
                    await state.topic_store.set_context(*topic_key, resolved.context)
                    await _maybe_rename_topic(
                        cfg,
                        state.topic_store,
                        chat_id=topic_key[0],
                        thread_id=topic_key[1],
                        context=resolved.context,
                    )
                    effective_context = resolved.context
                if (
                    state.topic_store is not None
                    and topic_key is not None
                    and effective_context is None
                    and resolved.context_source not in {"directives", "reply_ctx"}
                ):
                    await reply(
                        text="this topic isn't bound to a project yet.\n"
                        f"{_usage_ctx_set(chat_project=chat_project)} or "
                        f"{_usage_topic(chat_project=chat_project)}",
                    )
                    return effective_context, False
                return effective_context, True

            resume_resolver = ResumeResolver(
                cfg=cfg,
                task_group=tg,
                running_tasks=state.running_tasks,
                enqueue_resume=scheduler.enqueue_resume,
                topic_store=state.topic_store,
                chat_session_store=state.chat_session_store,
            )

            async def dispatch_prompt_run(
                *,
                msg: TelegramIncomingMessage,
                prompt_text: str,
                resolved: ResolvedMessage,
                topic_key: tuple[int, int] | None,
                chat_session_key: tuple[int, int | None] | None,
                reply_ref: MessageRef | None,
                reply_id: int | None,
            ) -> None:
                chat_id = msg.chat_id
                user_msg_id = msg.message_id
                context = resolved.context
                engine_resolution = await resolve_engine_defaults(
                    explicit_engine=resolved.engine_override,
                    context=context,
                    chat_id=chat_id,
                    topic_key=topic_key,
                )
                engine_override = engine_resolution.engine
                resume_decision = await resume_resolver.resolve(
                    resume_token=resolved.resume_token,
                    reply_id=reply_id,
                    chat_id=chat_id,
                    user_msg_id=user_msg_id,
                    thread_id=msg.thread_id,
                    chat_session_key=chat_session_key,
                    topic_key=topic_key,
                    engine_for_session=engine_resolution.engine,
                    prompt_text=prompt_text,
                )
                if resume_decision.handled_by_running_task:
                    return
                resume_token = resume_decision.resume_token
                # Model validation + resume-model capability gate (see
                # docs/plans/slash-model-command-enhancement-plan.md).
                # Runs before any job creation / enqueue so a rejection
                # produces zero runner starts and unchanged stores.
                effective_model = resolved.model
                if effective_model is not None:
                    # Empty or whitespace-only model values are usage errors.
                    if not effective_model.strip():
                        await send_plain(
                            cfg.exec_cfg.transport,
                            chat_id=chat_id,
                            user_msg_id=user_msg_id,
                            text="Model override must not be empty.",
                            thread_id=msg.thread_id,
                        )
                        return
                    effective_engine = (
                        resume_token.engine
                        if resume_token is not None
                        else engine_override
                    )
                    # Resume-model capability: reject before job creation
                    # when resuming an authentic session and the engine
                    # can't change model on resume.
                    if resume_token is not None:
                        limit_msg = _check_resume_model_capability(
                            engine=effective_engine,
                            runtime=cfg.runtime,
                            model=effective_model,
                        )
                        if limit_msg is not None:
                            await send_plain(
                                cfg.exec_cfg.transport,
                                chat_id=chat_id,
                                user_msg_id=user_msg_id,
                                text=limit_msg,
                                thread_id=msg.thread_id,
                            )
                            return
                    # Catalog validation: reject or fall back per policy.
                    validation = _validate_model_override(
                        effective_model,
                        effective_engine,
                        runtime=cfg.runtime,
                        fallback_enabled=cfg.unknown_model_fallback,
                    )
                    if validation.action == "reject":
                        assert validation.message is not None
                        await send_plain(
                            cfg.exec_cfg.transport,
                            chat_id=chat_id,
                            user_msg_id=user_msg_id,
                            text=validation.message,
                            thread_id=msg.thread_id,
                        )
                        return
                    if validation.action == "fallback":
                        assert validation.message is not None
                        await send_plain(
                            cfg.exec_cfg.transport,
                            chat_id=chat_id,
                            user_msg_id=user_msg_id,
                            text=validation.message,
                            thread_id=msg.thread_id,
                        )
                        effective_model = None
                # Build directive options with the (possibly cleared) model.
                if effective_model != resolved.model:
                    from dataclasses import replace as _dc_replace

                    resolved = _dc_replace(resolved, model=effective_model)
                if resume_token is None:
                    _dir_opts = _directive_options(resolved)
                    await run_job(
                        chat_id,
                        user_msg_id,
                        prompt_text,
                        None,
                        context,
                        msg.thread_id,
                        chat_session_key,
                        reply_ref,
                        scheduler.note_thread_known,
                        engine_override,
                        None,
                        _dir_opts,
                    )
                    return
                progress_ref = await _send_queued_progress(
                    cfg,
                    chat_id=chat_id,
                    user_msg_id=user_msg_id,
                    thread_id=msg.thread_id,
                    resume_token=resume_token,
                    context=context,
                    run_options=_directive_options(resolved),
                )
                await scheduler.enqueue_resume(
                    chat_id,
                    user_msg_id,
                    prompt_text,
                    resume_token,
                    context,
                    msg.thread_id,
                    chat_session_key,
                    progress_ref,
                    _directive_options(resolved),
                )

            async def run_prompt_from_upload(
                msg: TelegramIncomingMessage,
                prompt_text: str,
                resolved: ResolvedMessage,
            ) -> None:
                reply_id = msg.reply_to_message_id
                reply_ref = (
                    MessageRef(
                        channel_id=msg.chat_id,
                        message_id=msg.reply_to_message_id,
                        thread_id=msg.thread_id,
                    )
                    if msg.reply_to_message_id is not None
                    else None
                )
                chat_session_key = _chat_session_key(
                    msg, store=state.chat_session_store
                )
                topic_key = resolve_topic_key(msg)
                await dispatch_prompt_run(
                    msg=msg,
                    prompt_text=prompt_text,
                    resolved=resolved,
                    topic_key=topic_key,
                    chat_session_key=chat_session_key,
                    reply_ref=reply_ref,
                    reply_id=reply_id,
                )

            async def _dispatch_pending_prompt(pending: _PendingPrompt) -> None:
                msg = pending.msg
                reply = make_reply(cfg, msg)
                try:
                    resolved = cfg.runtime.resolve_message(
                        text=pending.text,
                        reply_text=msg.reply_to_text,
                        ambient_context=pending.ambient_context,
                        chat_id=msg.chat_id,
                    )
                except DirectiveError as exc:
                    await reply(text=f"error:\n{exc}")
                    return
                if pending.is_voice_transcribed:
                    resolved = ResolvedMessage(
                        prompt=f"(voice transcribed) {resolved.prompt}",
                        resume_token=resolved.resume_token,
                        engine_override=resolved.engine_override,
                        context=resolved.context,
                        context_source=resolved.context_source,
                    )

                prompt_text = resolved.prompt
                if pending.forwards:
                    forwarded = [
                        text
                        for _, text in sorted(
                            pending.forwards,
                            key=lambda item: item[0],
                        )
                    ]
                    prompt_text = _format_forwarded_prompt(
                        forwarded,
                        prompt_text,
                    )

                _effective_context, ok = await ensure_topic_context(
                    resolved=resolved,
                    ambient_context=pending.ambient_context,
                    topic_key=pending.topic_key,
                    chat_project=pending.chat_project,
                    reply=reply,
                )
                if not ok:
                    return
                await dispatch_prompt_run(
                    msg=msg,
                    prompt_text=prompt_text,
                    resolved=resolved,
                    topic_key=pending.topic_key,
                    chat_session_key=pending.chat_session_key,
                    reply_ref=pending.reply_ref,
                    reply_id=pending.reply_id,
                )

            forward_coalescer = ForwardCoalescer(
                task_group=tg,
                debounce_s=state.forward_coalesce_s,
                sleep=sleep,
                dispatch=_dispatch_pending_prompt,
                pending=state.pending_prompts,
            )

            async def _dispatch_batched_prompt(pending: _PendingPrompt) -> None:
                """Dispatch an assembled batch.

                Command decisions happen on the joined text so plugin commands
                and engine directives resolve after batching. Mentions trigger
                mode is evaluated on the assembled text as well.
                """
                command_id, args_text = parse_slash_command(pending.text)
                if command_id is not None and command_id not in state.reserved_commands:
                    if command_id not in state.command_ids:
                        refresh_commands()
                    if command_id in state.command_ids:
                        chat_id = pending.msg.chat_id
                        topic_key = pending.topic_key
                        engine_resolution = await resolve_engine_defaults(
                            explicit_engine=None,
                            context=pending.ambient_context,
                            chat_id=chat_id,
                            topic_key=topic_key,
                        )
                        default_engine_override = (
                            engine_resolution.engine
                            if engine_resolution.source
                            in {"directive", "topic_default", "chat_default"}
                            else None
                        )
                        overrides_thread_id = (
                            topic_key[1] if topic_key is not None else None
                        )
                        engine_overrides_resolver = partial(
                            _resolve_engine_run_options,
                            chat_id,
                            overrides_thread_id,
                            chat_prefs=state.chat_prefs,
                            topic_store=state.topic_store,
                        )
                        tg.start_soon(
                            dispatch_command,
                            cfg,
                            pending.msg,
                            pending.text,
                            command_id,
                            args_text,
                            state.running_tasks,
                            scheduler,
                            wrap_on_thread_known(
                                scheduler.note_thread_known,
                                topic_key,
                                pending.chat_session_key,
                            ),
                            (
                                pending.topic_key is not None
                                or pending.chat_session_key is not None
                            ),
                            default_engine_override,
                            engine_overrides_resolver,
                        )
                        return

                listen_mode = await resolve_listen_mode(
                    chat_id=pending.msg.chat_id,
                    thread_id=pending.msg.thread_id,
                    chat_prefs=state.chat_prefs,
                    topic_store=state.topic_store,
                )
                if listen_mode == "mentions" and not should_trigger_run(
                    pending.msg,
                    bot_username=state.bot_username,
                    runtime=cfg.runtime,
                    command_ids=state.command_ids,
                    reserved_chat_commands=state.reserved_chat_commands,
                ):
                    return

                await _dispatch_pending_prompt(pending)

            prompt_batcher = PromptInputBatcher(
                task_group=tg,
                debounce_s=(
                    state.prompt_batch_debounce_s
                    if state.prompt_batch_enabled
                    else -1.0
                ),
                sleep=sleep,
                dispatch=_dispatch_batched_prompt,
                pending=state.prompt_batches,
                max_messages=state.prompt_batch_max_messages,
                max_chars=state.prompt_batch_max_chars,
                separator=state.prompt_batch_separator,
            )

            async def handle_prompt_upload(
                msg: TelegramIncomingMessage,
                caption_text: str,
                ambient_context: RunContext | None,
                topic_store: TopicStateStore | None,
            ) -> None:
                resolved = await resolve_prompt_message(
                    msg,
                    caption_text,
                    ambient_context,
                )
                if resolved is None:
                    return
                saved = await save_file_put(
                    cfg,
                    msg,
                    "",
                    resolved.context,
                    topic_store,
                )
                if saved is None:
                    return
                rel = saved.rel_path.as_posix()
                is_img = msg.document is not None and is_image_document(
                    mime_type=msg.document.mime_type,
                    file_name=msg.document.file_name,
                    raw=msg.document.raw,
                )
                if is_img:
                    annotation = format_image_prompt_annotation([rel])
                else:
                    annotation = f"[uploaded file: {rel}]\n\nExecute the task specified in this file: `{rel}`."
                prompt = _build_upload_prompt(resolved.prompt, annotation)
                await run_prompt_from_upload(msg, prompt, resolved)

            media_group_buffer = MediaGroupBuffer(
                task_group=tg,
                debounce_s=state.media_group_debounce_s,
                sleep=sleep,
                cfg=cfg,
                chat_prefs=state.chat_prefs,
                topic_store=state.topic_store,
                bot_username=state.bot_username,
                command_ids=lambda: state.command_ids,
                reserved_chat_commands=state.reserved_chat_commands,
                groups=state.media_groups,
                run_prompt_from_upload=run_prompt_from_upload,
                resolve_prompt_message=resolve_prompt_message,
            )

            async def build_message_context(
                msg: TelegramIncomingMessage,
            ) -> TelegramMsgContext:
                chat_id = msg.chat_id
                reply_id = msg.reply_to_message_id
                reply_ref = (
                    MessageRef(channel_id=chat_id, message_id=reply_id)
                    if reply_id is not None
                    else None
                )
                topic_key = resolve_topic_key(msg)
                chat_session_key = _chat_session_key(
                    msg, store=state.chat_session_store
                )
                stateful_mode = topic_key is not None or chat_session_key is not None
                chat_project = (
                    _topics_chat_project(cfg, chat_id) if cfg.topics.enabled else None
                )
                bound_context = (
                    await state.topic_store.get_context(*topic_key)
                    if state.topic_store is not None and topic_key is not None
                    else None
                )
                chat_bound_context = None
                if state.chat_prefs is not None:
                    chat_bound_context = await state.chat_prefs.get_context(chat_id)
                if bound_context is not None:
                    ambient_context = _merge_topic_context(
                        chat_project=chat_project, bound=bound_context
                    )
                elif chat_bound_context is not None:
                    ambient_context = chat_bound_context
                else:
                    ambient_context = _merge_topic_context(
                        chat_project=chat_project, bound=None
                    )
                return TelegramMsgContext(
                    chat_id=chat_id,
                    thread_id=msg.thread_id,
                    reply_id=reply_id,
                    reply_ref=reply_ref,
                    topic_key=topic_key,
                    chat_session_key=chat_session_key,
                    stateful_mode=stateful_mode,
                    chat_project=chat_project,
                    ambient_context=ambient_context,
                )

            async def route_message(msg: TelegramIncomingMessage) -> None:
                reply = make_reply(cfg, msg)
                classification = _classify_message(msg, files_enabled=cfg.files.enabled)
                text = classification.text
                is_voice_transcribed = False
                if classification.is_forward_candidate:
                    if prompt_batcher.attach_forward(msg):
                        return
                    forward_coalescer.attach_forward(msg)
                    return
                forward_key = _forward_key(msg)
                if classification.is_media_group_document:
                    media_group_buffer.add(msg)
                    return
                ctx = await build_message_context(msg)
                chat_id = ctx.chat_id
                reply_id = ctx.reply_id
                reply_ref = ctx.reply_ref
                topic_key = ctx.topic_key
                chat_session_key = ctx.chat_session_key
                stateful_mode = ctx.stateful_mode
                chat_project = ctx.chat_project
                ambient_context = ctx.ambient_context
                if classification.is_cancel:
                    prompt_batcher.cancel(
                        prompt_batcher.key_for_message(
                            msg,
                            topic_key=topic_key,
                            chat_session_key=chat_session_key,
                        )
                    )
                    forward_coalescer.cancel(forward_key)
                    tg.start_soon(
                        handle_cancel, cfg, msg, state.running_tasks, scheduler
                    )
                    return

                # --- Compact/handoff: intercept before normal command dispatch ---
                from .commands.parse import (
                    parse_compact_invocation,
                    parse_handoff_invocation,
                )

                compact_invocation = parse_compact_invocation(
                    text, engine_ids=cfg.runtime.engine_ids
                )
                if compact_invocation is not None:
                    prompt_batcher.cancel(
                        prompt_batcher.key_for_message(
                            msg,
                            topic_key=topic_key,
                            chat_session_key=chat_session_key,
                        )
                    )
                    forward_coalescer.cancel(forward_key)
                    tg.start_soon(
                        handle_compact_command,
                        compact_invocation.instructions,
                        compact_invocation.engine,
                        cfg,
                        msg,
                        reply,
                        scheduler,
                        resume_resolver,
                        state.topic_store,
                        state.chat_session_store,
                        topic_key,
                        chat_session_key,
                        reply_id,
                        state.running_tasks,
                        state,
                        ambient_context,
                        False,
                        compact_invocation.destination_engine,
                    )
                    return
                handoff_invocation = parse_handoff_invocation(
                    text, engine_ids=cfg.runtime.engine_ids
                )
                if handoff_invocation is not None:
                    prompt_batcher.cancel(
                        prompt_batcher.key_for_message(
                            msg,
                            topic_key=topic_key,
                            chat_session_key=chat_session_key,
                        )
                    )
                    forward_coalescer.cancel(forward_key)
                    tg.start_soon(
                        handle_compact_command,
                        handoff_invocation.instructions,
                        handoff_invocation.engine,
                        cfg,
                        msg,
                        reply,
                        scheduler,
                        resume_resolver,
                        state.topic_store,
                        state.chat_session_store,
                        topic_key,
                        chat_session_key,
                        reply_id,
                        state.running_tasks,
                        state,
                        ambient_context,
                        True,
                        handoff_invocation.destination_engine,
                    )
                    return

                command_id = classification.command_id
                args_text = classification.args_text

                # Meta-form gate: /plan, /goal, /subagent with free-form
                # args are NOT meta commands — they fall through to the
                # normal prompt/directive path. Only sticky/help forms
                # are dispatched to the meta handler.
                if command_id in {"plan", "goal", "subagent"}:
                    _meta_tokens = split_command_args(args_text)
                    _meta_head = _meta_tokens[0].lower() if _meta_tokens else ""
                    if command_id == "plan":
                        _is_meta = not _meta_tokens or _meta_head in {
                            "on",
                            "off",
                            "clear",
                            "show",
                        }
                    elif command_id == "goal":
                        _is_meta = not (args_text or "").strip()
                    else:  # subagent
                        _is_meta = (
                            not _meta_tokens
                            or (
                                len(_meta_tokens) == 1
                                and _meta_head in {"off", "clear", "show"}
                            )
                            or (len(_meta_tokens) == 2 and _meta_head == "set")
                        )
                    if not _is_meta:
                        command_id = None
                if command_id == "continue":
                    prompt_batcher.cancel(
                        prompt_batcher.key_for_message(
                            msg,
                            topic_key=topic_key,
                            chat_session_key=chat_session_key,
                        )
                    )
                    prompt_text = args_text.strip() if args_text else ""
                    resolved = cfg.runtime.resolve_message(
                        text=prompt_text,
                        reply_text=msg.reply_to_text,
                        ambient_context=ambient_context,
                        chat_id=chat_id,
                    )
                    engine_resolution = await resolve_engine_defaults(
                        explicit_engine=resolved.engine_override,
                        context=resolved.context,
                        chat_id=chat_id,
                        topic_key=topic_key,
                    )
                    continue_token = ResumeToken(
                        engine=engine_resolution.engine,
                        value="",
                        is_continue=True,
                    )
                    resolved = ResolvedMessage(
                        prompt=resolved.prompt,
                        resume_token=continue_token,
                        engine_override=resolved.engine_override,
                        context=resolved.context,
                    )
                    await dispatch_prompt_run(
                        msg=msg,
                        prompt_text=resolved.prompt,
                        resolved=resolved,
                        topic_key=topic_key,
                        chat_session_key=chat_session_key,
                        reply_ref=reply_ref,
                        reply_id=reply_id,
                    )
                    return
                if command_id is not None and _dispatch_builtin_command(
                    ctx=TelegramCommandContext(
                        cfg=cfg,
                        msg=msg,
                        args_text=args_text,
                        ambient_context=ambient_context,
                        topic_store=state.topic_store,
                        chat_prefs=state.chat_prefs,
                        resolved_scope=state.resolved_topics_scope,
                        scope_chat_ids=state.topics_chat_ids,
                        reply=reply,
                        task_group=tg,
                        running_tasks=state.running_tasks,
                        chat_session_store=state.chat_session_store,
                        chat_session_key=chat_session_key,
                        scheduler=scheduler,
                    ),
                    command_id=command_id,
                ):
                    prompt_batcher.cancel(
                        prompt_batcher.key_for_message(
                            msg,
                            topic_key=topic_key,
                            chat_session_key=chat_session_key,
                        )
                    )
                    return

                listen_mode = await resolve_listen_mode(
                    chat_id=chat_id,
                    thread_id=msg.thread_id,
                    chat_prefs=state.chat_prefs,
                    topic_store=state.topic_store,
                )
                if listen_mode == "mentions" and not should_trigger_run(
                    msg,
                    bot_username=state.bot_username,
                    runtime=cfg.runtime,
                    command_ids=state.command_ids,
                    reserved_chat_commands=state.reserved_chat_commands,
                ):
                    return

                if msg.voice is not None:
                    from ..triggers.ssrf import parse_networks

                    text = await transcribe_voice(
                        bot=cfg.bot,
                        msg=msg,
                        enabled=cfg.voice_transcription,
                        model=cfg.voice_transcription_model,
                        max_bytes=cfg.voice_max_bytes,
                        reply=reply,
                        providers=cfg.voice_transcription_providers,
                        base_url=cfg.voice_transcription_base_url,
                        api_key=(
                            cfg.voice_transcription_api_key.get_secret_value()
                            if cfg.voice_transcription_api_key is not None
                            else None
                        ),
                        groq_api_key=(
                            cfg.voice_transcription_groq_api_key.get_secret_value()
                            if cfg.voice_transcription_groq_api_key is not None
                            else None
                        ),
                        local_command=cfg.voice_transcription_local_command,
                        local_backend=cfg.voice_transcription_local_backend,
                        local_model=cfg.voice_transcription_local_model,
                        timeout_s=cfg.voice_transcription_timeout_s,
                        url_allowlist=parse_networks(
                            cfg.voice_transcription_url_allowlist
                        ),
                        language=cfg.voice_transcription_language,
                        transcribing_status=cfg.voice_transcribing_status,
                    )
                    if text is None:
                        return
                    is_voice_transcribed = True
                    if cfg.voice_show_transcription:
                        await reply(text=f"🎙 {text}")
                if msg.document is not None:
                    if cfg.files.enabled and cfg.files.auto_put:
                        caption_text = text.strip()
                        if cfg.files.auto_put_mode == "prompt" and caption_text:
                            tg.start_soon(
                                handle_prompt_upload,
                                msg,
                                caption_text,
                                ambient_context,
                                state.topic_store,
                            )
                        elif not caption_text:
                            tg.start_soon(
                                handle_file_put_default,
                                cfg,
                                msg,
                                ambient_context,
                                state.topic_store,
                            )
                        else:
                            tg.start_soon(
                                handle_file_put_default,
                                cfg,
                                msg,
                                ambient_context,
                                state.topic_store,
                            )
                    elif cfg.files.enabled:
                        tg.start_soon(
                            partial(reply, text=FILE_PUT_USAGE),
                        )
                    return
                if command_id is not None and command_id not in state.reserved_commands:
                    if command_id not in state.command_ids:
                        refresh_commands()
                    if command_id in state.command_ids:
                        engine_resolution = await resolve_engine_defaults(
                            explicit_engine=None,
                            context=ambient_context,
                            chat_id=chat_id,
                            topic_key=topic_key,
                        )
                        default_engine_override = (
                            engine_resolution.engine
                            if engine_resolution.source
                            in {"directive", "topic_default", "chat_default"}
                            else None
                        )
                        overrides_thread_id = (
                            topic_key[1] if topic_key is not None else None
                        )
                        engine_overrides_resolver = partial(
                            _resolve_engine_run_options,
                            chat_id,
                            overrides_thread_id,
                            chat_prefs=state.chat_prefs,
                            topic_store=state.topic_store,
                        )
                        tg.start_soon(
                            dispatch_command,
                            cfg,
                            msg,
                            text,
                            command_id,
                            args_text,
                            state.running_tasks,
                            scheduler,
                            wrap_on_thread_known(
                                scheduler.note_thread_known,
                                topic_key,
                                chat_session_key,
                            ),
                            stateful_mode,
                            default_engine_override,
                            engine_overrides_resolver,
                        )
                        prompt_batcher.cancel(
                            prompt_batcher.key_for_message(
                                msg,
                                topic_key=topic_key,
                                chat_session_key=chat_session_key,
                            )
                        )
                        return

                # A1: Intercept text as AskUserQuestion reply if one is pending
                if text and not is_voice_transcribed:
                    from ..runners.claude import (
                        answer_ask_question,
                        answer_ask_question_with_options,
                        get_ask_question_flow,
                        get_pending_ask_request,
                    )
                    from .commands.ask_question import send_next_ask_question_message

                    # Check for active option flow in "Other" text mode first
                    flow = get_ask_question_flow(channel_id=msg.chat_id)
                    if flow is not None and flow.awaiting_text:
                        flow.awaiting_text = False
                        current_q = flow.questions[flow.current_index]
                        question_key = current_q.get(
                            "question",
                            f"Question {flow.current_index + 1}",
                        )
                        flow.answers[question_key] = text
                        flow.current_index += 1

                        if flow.current_index < len(flow.questions):
                            # More questions — send next one as a new message
                            # (callback-button continuation edits in place via
                            # ctx.executor.edit; see commands/ask_question.py).
                            await send_next_ask_question_message(
                                cfg.exec_cfg.transport,
                                chat_id=chat_id,
                                user_msg_id=msg.message_id,
                                thread_id=msg.thread_id,
                                flow=flow,
                            )
                            return
                        else:
                            # All done — send structured answer
                            success = await answer_ask_question_with_options(
                                flow.request_id
                            )
                            if success:
                                await reply(text=_format_answered_echo(text))
                            return

                    pending_ask = get_pending_ask_request(channel_id=msg.chat_id)
                    if pending_ask is not None:
                        ask_req_id, _ask_question = pending_ask
                        logger.info(
                            "ask_user_question.answering",
                            request_id=ask_req_id,
                            answer_len=len(text),
                        )
                        success = await answer_ask_question(ask_req_id, text)
                        if success:
                            await reply(text=_format_answered_echo(text))
                            return

                # #523: catch `.new`-style leading-dot typos for slash
                # commands and surface a hint instead of dispatching a
                # full agent subprocess (which costs the per-run cold-
                # start of OAuth handshake + MCP catalog probe + preamble
                # injection, then leaves the user to cancel).
                # Only fires for plain text inputs (not voice transcripts
                # or document captions) so user-typed prose like
                # ``.new project idea: ...`` stays out of the heuristic.
                if (
                    not is_voice_transcribed
                    and msg.voice is None
                    and msg.document is None
                ):
                    typo_cmd = parse_dot_typo(
                        text, state.command_ids | state.reserved_chat_commands
                    )
                    if typo_cmd is not None:
                        logger.info(
                            "command.dot_typo.suppressed",
                            chat_id=chat_id,
                            typed=text[:40],
                            command=typo_cmd,
                        )
                        await reply(
                            text=(
                                f"Did you mean `/{typo_cmd}`? "
                                f"(The leading `.` looks like a typo for `/`.)\n\n"
                                f"Re-send with the slash if you meant the command, "
                                f"or rephrase to send to the agent."
                            )
                        )
                        return

                pending = _PendingPrompt(
                    msg=msg,
                    text=text,
                    ambient_context=ambient_context,
                    chat_project=chat_project,
                    topic_key=topic_key,
                    chat_session_key=chat_session_key,
                    reply_ref=reply_ref,
                    reply_id=reply_id,
                    is_voice_transcribed=is_voice_transcribed,
                    forwards=[],
                )
                if reply_id is not None and state.running_tasks.get(
                    MessageRef(channel_id=chat_id, message_id=reply_id)
                ):
                    logger.debug(
                        "forward.prompt.bypass",
                        chat_id=chat_id,
                        thread_id=msg.thread_id,
                        sender_id=msg.sender_id,
                        message_id=msg.message_id,
                        reason="reply_resume",
                    )
                    tg.start_soon(_dispatch_pending_prompt, pending)
                    return
                if prompt_batcher.schedule(pending):
                    return
                forward_coalescer.schedule(pending)

            # #377: empty `allowed_user_ids` is now a startup ConfigError
            # (see TelegramTransportSettings._validate_allowed_user_ids_or_optin).
            # The only way to reach this hook with no allowlist is the explicit
            # `allow_any_user = true` opt-in — log it at INFO every boot so the
            # deviation stays visible in journalctl.
            if getattr(cfg, "allow_any_user", False) or not cfg.allowed_user_ids:
                logger.info(
                    "security.allow_any_user",
                    hint="allow_any_user=true is in effect — bot accepts "
                    "commands from any Telegram user. Intended for "
                    "demos/dev only.",
                )

            async def _safe_answer_callback(query_id: str) -> None:
                try:
                    await cfg.bot.answer_callback_query(query_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "callback.answer.failed",
                        error=str(exc),
                        error_type=exc.__class__.__name__,
                        callback_query_id=query_id,
                    )

            async def route_update(update: TelegramIncomingUpdate) -> None:
                current_allowed = frozenset(cfg.allowed_user_ids)
                if current_allowed:
                    sender_id = update.sender_id
                    if sender_id is None or sender_id not in current_allowed:
                        logger.debug(
                            "update.ignored",
                            reason="sender_not_allowed",
                            chat_id=update.chat_id,
                            sender_id=sender_id,
                        )
                        return
                if update.update_id is not None:
                    update_id = update.update_id
                    if update_id in state.seen_update_ids:
                        logger.debug(
                            "update.ignored",
                            reason="duplicate_update",
                            update_id=update_id,
                            chat_id=update.chat_id,
                            sender_id=update.sender_id,
                        )
                        return
                    state.seen_update_ids.add(update_id)
                    state.seen_update_order.append(update_id)
                    if len(state.seen_update_order) > _SEEN_UPDATES_LIMIT:
                        oldest_update_id = state.seen_update_order.popleft()
                        state.seen_update_ids.discard(oldest_update_id)
                elif isinstance(update, TelegramIncomingMessage):
                    key = (update.chat_id, update.message_id)
                    if key in state.seen_message_keys:
                        logger.debug(
                            "update.ignored",
                            reason="duplicate_message",
                            chat_id=update.chat_id,
                            message_id=update.message_id,
                            sender_id=update.sender_id,
                        )
                        return
                    state.seen_message_keys.add(key)
                    state.seen_messages_order.append(key)
                    if len(state.seen_messages_order) > _SEEN_MESSAGES_LIMIT:
                        oldest = state.seen_messages_order.popleft()
                        state.seen_message_keys.discard(oldest)
                if isinstance(update, TelegramCallbackQuery):
                    if update.data == CANCEL_CALLBACK_DATA:
                        tg.start_soon(
                            handle_callback_cancel,
                            cfg,
                            update,
                            state.running_tasks,
                            scheduler,
                        )
                    elif update.data == STEER_CALLBACK_DATA:
                        tg.start_soon(
                            handle_callback_steer,
                            cfg,
                            update,
                            state.running_tasks,
                            scheduler,
                        )
                    elif update.data and update.data.startswith("compact:"):
                        from .commands.compact import handle_compact_callback

                        tg.start_soon(
                            handle_compact_callback,
                            cfg,
                            update,
                            state.pending_confirms,
                            scheduler,
                            state,
                        )
                    elif update.data:
                        # Route callback to command backend if registered
                        cb_command_id, cb_args_text = parse_callback_data(update.data)
                        if cb_command_id not in state.command_ids:
                            refresh_commands()
                        if cb_command_id in state.command_ids:
                            # Extract thread_id from raw callback data
                            cb_thread_id: int | None = None
                            if update.raw and isinstance(
                                update.raw.get("message"), dict
                            ):
                                cb_thread_id = update.raw["message"].get(
                                    "message_thread_id"
                                )
                            # Compute stateful mode for callback
                            cb_topic_key = (
                                (update.chat_id, cb_thread_id)
                                if state.topic_store is not None
                                and cb_thread_id is not None
                                else None
                            )
                            cb_stateful_mode = cb_topic_key is not None
                            tg.start_soon(
                                dispatch_callback,
                                cfg,
                                update,
                                cb_command_id,
                                cb_args_text,
                                cb_thread_id,
                                state.running_tasks,
                                scheduler,
                                wrap_on_thread_known(
                                    scheduler.note_thread_known,
                                    cb_topic_key,
                                    None,  # No chat session key for callbacks
                                ),
                                cb_stateful_mode,
                                None,  # No engine override for callbacks
                                update.callback_query_id,
                            )
                        else:
                            tg.start_soon(
                                _safe_answer_callback, update.callback_query_id
                            )
                    else:
                        tg.start_soon(
                            _safe_answer_callback,
                            update.callback_query_id,
                        )
                    return
                await route_message(update)

            async for update in poller_fn(cfg):
                await route_update(update)

            # Poller exhausted (tests / finite pollers) — let nested
            # ``start_soon`` chains (route → dispatch → run_engine) register
            # their running task before deciding the loop is idle. A bare
            # checkpoint can repeatedly resume this parent task first, then
            # close the task group before its descendants run.
            for _ in range(10):
                await sleep(0.01)
            while state.running_tasks:
                await sleep(0.1)
            request_shutdown()
    finally:
        _signal.signal(_signal.SIGTERM, _prev_sigterm)
        _signal.signal(_signal.SIGINT, _prev_sigint)
        logger.debug("signal.handler.restored", signals=["SIGTERM", "SIGINT"])
        reset_shutdown()
        # #559: give queued outbox sends (e.g. an agent's final message after a
        # self-restart) a bounded chance to flush before close() drops them.
        _flush_outbox = getattr(cfg.exec_cfg.transport, "flush_outbox", None)
        if _flush_outbox is not None:
            try:
                await _flush_outbox(timeout=5.0)
            except Exception:  # noqa: BLE001 — never let cleanup raise
                logger.warning("shutdown.flush_outbox.failed", exc_info=True)
        await cfg.exec_cfg.transport.close()
