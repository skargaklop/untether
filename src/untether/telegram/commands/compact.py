"""Compact/handoff command handler.

Session resolution, engine routing, and confirmation flow for /compact and
/handoff. Adapted from the Takopi compact flow for Untether's infrastructure.
"""

from __future__ import annotations

import contextlib
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from ...model import EngineId, ResumeToken
from ..engine_defaults import resolve_engine_for_message

if TYPE_CHECKING:
    from ...context import RunContext
    from ...scheduler import ThreadScheduler
    from ..bridge import TelegramBridgeConfig
    from ..loop import ResumeResolver, TelegramLoopState
    from ..types import TelegramIncomingMessage

Callable = Any  # ReplyCallable alias to avoid import cycle

_CONFIRM_EXPIRY_S = 300.0  # 5 minutes
_MAX_PENDING_CONFIRMS = 256
CompactOpKind = Literal["compact", "handoff"]


@dataclass(slots=True)
class CompactConfirmRecord:
    """Authorization-scoped pending compact/handoff confirmation.

    The callback payload carries only ``compact:<token>:confirm|cancel``;
    resumes/instructions are never exposed in callback data.
    """

    token: str
    kind: CompactOpKind
    resume_token: ResumeToken
    instructions: str | None
    destination_engine: EngineId | None
    chat_id: int
    thread_id: int | None
    session_key: tuple[int, int | None] | None
    sender_id: int | None
    user_msg_id: int
    progress_ref: Any  # MessageRef, typed Any to avoid import cycle
    created_monotonic: float
    expiry_monotonic: float
    claimed: bool = False


def _new_token() -> str:
    return secrets.token_urlsafe(16)


def _confirm_callback_data(token: str, action: str) -> str:
    return f"compact:{token}:{action}"


def _confirm_markup(token: str) -> dict[str, list]:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "confirm",
                    "callback_data": _confirm_callback_data(token, "confirm"),
                },
                {
                    "text": "cancel",
                    "callback_data": _confirm_callback_data(token, "cancel"),
                },
            ]
        ]
    }


def _expired(record: CompactConfirmRecord, now: float | None = None) -> bool:
    _now = now if now is not None else time.monotonic()
    return _now >= record.expiry_monotonic


def prune_pending_confirms(
    registry: dict[str, CompactConfirmRecord],
    *,
    now: float | None = None,
) -> list[CompactConfirmRecord]:
    """Remove and return expired records. Called before insertion and on touch."""
    _now = now if now is not None else time.monotonic()
    expired = [rec for rec in registry.values() if _now >= rec.expiry_monotonic]
    for rec in expired:
        registry.pop(rec.token, None)
    return expired


def register_pending_confirm(
    registry: dict[str, CompactConfirmRecord],
    record: CompactConfirmRecord,
    *,
    now: float | None = None,
) -> None:
    """Insert a pending record, pruning expired/oldest to stay bounded."""
    prune_pending_confirms(registry, now=now)
    while len(registry) >= _MAX_PENDING_CONFIRMS:
        # Prune oldest by created_monotonic
        oldest_token = min(registry, key=lambda t: registry[t].created_monotonic)
        registry.pop(oldest_token, None)
    registry[record.token] = record


def claim_pending_confirm(
    registry: dict[str, CompactConfirmRecord],
    token: str,
    *,
    chat_id: int,
    thread_id: int | None,
    sender_id: int | None,
    now: float | None = None,
) -> CompactConfirmRecord | None:
    """Atomically claim a scoped pending record.

    Rejected callbacks leave a valid record pending, so an unauthorized or
    misrouted callback cannot deny the initiating user their confirmation.
    Successful and expired claims are removed, making callbacks one-shot.
    """
    record = registry.get(token)
    if record is None or record.claimed:
        return None
    if record.chat_id != chat_id or record.thread_id != thread_id:
        return None
    if record.sender_id != sender_id:
        return None
    if _expired(record, now=now):
        registry.pop(token, None)
        return None
    record.claimed = True
    registry.pop(token, None)
    return record


def _card(text: str, *, keyboard: bool = False) -> Any:
    """Render a compact operation card and clear controls when terminal."""
    from ...transport import RenderedMessage

    extra: dict[str, Any] = {"parse_mode": "Markdown"}
    if not keyboard:
        extra["reply_markup"] = {"inline_keyboard": []}
    return RenderedMessage(text=text, extra=extra)


async def handle_compact_command(
    instructions: str | None,
    engine_override: EngineId | None,
    cfg: TelegramBridgeConfig,
    msg: TelegramIncomingMessage,
    reply: Any,
    scheduler: ThreadScheduler,
    resume_resolver: ResumeResolver,
    topic_store: object | None,
    chat_session_store: object | None,
    topic_key: tuple[int, int] | None,
    chat_session_key: tuple[int, int | None] | None,
    reply_id: int | None,
    running_tasks: Any,
    state: TelegramLoopState,
    ambient_context: RunContext | None,
    force_handoff: bool,
    destination_engine: EngineId | None,
) -> None:
    """Resolve an existing session and enqueue a compact/handoff job."""
    from ...compact import get_compact_support, warn_if_dropping_instructions
    from ...scheduler import ThreadJob
    from ...transport import MessageRef, SendOptions
    from ...utils.error_display import user_safe_error

    chat_id = msg.chat_id
    user_msg_id = msg.message_id

    # --- Engine resolution with precedence ---
    reply_resume: ResumeToken | None = None
    if msg.reply_to_text:
        for eid in cfg.runtime.engine_ids:
            try:
                resolved_r = cfg.runtime.resolve_runner(
                    resume_token=None, engine_override=eid
                )
                if resolved_r.available:
                    reply_resume = resolved_r.runner.extract_resume(msg.reply_to_text)
                    if reply_resume is not None:
                        break
            except (KeyError, LookupError, ValueError):
                continue
    if engine_override is not None:
        engine = engine_override
    elif reply_resume is not None:
        engine = reply_resume.engine
    else:
        engine_resolution = await resolve_engine_for_message(
            runtime=cfg.runtime,
            context=ambient_context,
            explicit_engine=None,
            chat_id=chat_id,
            topic_key=topic_key,
            topic_store=topic_store,  # type: ignore[arg-type]
            chat_prefs=state.chat_prefs,
        )
        engine = engine_resolution.engine

    footer_token: ResumeToken | None = None
    if reply_resume is not None and reply_resume.engine == engine:
        footer_token = reply_resume

    # --- Token resolution ---
    resume_token: ResumeToken | None = None

    if reply_id is not None:
        ref = MessageRef(channel_id=chat_id, message_id=reply_id)
        running_task = running_tasks.get(ref)  # type: ignore[call-overload]
        if running_task is not None:
            from ..loop import _wait_for_resume

            resume_token = await _wait_for_resume(running_task)

    if resume_token is None:
        resume_decision = await resume_resolver.resolve(
            resume_token=footer_token,
            reply_id=reply_id,
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            thread_id=msg.thread_id,
            chat_session_key=chat_session_key,
            topic_key=topic_key,
            engine_for_session=engine,
            prompt_text="",
        )
        resume_token = resume_decision.resume_token

    if resume_token is None:
        await reply(
            text=(
                "no active session to compact.\n"
                "reply to an Untether progress/final message, "
                "or send a normal prompt first."
            )
        )
        return

    resolved = cfg.runtime.resolve_runner(
        resume_token=resume_token,
        engine_override=resume_token.engine,
    )
    runner = resolved.runner
    support = get_compact_support(runner)

    # --- Destination validation ---
    cross_engine = (
        destination_engine is not None and destination_engine != resume_token.engine
    )
    if cross_engine:
        dest_resolved = cfg.runtime.resolve_runner(
            resume_token=None,
            engine_override=destination_engine,
        )
        if not dest_resolved.available:
            await reply(
                text=f"engine {destination_engine} is not available for handoff."
            )
            return

    # --- True-compaction engines: immediate compact path ---
    # (Approval gate for handoff_only/none/cross-engine handled via job.kind)
    if not force_handoff and not cross_engine and support.true_compaction:
        final_instructions = instructions
        if final_instructions and not support.accepts_instructions:
            warning = warn_if_dropping_instructions(
                resume_token.engine, final_instructions
            )
            if warning:
                await reply(text=warning)
            final_instructions = None

        card = await cfg.exec_cfg.transport.send(
            channel_id=chat_id,
            message=_card(f"queued — compacting {resume_token.engine} session…"),
            options=SendOptions(
                reply_to=MessageRef(channel_id=chat_id, message_id=user_msg_id),
                notify=True,
                thread_id=msg.thread_id,
            ),
        )
        if card is None:
            await reply(text="failed to create compact operation card.")
            return
        job = ThreadJob(
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            text="[compact]",
            resume_token=resume_token,
            context=None,
            thread_id=msg.thread_id,
            session_key=chat_session_key,
            progress_ref=card,
            kind="compact",
            compact_instructions=final_instructions,
        )
        try:
            await scheduler.enqueue(job)
        except Exception as exc:  # noqa: BLE001
            await cfg.exec_cfg.transport.edit(
                ref=card,
                message=_card(
                    f"failed: {user_safe_error(exc, fallback='enqueue failed')}"
                ),
            )
        return
    # --- Handoff path: create confirmation card (do NOT enqueue yet) ---
    target_engine = destination_engine if cross_engine else resume_token.engine
    op_kind: CompactOpKind = "handoff"

    token = _new_token()
    now = time.monotonic()
    record = CompactConfirmRecord(
        token=token,
        kind=op_kind,
        resume_token=resume_token,
        instructions=instructions,
        destination_engine=target_engine,
        chat_id=chat_id,
        thread_id=msg.thread_id,
        session_key=chat_session_key,
        sender_id=msg.sender_id,
        user_msg_id=user_msg_id,
        progress_ref=None,
        created_monotonic=now,
        expiry_monotonic=now + _CONFIRM_EXPIRY_S,
    )

    # Build confirmation card text.
    if cross_engine:
        desc = (
            f"Handoff from {resume_token.engine} to a NEW {target_engine} session.\n"
            "A new destination session will be seeded with the summary.\n"
            "Confirm to proceed, or cancel."
        )
    else:
        desc = (
            f"Creating handoff summary for a new {target_engine} session.\n"
            "A new destination session will be seeded with the summary.\n"
            "Confirm to proceed, or cancel."
        )

    from ...transport import RenderedMessage

    markup = _confirm_markup(token)
    sent = await cfg.exec_cfg.transport.send(
        channel_id=chat_id,
        message=RenderedMessage(
            text=desc,
            extra={"parse_mode": "Markdown", "reply_markup": markup},
        ),
        options=SendOptions(
            reply_to=MessageRef(channel_id=chat_id, message_id=user_msg_id),
            notify=True,
            thread_id=msg.thread_id,
        ),
    )
    if sent is None:
        await reply(text="failed to send confirmation card.")
        return
    record.progress_ref = sent
    register_pending_confirm(state.pending_confirms, record, now=now)


async def handle_compact_callback(
    cfg: Any,
    update: Any,
    registry: dict[str, CompactConfirmRecord],
    scheduler: Any,
    state: Any,
) -> None:
    """Atomically authorize and apply a compact confirmation callback."""
    from ...scheduler import ThreadJob
    from ...transport import MessageRef
    from ...utils.error_display import user_safe_error

    query_id = update.callback_query_id
    if (
        cfg.allowed_user_ids
        and update.sender_id is not None
        and update.sender_id not in cfg.allowed_user_ids
    ):
        await cfg.bot.answer_callback_query(query_id, text="Not authorised")
        return
    with contextlib.suppress(Exception):
        await cfg.bot.answer_callback_query(query_id)

    parts = (update.data or "").split(":")
    if len(parts) != 3:
        return
    prefix, token, action = parts
    if prefix != "compact" or action not in {"confirm", "cancel"}:
        return

    cb_thread_id: int | None = None
    if update.raw and isinstance(update.raw.get("message"), dict):
        cb_thread_id = update.raw["message"].get("message_thread_id")

    candidate = registry.get(token)
    expired = candidate is not None and _expired(candidate)
    record = claim_pending_confirm(
        registry,
        token,
        chat_id=update.chat_id,
        thread_id=cb_thread_id,
        sender_id=update.sender_id,
    )
    if record is None:
        if not expired:
            return
        with contextlib.suppress(Exception):
            await cfg.exec_cfg.transport.edit(
                ref=MessageRef(channel_id=update.chat_id, message_id=update.message_id),
                message=_card("expired"),
            )
        return

    if action == "cancel":
        if record.progress_ref is not None:
            with contextlib.suppress(Exception):
                await cfg.exec_cfg.transport.edit(
                    ref=record.progress_ref, message=_card("cancelled")
                )
        return

    target_engine = record.destination_engine or record.resume_token.engine
    job = ThreadJob(
        chat_id=record.chat_id,
        user_msg_id=record.user_msg_id,
        text="[handoff]",
        resume_token=record.resume_token,
        context=None,
        thread_id=record.thread_id,
        session_key=record.session_key,
        progress_ref=record.progress_ref,
        kind="handoff",
        compact_instructions=record.instructions,
        handoff_target=target_engine,
    )
    try:
        await scheduler.enqueue(job)
    except Exception as exc:  # noqa: BLE001
        if record.progress_ref is not None:
            with contextlib.suppress(Exception):
                await cfg.exec_cfg.transport.edit(
                    ref=record.progress_ref,
                    message=_card(
                        f"failed: {user_safe_error(exc, fallback='enqueue failed')}"
                    ),
                )
        return

    if record.progress_ref is not None:
        with contextlib.suppress(Exception):
            await cfg.exec_cfg.transport.edit(
                ref=record.progress_ref,
                message=_card(f"queued — handoff to {target_engine}…"),
            )
