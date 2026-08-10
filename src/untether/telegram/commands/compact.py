"""Compact/handoff command handler.

Session resolution, engine routing, and confirmation flow for /compact and
/handoff. Adapted from the Takopi compact flow for Untether's infrastructure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ...model import EngineId, ResumeToken

if TYPE_CHECKING:
    from ...context import RunContext
    from ...scheduler import ThreadScheduler
    from ..bridge import TelegramBridgeConfig
    from ..loop import ResumeResolver, TelegramLoopState
    from ..types import TelegramIncomingMessage


Callable = Any  # ReplyCallable alias to avoid import cycle


@dataclass(frozen=True, slots=True)
class PendingCompactConfirm:
    """Stored state for a pending compact-confirmation callback."""

    resume_token: ResumeToken
    instructions: str | None
    user_msg_id: int
    thread_id: int | None
    session_key: tuple[int, int | None] | None
    destination_engine: EngineId | None = None


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
    from ...transport import MessageRef
    from ..bridge import send_plain
    from ..engine_defaults import resolve_engine_for_message

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
                    reply_resume = resolved_r.runner.extract_resume(
                        msg.reply_to_text
                    )
                    if reply_resume is not None:
                        break
            except Exception:  # noqa: BLE001
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

        job = ThreadJob(
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            text="[compact]",
            resume_token=resume_token,
            context=None,
            thread_id=msg.thread_id,
            session_key=chat_session_key,
            progress_ref=None,
            plan=False,
            goal=None,
            kind="compact",
            compact_instructions=final_instructions,
        )
        await scheduler.enqueue(job)

        await send_plain(
            cfg.exec_cfg.transport,
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            text=f"compacting {resume_token.engine} session…",
            notify=False,
            thread_id=msg.thread_id,
        )
        return

    # --- Handoff path: enqueue as handoff job ---
    target_engine = destination_engine if cross_engine else resume_token.engine
    job = ThreadJob(
        chat_id=chat_id,
        user_msg_id=user_msg_id,
        text="[handoff]",
        resume_token=resume_token,
        context=None,
        thread_id=msg.thread_id,
        session_key=chat_session_key,
        progress_ref=None,
        plan=False,
        goal=None,
        kind="handoff",
        compact_instructions=instructions,
        handoff_target=target_engine,
    )
    await scheduler.enqueue(job)

    prefix = ""
    if cross_engine:
        prefix = f"Handoff from {resume_token.engine} to a NEW {target_engine} session…"
    else:
        prefix = f"Creating handoff summary for {target_engine} session…"

    await send_plain(
        cfg.exec_cfg.transport,
        chat_id=chat_id,
        user_msg_id=user_msg_id,
        text=prefix,
        notify=False,
        thread_id=msg.thread_id,
    )
