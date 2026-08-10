"""/queue — show FIFO jobs for the current thread."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...model import ResumeToken
    from ...runner_bridge import RunningTasks
    from ...scheduler import ThreadScheduler
    from ...transport import MessageRef
    from ..bridge import TelegramBridgeConfig
    from ..types import TelegramIncomingMessage

_PREVIEW_CHARS = 80


def _preview(text: str, limit: int = _PREVIEW_CHARS) -> str:
    cleaned = " ".join(text.split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1] + "…"


async def handle_queue_command(
    cfg: TelegramBridgeConfig,
    msg: TelegramIncomingMessage,
    *,
    scheduler: ThreadScheduler | None,
    running_tasks: RunningTasks | None,
    reply: Callable[..., Awaitable[None]],
) -> None:
    if scheduler is None:
        await reply(text="queue is unavailable.")
        return

    resume: ResumeToken | None = None
    if msg.reply_to_message_id is not None and running_tasks is not None:
        ref = MessageRef(channel_id=msg.chat_id, message_id=msg.reply_to_message_id)
        task = running_tasks.get(ref)
        if task is not None:
            resume = task.resume

    if resume is None and msg.reply_to_text:
        try:
            resolved = cfg.runtime.resolve_message(
                text=msg.reply_to_text,
                reply_text=None,
                ambient_context=None,
                chat_id=msg.chat_id,
            )
            resume = resolved.resume_token
        except Exception:  # noqa: BLE001
            resume = None

    if resume is None and running_tasks is not None:
        for ref, task in running_tasks.items():
            if ref.channel_id != msg.chat_id:
                continue
            if msg.thread_id is not None and ref.thread_id != msg.thread_id:
                continue
            if task.resume is not None:
                resume = task.resume
                break

    if resume is None:
        await reply(
            text=(
                "no active thread found for queue status.\n"
                "reply to a progress/final message, or wait for a run to start."
            )
        )
        return

    jobs = await scheduler.list_queued_for_thread(resume)
    busy = await scheduler.is_busy(resume)
    lines = [
        f"thread: `{resume.engine}:{resume.value}`",
        f"busy: {'yes' if busy else 'no'}",
        f"queued: {len(jobs)}",
    ]
    if jobs:
        lines.append("")
        for i, job in enumerate(jobs, start=1):
            flags = []
            if job.plan:
                flags.append("plan")
            if job.goal:
                flags.append("goal")
            flag_s = f" [{', '.join(flags)}]" if flags else ""
            lines.append(f"{i}. {_preview(job.text)}{flag_s}")
    await reply(text="\n".join(lines))
