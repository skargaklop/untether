"""Behavior tests for the Telegram /queue command."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from untether.model import ResumeToken
from untether.runners.run_options import EngineRunOptions
from untether.scheduler import ThreadJob
from untether.telegram.commands.queue_cmd import _preview, handle_queue_command
from untether.telegram.types import TelegramIncomingMessage
from untether.transport import MessageRef


def _message(**overrides: Any) -> TelegramIncomingMessage:
    values: dict[str, Any] = {
        "transport": "telegram",
        "chat_id": 9,
        "message_id": 20,
        "text": "/queue",
        "reply_to_message_id": None,
        "reply_to_text": None,
        "sender_id": 3,
    }
    values.update(overrides)
    return TelegramIncomingMessage(**values)


class _Scheduler:
    def __init__(self, jobs: list[ThreadJob], *, busy: bool) -> None:
        self.jobs = jobs
        self.busy = busy
        self.requested: ResumeToken | None = None

    async def list_queued_for_thread(self, resume: ResumeToken) -> list[ThreadJob]:
        self.requested = resume
        return self.jobs

    async def is_busy(self, resume: ResumeToken) -> bool:
        assert resume == self.requested
        return self.busy


async def _reply_collector(calls: list[str], *, text: str) -> None:
    calls.append(text)


def _job(text: str, **options: Any) -> ThreadJob:
    return ThreadJob(
        chat_id=9,
        user_msg_id=1,
        text=text,
        resume_token=ResumeToken(engine="codex", value="session"),
        progress_ref=MessageRef(channel_id=9, message_id=2),
        run_options=EngineRunOptions(**options) if options else None,
    )


def test_preview_normalizes_whitespace_and_truncates() -> None:
    assert _preview("  first\n second  ") == "first second"
    assert _preview("x" * 10, limit=5) == "xxxx…"


@pytest.mark.anyio
async def test_queue_command_reports_unavailable_scheduler() -> None:
    replies: list[str] = []

    await handle_queue_command(
        cast(Any, SimpleNamespace()),
        _message(),
        scheduler=None,
        running_tasks=None,
        reply=lambda *, text: _reply_collector(replies, text=text),
    )

    assert replies == ["queue is unavailable."]


@pytest.mark.anyio
async def test_queue_command_reports_no_active_thread() -> None:
    replies: list[str] = []

    await handle_queue_command(
        cast(Any, SimpleNamespace()),
        _message(),
        scheduler=cast(Any, _Scheduler([], busy=False)),
        running_tasks={},
        reply=lambda *, text: _reply_collector(replies, text=text),
    )

    assert replies == [
        "no active thread found for queue status.\n"
        "reply to a progress/final message, or wait for a run to start."
    ]


@pytest.mark.anyio
async def test_queue_command_renders_thread_jobs_and_modes() -> None:
    resume = ResumeToken(engine="codex", value="session")
    scheduler = _Scheduler(
        [_job("  first\njob  ", plan=True), _job("second", goal=True)], busy=True
    )
    replies: list[str] = []

    await handle_queue_command(
        cast(Any, SimpleNamespace()),
        _message(reply_to_message_id=7),
        scheduler=cast(Any, scheduler),
        running_tasks=cast(
            Any,
            {MessageRef(channel_id=9, message_id=7): SimpleNamespace(resume=resume)},
        ),
        reply=lambda *, text: _reply_collector(replies, text=text),
    )

    assert scheduler.requested == resume
    assert replies == [
        "thread: `codex:session`\n"
        "busy: yes\n"
        "queued: 2\n\n"
        "1. first job [plan]\n"
        "2. second [goal]"
    ]


@pytest.mark.anyio
async def test_queue_command_resolves_reply_context_when_running_task_is_absent() -> (
    None
):
    resume = ResumeToken(engine="claude", value="from-reply")
    scheduler = _Scheduler([], busy=False)
    replies: list[str] = []
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            resolve_message=lambda **_: SimpleNamespace(resume_token=resume)
        )
    )

    await handle_queue_command(
        cast(Any, cfg),
        _message(reply_to_text="prior answer"),
        scheduler=cast(Any, scheduler),
        running_tasks={},
        reply=lambda *, text: _reply_collector(replies, text=text),
    )

    assert scheduler.requested == resume
    assert replies == ["thread: `claude:from-reply`\nbusy: no\nqueued: 0"]


@pytest.mark.anyio
async def test_queue_command_falls_back_to_matching_active_thread() -> None:
    resume = ResumeToken(engine="pi", value="active")
    scheduler = _Scheduler([], busy=False)
    replies: list[str] = []
    running_tasks = {
        MessageRef(channel_id=8, message_id=1): SimpleNamespace(resume=resume),
        MessageRef(channel_id=9, message_id=2, thread_id=4): SimpleNamespace(
            resume=None
        ),
        MessageRef(channel_id=9, message_id=3, thread_id=5): SimpleNamespace(
            resume=resume
        ),
    }

    await handle_queue_command(
        cast(Any, SimpleNamespace()),
        _message(thread_id=5),
        scheduler=cast(Any, scheduler),
        running_tasks=cast(Any, running_tasks),
        reply=lambda *, text: _reply_collector(replies, text=text),
    )

    assert scheduler.requested == resume
    assert replies == ["thread: `pi:active`\nbusy: no\nqueued: 0"]
