"""Observable compact/handoff confirmation contracts."""

import time
from typing import Any, cast

import pytest

from untether.model import ResumeToken
from untether.telegram.commands.compact import (
    CompactConfirmRecord,
    claim_pending_confirm,
    register_pending_confirm,
)
from untether.transport import MessageRef


def _record(
    *, token: str = "token", expires_at: float | None = None
) -> CompactConfirmRecord:
    return CompactConfirmRecord(
        token=token,
        kind="handoff",
        resume_token=ResumeToken(engine="claude", value="source"),
        instructions=None,
        destination_engine="codex",
        chat_id=10,
        thread_id=20,
        session_key=(10, 20),
        sender_id=30,
        user_msg_id=40,
        progress_ref=None,
        created_monotonic=100.0,
        expiry_monotonic=(
            expires_at if expires_at is not None else time.monotonic() + 100.0
        ),
    )


def test_unauthorized_callback_does_not_consume_confirmation() -> None:
    registry: dict[str, CompactConfirmRecord] = {}
    record = _record()
    register_pending_confirm(registry, record, now=100.0)

    assert (
        claim_pending_confirm(
            registry,
            record.token,
            chat_id=10,
            thread_id=20,
            sender_id=999,
            now=101.0,
        )
        is None
    )
    assert registry[record.token] is record

    assert (
        claim_pending_confirm(
            registry,
            record.token,
            chat_id=10,
            thread_id=20,
            sender_id=30,
            now=101.0,
        )
        is record
    )


class _Scheduler:
    def __init__(self) -> None:
        self.jobs: list[object] = []

    async def enqueue(self, job: object) -> None:
        self.jobs.append(job)


@pytest.mark.anyio
async def test_confirm_queues_once_and_clears_card_keyboard() -> None:
    from tests.telegram_fakes import FakeTransport, make_cfg
    from untether.telegram.commands.compact import handle_compact_callback
    from untether.telegram.types import TelegramCallbackQuery

    transport = FakeTransport()
    cfg = make_cfg(transport)
    record = _record()
    record.progress_ref = MessageRef(channel_id=10, message_id=50)
    registry = {record.token: record}
    scheduler = _Scheduler()
    update = TelegramCallbackQuery(
        transport="telegram",
        chat_id=10,
        message_id=50,
        callback_query_id="query",
        data="compact:token:confirm",
        sender_id=30,
        raw={"message": {"message_thread_id": 20}},
    )

    await handle_compact_callback(cfg, update, registry, scheduler, object())

    assert len(scheduler.jobs) == 1
    assert transport.edit_calls[-1]["message"].text.startswith("queued")
    assert transport.edit_calls[-1]["message"].extra["reply_markup"] == {
        "inline_keyboard": []
    }


@pytest.mark.anyio
async def test_callback_allowlist_rejects_before_confirmation_claim() -> None:
    from tests.telegram_fakes import FakeTransport, make_cfg
    from untether.telegram.commands.compact import handle_compact_callback
    from untether.telegram.types import TelegramCallbackQuery

    transport = FakeTransport()
    cfg = make_cfg(transport)
    cfg.allowed_user_ids = (30,)
    record = _record()
    registry = {record.token: record}
    update = TelegramCallbackQuery(
        transport="telegram",
        chat_id=10,
        message_id=50,
        callback_query_id="query",
        data="compact:token:confirm",
        sender_id=999,
        raw={"message": {"message_thread_id": 20}},
    )

    await handle_compact_callback(cfg, update, registry, _Scheduler(), object())

    assert registry[record.token] is record
    assert cast(Any, cfg.bot).callback_calls[-1]["text"] == "Not authorised"


@pytest.mark.anyio
async def test_queued_handoff_cancel_edits_operation_card() -> None:
    from tests.telegram_fakes import FakeTransport, make_cfg
    from untether.scheduler import ThreadJob
    from untether.telegram.commands.cancel import _edit_cancelled_message

    transport = FakeTransport()
    cfg = make_cfg(transport)
    ref = MessageRef(channel_id=10, message_id=50)
    job = ThreadJob(
        chat_id=10,
        user_msg_id=40,
        text="[handoff]",
        resume_token=ResumeToken(engine="claude", value="source"),
        progress_ref=ref,
        kind="handoff",
    )

    await _edit_cancelled_message(cfg, ref, job)

    assert transport.edit_calls[-1]["message"].text == "cancelled"
    assert transport.edit_calls[-1]["message"].extra["reply_markup"] == {
        "inline_keyboard": []
    }


@pytest.mark.anyio
async def test_expired_callback_marks_its_card_expired() -> None:
    from tests.telegram_fakes import FakeTransport, make_cfg
    from untether.telegram.commands.compact import handle_compact_callback
    from untether.telegram.types import TelegramCallbackQuery

    transport = FakeTransport()
    cfg = make_cfg(transport)
    record = _record(expires_at=time.monotonic() - 1.0)
    record.progress_ref = MessageRef(channel_id=10, message_id=50)
    registry = {record.token: record}
    update = TelegramCallbackQuery(
        transport="telegram",
        chat_id=10,
        message_id=50,
        callback_query_id="query",
        data="compact:token:cancel",
        sender_id=30,
        raw={"message": {"message_thread_id": 20}},
    )

    await handle_compact_callback(cfg, update, registry, _Scheduler(), object())

    assert transport.edit_calls[-1]["message"].text == "expired"
    assert transport.edit_calls[-1]["message"].extra["reply_markup"] == {
        "inline_keyboard": []
    }


@pytest.mark.anyio
async def test_executor_returns_completed_run_outcome(monkeypatch) -> None:
    from tests.telegram_fakes import FakeTransport, make_cfg
    from untether.runner_bridge import RunOutcome
    from untether.telegram.commands import executor

    expected = RunOutcome(resume=ResumeToken(engine="codex", value="destination"))

    async def fake_handle_message(*_args: object, **_kwargs: object) -> RunOutcome:
        return expected

    monkeypatch.setattr(executor, "handle_message", fake_handle_message)
    cfg = make_cfg(FakeTransport())

    outcome = await executor._run_engine(
        exec_cfg=cfg.exec_cfg,
        runtime=cfg.runtime,
        running_tasks={},
        chat_id=10,
        user_msg_id=20,
        text="seed",
        resume_token=None,
        context=None,
        engine_override="codex",
    )

    assert outcome is expected


@pytest.mark.anyio
async def test_handoff_route_commit_rolls_back_first_store_on_second_failure() -> None:
    from untether.telegram.loop import _commit_handoff_routing

    class Store:
        def __init__(self, previous: ResumeToken | None, *, fail: bool = False) -> None:
            self.previous = previous
            self.current = previous
            self.fail = fail

        async def get_session_resume(self, *_args: object) -> ResumeToken | None:
            return cast(ResumeToken | None, self.current)

        async def set_session_resume(self, *_args: object) -> None:
            if self.fail:
                raise OSError("write failed")
            self.current = _args[-1]

        async def clear_engine_session(self, *_args: object) -> None:
            self.current = None

    old_topic = ResumeToken(engine="codex", value="old-topic")
    old_chat = ResumeToken(engine="codex", value="old-chat")
    topic_store = Store(old_topic)
    chat_store = Store(old_chat, fail=True)
    destination = ResumeToken(engine="codex", value="destination")

    with pytest.raises(OSError, match="write failed"):
        await _commit_handoff_routing(
            topic_store=cast(Any, topic_store),
            topic_key=(10, 20),
            chat_session_store=cast(Any, chat_store),
            chat_session_key=(10, 30),
            destination=destination,
        )

    assert topic_store.current == old_topic
    assert chat_store.current == old_chat
