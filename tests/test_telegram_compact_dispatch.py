"""Observable compact/handoff confirmation contracts."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any, cast

import anyio
import pytest

from untether.compact import CompactSupport
from untether.events import EventFactory
from untether.model import ResumeToken, UntetherEvent
from untether.runners.mock import Return, ScriptRunner
from untether.telegram.bridge import TelegramBridgeConfig, run_main_loop
from untether.telegram.commands.compact import (
    CompactConfirmRecord,
    _card,
    _confirm_callback_data,
    _confirm_markup,
    _expired,
    claim_pending_confirm,
    prune_pending_confirms,
    register_pending_confirm,
)
from untether.telegram.types import TelegramIncomingMessage
from untether.transport import MessageRef

from .telegram_fakes import FakeTransport, make_cfg


class _CompactRunner(ScriptRunner):
    """Scripted runner whose compact invocation is independently observable."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.compact_calls: list[tuple[ResumeToken, str | None]] = []

    def compact_support(self) -> CompactSupport:
        return CompactSupport(
            mode="slash_prompt",
            accepts_instructions=True,
            true_compaction=True,
        )

    async def compact(
        self, resume: ResumeToken, instructions: str | None = None
    ) -> AsyncIterator[UntetherEvent]:
        self.compact_calls.append((resume, instructions))
        yield EventFactory(self.engine).completed_ok(answer="", resume=resume)


def _compact_message(
    text: str,
    *,
    message_id: int,
    reply_to_text: str | None = None,
    reply_to_message_id: int | None = None,
) -> TelegramIncomingMessage:
    return TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=message_id,
        text=text,
        reply_to_message_id=reply_to_message_id,
        reply_to_text=reply_to_text,
        sender_id=123,
    )


def _compact_poller(
    messages: list[TelegramIncomingMessage],
) -> Any:
    async def poller(
        _cfg: TelegramBridgeConfig,
    ) -> AsyncIterator[TelegramIncomingMessage]:
        for message in messages:
            yield message
            await anyio.sleep(0.01)
        await anyio.sleep(0.1)

    return poller


@pytest.mark.anyio
async def test_compact_reply_footer_bypasses_debounce_and_uses_footer_session() -> None:
    runner = _CompactRunner([Return(answer="unused")], engine="codex")
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=10.0,
    )
    session_id = "footer-session-complete-distinct-suffix"

    await run_main_loop(
        cfg,
        _compact_poller(
            [
                _compact_message("pending prose", message_id=1),
                _compact_message(
                    "/compact focus on tests",
                    message_id=2,
                    reply_to_message_id=99,
                    reply_to_text=f"done\n`codex resume {session_id}`",
                ),
            ]
        ),
    )

    assert runner.calls == []
    assert runner.compact_calls == [
        (ResumeToken(engine="codex", value=session_id), "focus on tests")
    ]


@pytest.mark.anyio
async def test_compact_without_session_returns_guidance_without_operation_card() -> (
    None
):
    transport = FakeTransport()
    runner = _CompactRunner([Return(answer="unused")], engine="codex")
    cfg = replace(make_cfg(transport, runner), allowed_user_ids=(123,))

    await run_main_loop(
        cfg, _compact_poller([_compact_message("/compact", message_id=1)])
    )

    assert runner.compact_calls == []
    assert any(
        "no active session to compact" in call["message"].text.lower()
        for call in transport.send_calls
    )


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


def test_compact_confirmation_helpers_preserve_callback_contract() -> None:
    assert _confirm_callback_data("token", "confirm") == "compact:token:confirm"
    assert _confirm_markup("token")["inline_keyboard"][0][0]["callback_data"] == (
        "compact:token:confirm"
    )
    assert _card("queued", keyboard=True).extra == {"parse_mode": "Markdown"}
    assert _card("queued").extra["reply_markup"] == {"inline_keyboard": []}


def test_pending_confirmation_pruning_and_bounded_registration() -> None:
    expired = _record(token="expired", expires_at=100.0)
    current = _record(token="current", expires_at=102.0)
    registry = {expired.token: expired, current.token: current}

    assert _expired(expired, now=100.0)
    assert prune_pending_confirms(registry, now=101.0) == [expired]
    assert registry == {current.token: current}

    register_pending_confirm(registry, expired, now=101.0)
    assert registry[expired.token] is expired


def test_expired_or_reclaimed_confirmation_is_unavailable() -> None:
    record = _record(expires_at=100.0)
    registry = {record.token: record}

    assert (
        claim_pending_confirm(
            registry,
            record.token,
            chat_id=10,
            thread_id=20,
            sender_id=30,
            now=100.0,
        )
        is None
    )
    assert registry == {}

    record.claimed = True
    registry[record.token] = record
    assert (
        claim_pending_confirm(
            registry,
            record.token,
            chat_id=10,
            thread_id=20,
            sender_id=30,
        )
        is None
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
