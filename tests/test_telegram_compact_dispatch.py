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
                    reply_to_text=f"done\n↩️ `codex resume {session_id}`",
                ),
            ]
        ),
    )

    assert runner.calls == []
    assert runner.compact_calls == [
        (ResumeToken(engine="codex", value=session_id), "focus on tests")
    ]
    assert any(
        call["message"].text == "compacting codex session…"
        for call in cast(FakeTransport, cfg.exec_cfg.transport).send_calls
    )
    operation_edits = [
        call["message"].text
        for call in cast(FakeTransport, cfg.exec_cfg.transport).edit_calls
    ]
    assert "claimed" not in operation_edits
    assert operation_edits[-1] == "completed — compaction finished."


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
async def test_confirm_shows_progress_before_handoff_starts() -> None:
    from tests.telegram_fakes import FakeTransport, make_cfg
    from untether.telegram.commands.compact import handle_compact_callback
    from untether.telegram.types import TelegramCallbackQuery

    transport = FakeTransport()
    cfg = make_cfg(transport)
    record = _record()
    record.progress_ref = MessageRef(channel_id=10, message_id=50)
    registry = {record.token: record}

    class _AssertingScheduler:
        status_before_enqueue: str | None = None

        async def enqueue(self, job: object) -> None:
            if transport.edit_calls:
                self.status_before_enqueue = transport.edit_calls[-1]["message"].text

    update = TelegramCallbackQuery(
        transport="telegram",
        chat_id=10,
        message_id=50,
        callback_query_id="query",
        data="compact:token:confirm",
        sender_id=30,
        raw={"message": {"message_thread_id": 20}},
    )

    scheduler = _AssertingScheduler()
    await handle_compact_callback(cfg, update, registry, scheduler, object())

    assert scheduler.status_before_enqueue == "creating handoff summary…"


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
    assert transport.edit_calls[-1]["message"].text == "creating handoff summary…"
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


@pytest.mark.anyio
@pytest.mark.parametrize("handoff", [False, True])
@pytest.mark.parametrize("topic", [False, True])
async def test_compact_job_applies_chat_model_override(
    tmp_path, handoff, topic
) -> None:
    """Compact and confirmed handoff retain the source model preference."""
    from untether.config import ProjectsConfig
    from untether.router import AutoRouter, RunnerEntry
    from untether.runners.run_options import EngineRunOptions, get_run_options
    from untether.settings import TelegramTopicsSettings
    from untether.telegram.chat_prefs import ChatPrefsStore, resolve_prefs_path
    from untether.telegram.engine_overrides import EngineOverrides
    from untether.telegram.topic_state import TopicStateStore, resolve_state_path
    from untether.telegram.types import TelegramCallbackQuery
    from untether.transport_runtime import TransportRuntime

    observed_options: list[EngineRunOptions | None] = []
    destination_models: list[str | None] = []
    source_model = "topic-model" if topic else "chat-model"

    class _OptionsObservingRunner(_CompactRunner):
        async def compact(
            self, resume: ResumeToken, instructions: str | None = None
        ) -> AsyncIterator[UntetherEvent]:
            opts = get_run_options()
            observed_options.append(opts)
            yield EventFactory(self.engine).completed_ok(
                answer="summary", resume=resume
            )

    class _DestinationRunner(ScriptRunner):
        async def run(self, prompt, resume):
            opts = get_run_options()
            destination_models.append(opts.model if opts else None)
            async for event in super().run(prompt, resume):
                yield event

    runner = _OptionsObservingRunner([Return(answer="unused")], engine="codex")
    destination_runner = _DestinationRunner([Return(answer="continued")], engine="pi")
    runtime = TransportRuntime(
        router=AutoRouter(
            entries=[
                RunnerEntry(engine=runner.engine, runner=runner),
                RunnerEntry(engine="pi", runner=destination_runner),
            ],
            default_engine=runner.engine,
        ),
        projects=ProjectsConfig(projects={}, default_project=None),
        config_path=tmp_path / "untether.toml",
    )
    transport = FakeTransport()
    cfg = replace(
        make_cfg(transport, runner),
        allowed_user_ids=(123,),
        runtime=runtime,
        topics=TelegramTopicsSettings(enabled=topic, scope="main"),
    )
    prefs = ChatPrefsStore(resolve_prefs_path(tmp_path / "untether.toml"))
    await prefs.set_engine_override(
        123, "codex", EngineOverrides(model="chat-model", reasoning="low")
    )
    await prefs.set_engine_override(
        123, "pi", EngineOverrides(model="destination-model")
    )
    await prefs.set_plan_mode(123, True)
    await prefs.set_subagent(123, "not-for-summary")
    if topic:
        store = TopicStateStore(resolve_state_path(tmp_path / "untether.toml"))
        await store.set_engine_override(
            123, 77, "codex", EngineOverrides(model="topic-model", reasoning="low")
        )

    session_id = "model-override-compact-source"

    async def poller(_cfg: TelegramBridgeConfig) -> Any:
        yield replace(
            _compact_message(
                "/handoff to pi" if handoff else "/compact",
                message_id=2,
                reply_to_message_id=99,
                reply_to_text=f"done\n↩️ `codex resume {session_id}`",
            ),
            thread_id=77 if topic else None,
        )
        if handoff:
            with anyio.fail_after(5):
                while not any(
                    "reply_markup" in call["message"].extra
                    for call in transport.send_calls
                ):
                    await anyio.sleep(0.01)
            call = next(
                call
                for call in transport.send_calls
                if "reply_markup" in call["message"].extra
            )
            data = call["message"].extra["reply_markup"]["inline_keyboard"][0][0][
                "callback_data"
            ]
            yield TelegramCallbackQuery(
                transport="telegram",
                chat_id=123,
                message_id=call["ref"].message_id,
                callback_query_id="query",
                data=data,
                sender_id=123,
                raw={"message": {"message_thread_id": 77 if topic else None}},
            )
        with anyio.fail_after(5):
            while not observed_options:
                await anyio.sleep(0.01)
            if handoff:
                while not any(
                    "completed — new session started" in call["message"].text
                    for call in transport.edit_calls
                ):
                    await anyio.sleep(0.01)

    await run_main_loop(cfg, poller)

    assert observed_options == [EngineRunOptions(model=source_model, reasoning="low")]
    assert destination_models == (["destination-model"] if handoff else [])
    if handoff:
        assert any(
            "completed — new session started" in call["message"].text
            for call in transport.edit_calls
        )
