"""Integration tests for prompt batching through the Telegram loop.

These exercise the PromptInputBatcher wired into run_main_loop: consecutive
text messages from the same sender in the same chat are joined into one
prompt, control commands interrupt the batch, and the assembled prompt is
dispatched as one run.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace

import anyio
import pytest

from untether.runners.mock import Return, ScriptRunner
from untether.telegram.bridge import TelegramBridgeConfig, run_main_loop
from untether.telegram.types import TelegramIncomingMessage

from .telegram_fakes import FakeTransport, make_cfg

CODEX_ENGINE = "codex"


def _msg(
    message_id: int,
    text: str,
    *,
    sender_id: int | None = 123,
    chat_id: int = 123,
    reply_to: int | None = None,
    reply_to_text: str | None = None,
) -> TelegramIncomingMessage:
    return TelegramIncomingMessage(
        transport="telegram",
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        reply_to_message_id=reply_to,
        reply_to_text=reply_to_text,
        sender_id=sender_id,
    )


def _make_poller(
    messages: list[TelegramIncomingMessage],
    *,
    tail_delay: float = 0.2,
) -> tuple[list[TelegramIncomingMessage], float]:
    """Return (messages, tail_delay) for test customisation."""
    return messages, tail_delay


def _poller_factory(
    messages: list[TelegramIncomingMessage], *, tail_delay: float = 0.2
):
    async def poller(
        _cfg: TelegramBridgeConfig,
    ) -> AsyncIterator[TelegramIncomingMessage]:
        for msg in messages:
            yield msg
            await anyio.sleep(0.01)
        # Allow the debounce window to expire so the batch flushes
        # before run_main_loop exits.
        await anyio.sleep(tail_delay)

    return poller


@pytest.mark.anyio
async def test_prompt_batch_joins_consecutive_messages() -> None:
    """Two consecutive text messages from the same sender are joined."""
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.05,
    )

    messages = [
        _msg(1, "first part"),
        _msg(2, "second part"),
    ]
    poller = _poller_factory(messages)

    await run_main_loop(cfg, poller)

    assert len(runner.calls) == 1
    prompt = runner.calls[0][0]
    assert "first part" in prompt
    assert "second part" in prompt
    # Blank-line separator
    assert "\n\n" in prompt


@pytest.mark.anyio
async def test_prompt_batch_disabled_dispatches_separately() -> None:
    """When debounce=0, each message dispatches independently."""
    runner = ScriptRunner(
        [Return(answer="ok"), Return(answer="ok")],
        engine=CODEX_ENGINE,
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.0,
    )

    messages = [
        _msg(1, "first"),
        _msg(2, "second"),
    ]
    poller = _poller_factory(messages)

    await run_main_loop(cfg, poller)

    assert len(runner.calls) == 2


@pytest.mark.anyio
async def test_cancel_interrupts_pending_batch() -> None:
    """A /cancel command flushes/drops the pending batch and cancels."""
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.05,
    )

    messages = [
        _msg(1, "text that will be batched"),
        _msg(2, "/cancel"),
    ]
    poller = _poller_factory(messages)

    await run_main_loop(cfg, poller)

    # The batched text should not have been dispatched as a run
    # (cancel flushes the batch and the cancel handler runs instead)
    assert len(runner.calls) == 0


@pytest.mark.anyio
async def test_new_command_interrupts_batch() -> None:
    """/new cancels any pending batch before starting a new thread."""
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.05,
    )

    messages = [
        _msg(1, "batched text"),
        _msg(2, "/new"),
    ]
    poller = _poller_factory(messages)

    await run_main_loop(cfg, poller)

    # The batched text should not have reached the runner
    assert len(runner.calls) == 0


@pytest.mark.anyio
async def test_control_command_not_batched() -> None:
    """Control commands like /ping are never batched."""
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.05,
    )

    messages = [
        _msg(1, "/ping"),
    ]
    poller = _poller_factory(messages)

    await run_main_loop(cfg, poller)

    # /ping is a control command — it should not start a run
    assert len(runner.calls) == 0


@pytest.mark.anyio
async def test_max_messages_flushes_early() -> None:
    """When max_messages is reached, the batch flushes immediately."""
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=10.0,  # long debounce so only max_messages triggers
    )
    # Override max_messages via the cfg
    cfg = replace(cfg, prompt_batch_max_messages=3)

    messages = [
        _msg(1, "one"),
        _msg(2, "two"),
        _msg(3, "three"),
    ]
    poller = _poller_factory(messages, tail_delay=0.2)

    await run_main_loop(cfg, poller)

    # The batch should have flushed at 3 messages despite the long debounce
    assert len(runner.calls) == 1
    prompt = runner.calls[0][0]
    assert "one" in prompt
    assert "two" in prompt
    assert "three" in prompt


@pytest.mark.anyio
async def test_different_senders_not_batched() -> None:
    """Messages from different senders are batched separately."""
    runner = ScriptRunner(
        [Return(answer="ok"), Return(answer="ok")],
        engine=CODEX_ENGINE,
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123, 456),
        prompt_batch_debounce_s=0.05,
    )

    messages = [
        _msg(1, "from alice", sender_id=123),
        _msg(2, "from bob", sender_id=456),
    ]
    poller = _poller_factory(messages)

    await run_main_loop(cfg, poller)

    # Two separate runs — different senders don't share a batch
    assert len(runner.calls) == 2


@pytest.mark.anyio
async def test_different_reply_targets_do_not_share_a_batch() -> None:
    """Reply-scoped prompts retain distinct resume targets."""
    runner = ScriptRunner(
        [Return(answer="ok"), Return(answer="ok")], engine=CODEX_ENGINE
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.05,
    )

    await run_main_loop(
        cfg,
        _poller_factory(
            [
                _msg(1, "first", reply_to=10),
                _msg(2, "second", reply_to=20),
            ]
        ),
    )

    assert [
        call[0].endswith(text)
        for call, text in zip(runner.calls, ["first", "second"], strict=True)
    ] == [True, True]


@pytest.mark.anyio
async def test_missing_sender_is_not_batched() -> None:
    """Messages without a sender cannot safely share a user batch."""
    runner = ScriptRunner(
        [Return(answer="ok"), Return(answer="ok")], engine=CODEX_ENGINE
    )
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.05,
    )

    await run_main_loop(
        cfg,
        _poller_factory(
            [_msg(1, "first", sender_id=None), _msg(2, "second", sender_id=None)]
        ),
    )

    assert runner.calls == []


@pytest.mark.anyio
async def test_handoff_command_cancels_a_pending_prompt_batch() -> None:
    """Handoff is a control operation and never allows earlier prose to run."""
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    cfg = replace(
        make_cfg(FakeTransport(), runner),
        allowed_user_ids=(123,),
        prompt_batch_debounce_s=0.05,
    )

    await run_main_loop(
        cfg,
        _poller_factory([_msg(1, "queued prose"), _msg(2, "/handoff")]),
    )

    assert runner.calls == []
