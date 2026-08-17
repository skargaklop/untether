"""Regression tests for the Telegram quick-action menu."""

from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.telegram_fakes import FakeBot, FakeTransport, make_cfg
from untether.telegram.bridge import TelegramBridgeConfig, run_main_loop
from untether.telegram.commands.menu_panel import (
    BACKEND,
    MENU_ACTION_COMMANDS,
    menu_command_text,
)
from untether.telegram.types import TelegramCallbackQuery


def _context() -> MagicMock:
    ctx = MagicMock()
    ctx.executor = AsyncMock()
    ctx.executor.send = AsyncMock(return_value=None)
    ctx.message = MagicMock()
    return ctx


@pytest.mark.anyio
async def test_menu_renders_full_width_single_button_rows() -> None:
    """Every action occupies a full Telegram keyboard row."""
    ctx = _context()

    result = await BACKEND.handle(ctx)

    assert result is None
    message = ctx.executor.send.call_args.args[0]
    assert message.text == "Quick actions"
    assert message.extra["reply_markup"]["inline_keyboard"] == [
        [{"text": "New", "callback_data": "menu:new"}],
        [{"text": "Settings", "callback_data": "menu:config"}],
        [{"text": "Model", "callback_data": "menu:model"}],
        [{"text": "Topic", "callback_data": "menu:topic"}],
        [{"text": "Stats", "callback_data": "menu:stats"}],
        [{"text": "Engines", "callback_data": "menu:engines"}],
        [{"text": "Compact", "callback_data": "menu:compact"}],
        [{"text": "Cancel", "callback_data": "untether:cancel"}],
    ]


def test_menu_callback_mapping_is_closed() -> None:
    """Only fixed menu callbacks can become slash-command text."""
    assert MENU_ACTION_COMMANDS == {
        "new": "/new",
        "config": "/config",
        "model": "/model",
        "topic": "/topic",
        "stats": "/stats",
        "engines": "/config ag",
        "compact": "/compact",
    }
    assert menu_command_text("menu:model") == "/model"
    assert menu_command_text("menu:unknown") is None
    assert menu_command_text("menu:") is None
    assert menu_command_text("menu:model:injected") is None
    assert menu_command_text("config:ag") is None


@pytest.mark.anyio
async def test_menu_model_callback_replays_existing_model_command() -> None:
    """A menu callback reaches the ordinary /model handler."""
    transport = FakeTransport()
    cfg = make_cfg(transport)
    bot = cast(FakeBot, cfg.bot)

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramCallbackQuery(
            transport="telegram",
            chat_id=123,
            message_id=42,
            callback_query_id="menu-model",
            data="menu:model",
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert bot.callback_calls == [
        {"callback_query_id": "menu-model", "text": None, "show_alert": None}
    ]
    assert any(
        "available engines:" in call["message"].text for call in transport.send_calls
    )


@pytest.mark.anyio
async def test_menu_topic_callback_replays_existing_topic_validation() -> None:
    """A menu callback preserves the existing disabled-topic response."""
    transport = FakeTransport()
    cfg = make_cfg(transport)

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramCallbackQuery(
            transport="telegram",
            chat_id=123,
            message_id=42,
            callback_query_id="menu-topic",
            data="menu:topic",
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert any(
        "topics are not enabled" in call["message"].text
        for call in transport.send_calls
    )


@pytest.mark.anyio
async def test_unknown_menu_callback_is_answered_without_execution() -> None:
    """Malformed menu payloads clear Telegram's spinner but do nothing else."""
    transport = FakeTransport()
    cfg = make_cfg(transport)
    bot = cast(FakeBot, cfg.bot)

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramCallbackQuery(
            transport="telegram",
            chat_id=123,
            message_id=42,
            callback_query_id="menu-invalid",
            data="menu:unknown",
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert bot.callback_calls == [
        {"callback_query_id": "menu-invalid", "text": None, "show_alert": None}
    ]
    assert transport.send_calls == []
