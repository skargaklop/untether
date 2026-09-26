from __future__ import annotations

import pytest

from tests.telegram_fakes import FakeTransport
from untether.telegram.bridge import send_plain


@pytest.mark.anyio
async def test_send_plain_short_text_single_message() -> None:
    transport = FakeTransport()
    await send_plain(
        transport,
        chat_id=123,
        user_msg_id=1,
        text="hello\nexit: 0",
    )
    assert len(transport.send_calls) == 1
    call = transport.send_calls[0]
    assert "hello" in call["message"].text
    assert not call["message"].extra.get("followups")


@pytest.mark.anyio
async def test_send_plain_splits_long_text_like_general_answers() -> None:
    """Long plain replies must split into followups like engine answers.

    The general answer path splits bodies >3500 chars into multiple
    Telegram messages (prepare_telegram_multi). A plain reply path used
    by /bash and /powershell must not silently truncate instead.
    """
    transport = FakeTransport()
    long_output = "\n\n".join(f"line {i:03d} " + "x" * 60 for i in range(200))
    await send_plain(
        transport,
        chat_id=123,
        user_msg_id=1,
        text=long_output,
    )
    assert len(transport.send_calls) == 1
    call = transport.send_calls[0]
    followups = call["message"].extra.get("followups")
    assert followups, "expected long plain reply to be split into followups"
    total_len = len(call["message"].text) + sum(len(f.text) for f in followups)
    assert total_len >= len(long_output) - 100, (
        "content must be preserved, not truncated"
    )
