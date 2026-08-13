from __future__ import annotations

import pytest

from untether.runners.acp.auth import AcpAuth
from untether.runners.acp.interactions import (
    BrokerFull,
    InteractionBroker,
    InteractionOwnerError,
    InteractionUnknown,
)
from untether.runners.acp.turn import AcpTurnControl


@pytest.mark.anyio
async def test_broker_resolves_for_owner_and_rejects_other_owner() -> None:
    broker = InteractionBroker(max_pending=1, timeout_s=1)
    pending = await broker.open("chat:1", "permission", {"tool": "shell"})
    with pytest.raises(InteractionOwnerError):
        await broker.resolve("chat:2", pending.nonce, {"approved": True})
    assert await broker.resolve("chat:1", pending.nonce, {"approved": True}) is True
    assert await pending.wait() == {"approved": True}
    with pytest.raises(InteractionUnknown):
        await broker.resolve("chat:1", pending.nonce, {"approved": True})


@pytest.mark.anyio
async def test_broker_bounds_and_timeout_cancel() -> None:
    broker = InteractionBroker(max_pending=1, timeout_s=0.01)
    pending = await broker.open("owner", "form", {})
    with pytest.raises(BrokerFull):
        await broker.open("owner", "form", {})
    with pytest.raises(TimeoutError):
        await pending.wait()
    assert broker.pending_count == 0


@pytest.mark.anyio
async def test_broker_cancels_all_pending_interactions_for_owner() -> None:
    broker = InteractionBroker(timeout_s=1)
    first = await broker.open("session-1", "permission", {})
    second = await broker.open("session-1", "elicitation", {})
    other = await broker.open("session-2", "permission", {})

    assert await broker.cancel_owner("session-1") == 2
    assert broker.pending_count == 1
    with pytest.raises(RuntimeError, match="cancelled"):
        await first.wait()
    with pytest.raises(RuntimeError, match="cancelled"):
        await second.wait()
    assert await broker.resolve("session-2", other.nonce, {"ok": True})


@pytest.mark.anyio
async def test_turn_control_sends_cancel_notification_and_cannot_steer() -> None:
    sent: list[tuple[str, dict[str, object]]] = []

    async def notify(method: str, params: dict[str, object]) -> None:
        sent.append((method, params))

    control = AcpTurnControl(notify, "session-1")
    assert control.can_steer is False
    assert await control.interrupt() is True
    assert sent == [("session/cancel", {"sessionId": "session-1"})]


def test_auth_login_logout_and_retry_once() -> None:
    calls: list[str] = []
    auth = AcpAuth(method="browser")
    assert auth.login(lambda method: calls.append(method)) is True
    assert auth.logged_in
    assert auth.logout(lambda method: calls.append(method)) is True
    assert not auth.logged_in
    assert auth.retry_once(lambda: calls.append("retry")) is True
    assert auth.retry_once(lambda: calls.append("retry")) is False
    assert calls == ["browser", "logout", "retry"]
