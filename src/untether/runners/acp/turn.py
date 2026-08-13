"""ACP turn cancellation control."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any


class AcpTurnControl:
    can_steer = False

    def __init__(
        self, notify: Callable[[str, dict[str, Any]], Awaitable[None]], session_id: str
    ) -> None:
        self._notify = notify
        self._session_id = session_id

    async def steer(self, text: str) -> None:
        raise RuntimeError("ACP does not support mid-turn steering")

    async def interrupt(self) -> bool:
        await self._notify("session/cancel", {"sessionId": self._session_id})
        return True
