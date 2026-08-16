"""Bounded, owner-scoped broker for ACP reverse interactions."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import anyio


class InteractionUnknown(RuntimeError):
    pass


class InteractionOwnerError(RuntimeError):
    pass


class BrokerFull(RuntimeError):
    pass


@dataclass(slots=True)
class PendingInteraction:
    nonce: str
    owner: str
    kind: str
    payload: dict[str, Any]
    _event: anyio.Event
    _result: Any = None
    _error: BaseException | None = None
    timeout_s: float = 600.0
    broker: Any = None

    async def wait(self) -> Any:
        try:
            with anyio.fail_after(self.timeout_s):
                await self._event.wait()
        except TimeoutError:
            await self.broker.cancel(
                self.owner, self.nonce, TimeoutError("interaction timed out")
            )
            raise
        if self._error is not None:
            raise self._error
        return self._result


class InteractionBroker:
    def __init__(self, *, max_pending: int = 64, timeout_s: float = 600.0) -> None:
        if max_pending < 1 or timeout_s <= 0:
            raise ValueError("broker bounds must be positive")
        self.max_pending = max_pending
        self.timeout_s = timeout_s
        self._pending: dict[str, PendingInteraction] = {}
        self._lock = anyio.Lock()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    async def open(
        self, owner: str, kind: str, payload: dict[str, Any]
    ) -> PendingInteraction:
        async with self._lock:
            if len(self._pending) >= self.max_pending:
                raise BrokerFull("interaction broker is full")
            item = PendingInteraction(
                uuid.uuid4().hex,
                owner,
                kind,
                payload,
                anyio.Event(),
                timeout_s=self.timeout_s,
                broker=self,
            )
            self._pending[item.nonce] = item
            publish(item)
        return item

    async def resolve(self, owner: str, nonce: str, result: Any) -> bool:
        item = await self._take(owner, nonce)
        item._result = result
        item._event.set()
        return True

    async def cancel(
        self, owner: str, nonce: str, error: BaseException | None = None
    ) -> bool:
        item = await self._take(owner, nonce)
        item._error = error or TimeoutError("interaction cancelled")
        item._event.set()
        return True

    async def cancel_owner(self, owner: str, error: BaseException | None = None) -> int:
        async with self._lock:
            items = self._pop_owner(owner)
        for item in items:
            unpublish(item.nonce)
            item._error = error or RuntimeError("interaction cancelled")
            item._event.set()
        return len(items)

    def cancel_owner_nowait(
        self, owner: str, error: BaseException | None = None
    ) -> int:
        """Synchronous cancel_owner for teardown paths that must not suspend
        (async-generator aclose / GeneratorExit handling)."""
        items = self._pop_owner(owner)
        for item in items:
            unpublish(item.nonce)
            item._error = error or RuntimeError("interaction cancelled")
            item._event.set()
        return len(items)

    def _pop_owner(self, owner: str) -> list[PendingInteraction]:
        return [
            self._pending.pop(nonce)
            for nonce, item in list(self._pending.items())
            if item.owner == owner
        ]

    async def _take(self, owner: str, nonce: str) -> PendingInteraction:
        async with self._lock:
            item = self._pending.get(nonce)
            if item is None:
                raise InteractionUnknown("unknown or expired interaction")
            if item.owner != owner:
                raise InteractionOwnerError("interaction owner mismatch")
            unpublish(nonce)
            return self._pending.pop(nonce)


# Module-level nonce registry: maps every pending interaction's nonce to its
# broker and owner so Telegram callbacks (acp_control) can resolve an
# interaction without holding a runner reference. Populated only by
# InteractionBroker.open; depopulated on every exit path (resolve / cancel /
# cancel_owner / timeout via wait()'s broker.cancel).
_NONCE_REGISTRY: dict[str, tuple[InteractionBroker, str]] = {}


def publish(pending: PendingInteraction) -> None:
    if pending.broker is None:
        raise ValueError("pending interaction has no broker")
    _NONCE_REGISTRY[pending.nonce] = (pending.broker, pending.owner)


def unpublish(nonce: str) -> None:
    _NONCE_REGISTRY.pop(nonce, None)


def resolve_nonce(nonce: str) -> tuple[InteractionBroker, str] | None:
    """Return (broker, owner) for a live interaction nonce, else None."""
    return _NONCE_REGISTRY.get(nonce)
