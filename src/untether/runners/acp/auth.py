"""Minimal capability-gated ACP authentication helper."""

from __future__ import annotations

from collections.abc import Callable


class AcpAuth:
    def __init__(self, *, method: str) -> None:
        self.method = method
        self.logged_in = False
        self._retried = False

    def login(self, send: Callable[[str], None]) -> bool:
        send(self.method)
        self.logged_in = True
        return True

    def logout(self, send: Callable[[str], None]) -> bool:
        send("logout")
        self.logged_in = False
        return True

    def retry_once(self, retry: Callable[[], None]) -> bool:
        if self._retried:
            return False
        self._retried = True
        retry()
        return True
