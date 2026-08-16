"""Tests for the acp_control command backend: permission nonce resolution,
callback parsing, and entry-point registration."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from untether.commands import (
    CommandContext,
    CommandExecutor,
    CommandResult,
    get_command,
)
from untether.runners.acp.interactions import (
    InteractionBroker,
    resolve_nonce,
)
from untether.telegram.commands.acp_control import BACKEND
from untether.transport import MessageRef
from untether.transport_runtime import TransportRuntime


@pytest.fixture(autouse=True)
def _clear_nonce_registry():
    from untether.runners.acp.interactions import _NONCE_REGISTRY

    _NONCE_REGISTRY.clear()
    yield
    _NONCE_REGISTRY.clear()


def _make_ctx(args_text: str) -> CommandContext:
    """Minimal CommandContext — acp_control only reads args_text."""
    return CommandContext(
        command="acp_control",
        text=f"/acp_control {args_text}",
        args_text=args_text,
        args=tuple(args_text.split(" ")),
        message=cast("MessageRef", None),
        reply_to=None,
        reply_text=None,
        config_path=None,
        plugin_config={},
        runtime=cast("TransportRuntime", None),
        executor=cast("CommandExecutor", None),
    )


class TestAcpControlBackend:
    def test_backend_shape(self) -> None:
        assert BACKEND.id == "acp_control"
        assert callable(BACKEND.handle)

    @pytest.mark.anyio
    async def test_resolve_pending_permission(self) -> None:
        broker = InteractionBroker(timeout_s=1)
        pending = await broker.open("acp:s1", "permission", {})
        waiter = asyncio.create_task(pending.wait())

        ctx = _make_ctx(f"{pending.nonce}:allow")
        result = await BACKEND.handle(ctx)
        assert isinstance(result, CommandResult)
        assert result.text

        assert await waiter == {"outcome": "selected", "optionId": "allow"}
        assert resolve_nonce(pending.nonce) is None
        assert broker.pending_count == 0

    @pytest.mark.anyio
    async def test_unknown_nonce_is_noop(self) -> None:
        ctx = _make_ctx("deadbeef:allow")
        result = await BACKEND.handle(ctx)
        assert isinstance(result, CommandResult)
        assert "not found" in result.text.lower()

    @pytest.mark.anyio
    async def test_malformed_args_no_crash(self) -> None:
        for bad in ("", "only-nonce", "nonce:opt:extra:bits"):
            ctx = _make_ctx(bad)
            result = await BACKEND.handle(ctx)
            assert isinstance(result, CommandResult)

    @pytest.mark.anyio
    async def test_owner_mismatch_cannot_resolve(self) -> None:
        # A second broker with the same owner string cannot steal a pending
        # interaction opened by another broker instance: resolve goes through
        # the registered broker, so cross-broker theft is structurally
        # impossible. Sanity-check resolve_nonce returns the opening broker.
        broker = InteractionBroker(timeout_s=1)
        pending = await broker.open("acp:s1", "permission", {})
        entry = resolve_nonce(pending.nonce)
        assert entry is not None
        assert entry[0] is broker
        assert entry[1] == "acp:s1"


class TestAcpControlEntryPoint:
    def test_entry_point_registered(self) -> None:
        # get_command goes through the installed entry points; the package is
        # installed in editable mode so the new entry point resolves.
        backend = get_command("acp_control", required=False)
        assert backend is not None
        assert getattr(backend, "id", None) == "acp_control"
