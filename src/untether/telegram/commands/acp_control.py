"""ACP permission control channel: resolves pending ACP permission
interactions from Telegram inline-keyboard callbacks.

Callback data format: ``acp_control:{nonce}:{optionId}``. The nonce is looked
up in the module-level nonce registry (populated by ``InteractionBroker.open``)
and resolved with the wire-conformant outcome dict
``{"outcome": "selected", "optionId": ...}``.
"""

from __future__ import annotations

from structlog import get_logger

from ...commands import CommandBackend, CommandContext, CommandResult
from ...runners.acp.interactions import resolve_nonce

logger = get_logger(__name__)


class AcpControlCommand:
    """Command backend for ACP permission approval/denial."""

    id = "acp_control"
    description = "Handle ACP agent permission requests"

    async def handle(self, ctx: CommandContext) -> CommandResult | None:
        """Handle callback from permission inline-keyboard buttons."""
        args = ctx.args_text
        nonce, _, option_id = args.rpartition(":")
        if not nonce or not option_id:
            logger.warning("acp_control.malformed", args=args)
            return CommandResult(
                text="⚠️ Malformed permission callback",
                notify=True,
            )
        entry = resolve_nonce(nonce)
        if entry is None:
            logger.info("acp_control.unknown_nonce", nonce=nonce)
            return CommandResult(
                text="⚠️ Permission request not found or already answered",
                notify=True,
            )
        broker, owner = entry
        resolved = await broker.resolve(
            owner, nonce, {"outcome": "selected", "optionId": option_id}
        )
        logger.info("acp_control.resolved", nonce=nonce, option_id=option_id)
        if not resolved:
            return CommandResult(
                text="⚠️ Permission request not found or already answered",
                notify=True,
            )
        return CommandResult(
            text=f"✅ Selected: {option_id}",
            notify=True,
        )


BACKEND: CommandBackend = AcpControlCommand()
