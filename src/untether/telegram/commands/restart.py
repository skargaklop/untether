"""Command backend for graceful restart."""

from __future__ import annotations

from ...commands import CommandBackend, CommandContext, CommandResult
from ...shutdown import is_shutting_down, request_shutdown


class RestartCommand:
    """Gracefully drain active runs and restart Untether."""

    id = "restart"
    description = "Gracefully restart Untether"

    async def handle(self, ctx: CommandContext) -> CommandResult | None:
        if is_shutting_down():
            return CommandResult(
                text="Already restarting — waiting for active runs to finish.",
                notify=True,
            )

        # Telegram channel ids are normally integers; bridge typing also permits
        # string transports, which cannot identify a shutdown origin.
        origin_chat_id = ctx.message.channel_id
        request_shutdown(
            origin_chat_id=origin_chat_id if isinstance(origin_chat_id, int) else None
        )
        return CommandResult(
            text="Draining active runs… will restart shortly.",
            notify=True,
        )


BACKEND: CommandBackend = RestartCommand()
