"""Quick-action Telegram inline menu."""

from __future__ import annotations

from ...commands import CommandBackend, CommandContext, CommandResult
from ...transport import RenderedMessage
from ..bridge import CANCEL_CALLBACK_DATA

MENU_ACTION_COMMANDS: dict[str, str] = {
    "new": "/new",
    "config": "/config",
    "model": "/model",
    "agent": "/agent",
    "topic": "/topic",
    "stats": "/stats",
    "engines": "/config ag",
    "compact": "/compact",
    "queue": "/queue",
    "health": "/health",
}

_MENU_ROWS = (
    (("New", "menu:new"), ("Settings", "menu:config")),
    (("Model", "menu:model"), ("Agent", "menu:agent")),
    (("Topic", "menu:topic"), ("Stats", "menu:stats")),
    (("Engines", "menu:engines"), ("Compact", "menu:compact")),
    (("Queue", "menu:queue"), ("Health", "menu:health")),
    (("Cancel", CANCEL_CALLBACK_DATA),),
)


def menu_command_text(data: str) -> str | None:
    """Return the fixed slash command represented by a menu callback."""
    prefix, separator, action = data.partition(":")
    if prefix != "menu" or separator != ":" or ":" in action:
        return None
    return MENU_ACTION_COMMANDS.get(action)


class MenuCommand:
    """Show shortcuts that replay existing Telegram commands."""

    id = "menu"
    description = "Show quick action buttons"

    async def handle(self, ctx: CommandContext) -> CommandResult | None:
        buttons = [
            [{"text": text, "callback_data": callback} for text, callback in row]
            for row in _MENU_ROWS
        ]
        message = RenderedMessage(
            text="Quick actions",
            extra={"reply_markup": {"inline_keyboard": buttons}},
        )
        await ctx.executor.send(message, reply_to=ctx.message, notify=True)
        return None


BACKEND: CommandBackend = MenuCommand()
