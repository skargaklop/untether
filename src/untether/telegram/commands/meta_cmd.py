"""Meta-command handler for /plan, /goal, /subagent.

Classification follows Takopi's source-backed semantics:
- /plan with no args or on|off|clear|show → meta (sticky preference).
- /plan <prompt> → falls through to the loop as a normal prompt with /plan directive.
- /goal with no args → help.
- /goal <condition> → falls through as a normal prompt with /goal directive.
- /subagent with no args or show|off|clear|set <name> → meta.
- /subagent <name> <prompt> → falls through as a one-shot.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..files import split_command_args
from ..topics import _topic_key
from ..types import TelegramIncomingMessage
from .overrides import require_admin_or_private
from .reply import make_reply

if TYPE_CHECKING:
    from ..bridge import TelegramBridgeConfig
    from ..chat_prefs import ChatPrefsStore
    from ..topic_state import TopicStateStore

PLAN_USAGE = (
    "usage: `/plan`, `/plan on`, `/plan off`, `/plan clear`\n"
    "or start a plan-mode run: `/plan <prompt>` "
    "(e.g. `/plan /agy design the API`)"
)

GOAL_HELP = (
    "goal mode starts an autonomous loop until a condition is met "
    "(supported natively by Claude; best-effort on Grok).\n\n"
    "usage:\n"
    "`/goal all tests pass and lint is clean`\n"
    "`/claude /goal CHANGELOG has this week's PRs`\n\n"
    "tip: pair with unattended permissions (yolo / skip-permissions) so the "
    "loop is not blocked on tool approval."
)

SUBAGENT_USAGE = (
    "usage: `/subagent`, `/subagent set <name>`, `/subagent off`, or "
    "`/subagent clear`\n"
    "one-shot: `/codex /subagent <name> <prompt>` or "
    "`/codex --subagent <name> <prompt>`"
)

_STICKY_PLAN_ACTIONS = frozenset({"on", "off", "clear", "show"})
_STICKY_SUBAGENT_ACTIONS = frozenset({"set", "off", "clear", "show"})


async def handle_meta_command(
    cfg: TelegramBridgeConfig,
    msg: TelegramIncomingMessage,
    args_text: str,
    command_id: str,
    topic_store: TopicStateStore | None,
    chat_prefs: ChatPrefsStore | None,
    *,
    resolved_scope: str | None = None,
    scope_chat_ids: frozenset[int] | None = None,
    reply=None,
) -> None:
    """Dispatch /plan, /goal, /subagent meta forms.

    Free-form args (plan prompt, goal condition, subagent one-shot) are NOT
    handled here — they fall through to the loop's normal prompt path where
    the directive parser picks them up.
    """
    del resolved_scope  # signature compat
    _reply = reply or make_reply(cfg, msg)

    if command_id == "plan":
        await _handle_plan(
            cfg,
            msg,
            args_text,
            topic_store,
            chat_prefs,
            scope_chat_ids=scope_chat_ids,
            reply=_reply,
        )
    elif command_id == "goal":
        await _handle_goal(args_text, reply=_reply)
    elif command_id == "subagent":
        await _handle_subagent(
            cfg,
            msg,
            args_text,
            chat_prefs,
            reply=_reply,
        )


async def _handle_plan(
    cfg: TelegramBridgeConfig,
    msg: TelegramIncomingMessage,
    args_text: str,
    topic_store: TopicStateStore | None,
    chat_prefs: ChatPrefsStore | None,
    *,
    scope_chat_ids: frozenset[int] | None,
    reply,
) -> None:
    tokens = split_command_args(args_text)
    action = tokens[0].lower() if tokens else "show"

    # Non-sticky args = free-form prompt → not meta, show usage.
    if action not in _STICKY_PLAN_ACTIONS:
        await reply(text=PLAN_USAGE)
        return

    tkey = (
        _topic_key(msg, cfg, scope_chat_ids=scope_chat_ids)
        if topic_store is not None
        else None
    )

    if action == "show":
        value, source = await _resolve_sticky_plan(
            chat_id=msg.chat_id,
            tkey=tkey,
            topic_store=topic_store,
            chat_prefs=chat_prefs,
        )
        if value is None:
            await reply(text=f"plan mode: off (default)\nsource: {source}")
        else:
            state = "on" if value else "off"
            await reply(text=f"plan mode: {state}\nsource: {source}")
        return

    # on / off / clear — requires authorization.
    if not await require_admin_or_private(
        cfg,
        msg,
        missing_sender="cannot verify sender for plan mode.",
        failed_member="failed to verify plan mode permissions.",
        denied="changing plan mode is restricted to group admins.",
    ):
        return

    enabled: bool | None = None if action == "clear" else action == "on"

    if tkey is not None and topic_store is not None:
        await topic_store.set_plan_mode(tkey[0], tkey[1], enabled)
        if enabled is None:
            await reply(text="topic plan mode cleared (using chat/default).")
        else:
            await reply(text=f"topic plan mode set to `{'on' if enabled else 'off'}`.")
        return

    if chat_prefs is None:
        await reply(text="chat plan mode is unavailable (no config path).")
        return

    await chat_prefs.set_plan_mode(msg.chat_id, enabled)
    if enabled is None:
        await reply(text="chat plan mode cleared.")
    else:
        await reply(text=f"chat plan mode set to `{'on' if enabled else 'off'}`.")


async def _resolve_sticky_plan(
    *,
    chat_id: int,
    tkey: tuple[int, int] | None,
    topic_store: TopicStateStore | None,
    chat_prefs: ChatPrefsStore | None,
) -> tuple[bool | None, str]:
    if tkey is not None and topic_store is not None:
        topic_val = await topic_store.get_plan_mode(tkey[0], tkey[1])
        if topic_val is not None:
            return topic_val, "topic"
    if chat_prefs is not None:
        chat_val = await chat_prefs.get_plan_mode(chat_id)
        if chat_val is not None:
            return chat_val, "chat"
    return None, "default"


async def _handle_goal(args_text: str, *, reply) -> None:
    # Bare /goal = help. Free-form conditions fall through to the loop.
    if not (args_text or "").strip():
        await reply(text=GOAL_HELP)
        return
    # Non-empty goal condition should have been handled by the loop's
    # directive parser before reaching here. Show help as fallback.
    await reply(text=GOAL_HELP)


async def _handle_subagent(
    cfg: TelegramBridgeConfig,
    msg: TelegramIncomingMessage,
    args_text: str,
    chat_prefs: ChatPrefsStore | None,
    *,
    reply,
) -> None:
    tokens = split_command_args(args_text)
    head = tokens[0].lower() if tokens else "show"

    # Non-sticky args = one-shot subagent → not meta, show usage.
    is_sticky = (
        not tokens
        or (len(tokens) == 1 and head in {"off", "clear", "show"})
        or (len(tokens) == 2 and head == "set")
    )
    if not is_sticky:
        await reply(text=SUBAGENT_USAGE)
        return

    if head in {"show", ""}:
        if chat_prefs is None:
            await reply(text="subagent: unavailable (no config path).")
            return
        current = await chat_prefs.get_subagent(msg.chat_id)
        if current is None:
            await reply(text="subagent: none (default)")
        else:
            await reply(text=f"subagent: `{current}`")
        return

    # set / off / clear — requires authorization.
    if not await require_admin_or_private(
        cfg,
        msg,
        missing_sender="cannot verify sender for subagent preference.",
        failed_member="failed to verify subagent permissions.",
        denied="changing subagent is restricted to group admins.",
    ):
        return

    if chat_prefs is None:
        await reply(text="subagent is unavailable (no config path).")
        return

    if head == "set":
        name = next(iter(tokens[1:]), None)
        if name is None:
            await reply(text=SUBAGENT_USAGE)
            return
    else:  # off / clear
        name = None

    await chat_prefs.set_subagent(msg.chat_id, name)
    if name is None:
        await reply(text="subagent cleared.")
    else:
        await reply(text=f"subagent set to `{name}`.")
