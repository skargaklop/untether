"""Pure decision helpers for combining Telegram text messages into one prompt.

This module only decides *whether* consecutive text messages are one input and
*how* to join their text. It does not know about directives, engines, sessions,
triggers, or queueing - the existing dispatcher handles those on the assembled
prompt.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .commands.parse import _parse_slash_command
from .files import split_command_args

type PromptBatchSeparator = Literal["newline", "blank_line"]

# Pure control commands never join chunks: each one keeps its own behavior.
# Includes both Untether-native commands (new, ctx, agent, model, reasoning,
# listen, topic, file, cancel, restart, verbose, stats, threads, health, ping,
# config, export, browse, planmode, auth, at) and Takopi-style aliases
# (trigger, compact, handoff) for forward compatibility.
CONTROL_COMMANDS = frozenset(
    {
        "cancel",
        "new",
        "continue",
        "ctx",
        "agent",
        "model",
        "reasoning",
        "listen",
        "trigger",
        "queue",
        "file",
        "topic",
        "compact",
        "handoff",
        "restart",
        "verbose",
        "stats",
        "threads",
        "health",
        "ping",
        "config",
        "export",
        "browse",
        "planmode",
        "auth",
        "at",
    }
)

_STICKY_PLAN_ACTIONS = frozenset({"on", "off", "clear", "show"})


def is_sticky_plan_args(args_text: str) -> bool:
    """True when /plan is the sticky preference command, not a plan-mode prompt."""
    tokens = split_command_args(args_text)
    if not tokens:
        return True
    return len(tokens) == 1 and tokens[0].lower() in _STICKY_PLAN_ACTIONS


def is_sticky_goal_args(args_text: str) -> bool:
    """True when /goal is help-only (no condition); free-form starts a goal run."""
    return not (args_text or "").strip()


@dataclass(frozen=True, slots=True)
class PromptBatchSettings:
    enabled: bool
    max_messages: int = 8
    max_chars: int = 120_000
    separator: PromptBatchSeparator = "blank_line"


@dataclass(frozen=True, slots=True)
class PromptBatchPart:
    message_id: int
    text: str


def should_batch_text(text: str, *, settings: PromptBatchSettings) -> bool:
    """True when a single text message may join a multi-message prompt batch."""
    if not settings.enabled:
        return False
    if not text.strip():
        return False

    command_id, args_text = _parse_slash_command(text)
    if command_id is None:
        return True
    if command_id in CONTROL_COMMANDS:
        return False
    if command_id == "plan":
        return not is_sticky_plan_args(args_text)
    if command_id == "goal":
        return not is_sticky_goal_args(args_text)

    # Engine directives, project aliases, and plugin commands with text are
    # interpreted after batching by the existing dispatcher.
    return bool(args_text.strip())


def join_prompt_parts(
    parts: list[PromptBatchPart],
    *,
    separator: PromptBatchSeparator,
) -> str:
    sep = "\n" if separator == "newline" else "\n\n"
    ordered = sorted(parts, key=lambda part: part.message_id)
    return sep.join(part.text for part in ordered)
