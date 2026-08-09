"""Msgspec models and decoder for pi --mode json output."""

from __future__ import annotations

from typing import Any

import msgspec


class _Event(msgspec.Struct, tag_field="type", forbid_unknown_fields=False):
    pass


class SessionHeader(_Event, tag="session"):
    id: str | None = None
    version: int | None = None
    timestamp: str | None = None
    cwd: str | None = None
    parentSession: str | None = None


class AgentStart(_Event, tag="agent_start"):
    pass


class AgentEnd(_Event, tag="agent_end"):
    messages: list[dict[str, Any]]


class MessageEnd(_Event, tag="message_end"):
    message: dict[str, Any]


class MessageStart(_Event, tag="message_start"):
    message: dict[str, Any] | None = None


class MessageUpdate(_Event, tag="message_update"):
    message: dict[str, Any] | None = None
    assistantMessageEvent: dict[str, Any] | None = None


class TurnStart(_Event, tag="turn_start"):
    pass


class TurnEnd(_Event, tag="turn_end"):
    message: dict[str, Any] | None = None
    toolResults: list[dict[str, Any]] | None = None


class ToolExecutionStart(_Event, tag="tool_execution_start"):
    toolCallId: str
    toolName: str | None = None
    args: dict[str, Any] = msgspec.field(default_factory=dict)


class ToolExecutionUpdate(_Event, tag="tool_execution_update"):
    toolCallId: str | None = None
    toolName: str | None = None
    args: dict[str, Any] = msgspec.field(default_factory=dict)
    partialResult: Any = None


class ToolExecutionEnd(_Event, tag="tool_execution_end"):
    toolCallId: str
    toolName: str | None = None
    result: Any = None
    isError: bool = False


class AutoCompactionStart(_Event, tag="auto_compaction_start"):
    reason: str | None = None


class AutoCompactionEnd(_Event, tag="auto_compaction_end"):
    result: dict[str, Any] | None = None
    aborted: bool | None = None
    willRetry: bool | None = None


class AutoRetryStart(_Event, tag="auto_retry_start"):
    attempt: int | None = None
    maxAttempts: int | None = None
    delayMs: int | float | None = None
    errorMessage: str | None = None


class AutoRetryEnd(_Event, tag="auto_retry_end"):
    success: bool | None = None
    attempt: int | None = None
    finalError: str | None = None


class PiUnknownEvent(msgspec.Struct, forbid_unknown_fields=False):
    """Catch-all for unrecognized event types (forward compatibility).

    Decoded by :func:`decode_event` when the ``type`` field does not match any
    known struct. The runner logs it at DEBUG — never WARNING — to avoid spam
    on new CLI event types such as ``notice`` or future additions.
    """

    type_name: str = ""


type PiEvent = (
    SessionHeader
    | AgentStart
    | AgentEnd
    | MessageStart
    | MessageUpdate
    | MessageEnd
    | TurnStart
    | TurnEnd
    | ToolExecutionStart
    | ToolExecutionUpdate
    | ToolExecutionEnd
    | AutoCompactionStart
    | AutoCompactionEnd
    | AutoRetryStart
    | AutoRetryEnd
    | PiUnknownEvent
)

# Only tagged structs participate in the msgspec tagged-union decoder.
# ``PiUnknownEvent`` is returned directly by ``decode_event`` for unrecognized
# types — it cannot be part of the tagged union (no ``type`` field).
_TAGGED_EVENT = (
    SessionHeader
    | AgentStart
    | AgentEnd
    | MessageStart
    | MessageUpdate
    | MessageEnd
    | TurnStart
    | TurnEnd
    | ToolExecutionStart
    | ToolExecutionUpdate
    | ToolExecutionEnd
    | AutoCompactionStart
    | AutoCompactionEnd
    | AutoRetryStart
    | AutoRetryEnd
)

_KNOWN_TYPES: frozenset[str] = frozenset(
    {
        "session",
        "agent_start",
        "agent_end",
        "message_start",
        "message_update",
        "message_end",
        "turn_start",
        "turn_end",
        "tool_execution_start",
        "tool_execution_update",
        "tool_execution_end",
        "auto_compaction_start",
        "auto_compaction_end",
        "auto_retry_start",
        "auto_retry_end",
    }
)

_DECODER = msgspec.json.Decoder(_TAGGED_EVENT)
_PEEK_DECODER = msgspec.json.Decoder(dict[str, Any])


def decode_event(line: str | bytes) -> PiEvent:
    """Decode a JSONL line into a known event struct or :class:`PiUnknownEvent`.

    Unknown-but-valid ``type`` values are tolerated (forward compatibility):
    they become ``PiUnknownEvent`` instead of raising ``ValidationError``.
    Missing/non-string ``type`` or a malformed known tag still raises the
    appropriate msgspec error so the runner can log and skip just that line.
    """
    obj = _PEEK_DECODER.decode(line)
    type_name = obj.get("type") if isinstance(obj, dict) else None
    if type_name in _KNOWN_TYPES:
        return _DECODER.decode(line)
    if isinstance(type_name, str):
        return PiUnknownEvent(type_name=type_name)
    # Absent or non-string type: let the strict decoder emit the structured
    # validation failure so the runner logs ``jsonl.msgspec.invalid``.
    return _DECODER.decode(line)
