"""Msgspec models and decoder for Grok headless streaming-json output."""

from __future__ import annotations

from dataclasses import field
from typing import Any

import msgspec


class StreamTextEvent(
    msgspec.Struct, tag="text", tag_field="type", forbid_unknown_fields=False
):
    data: str = ""


class StreamThoughtEvent(
    msgspec.Struct, tag="thought", tag_field="type", forbid_unknown_fields=False
):
    data: str = ""


class StreamEndEvent(
    msgspec.Struct, tag="end", tag_field="type", forbid_unknown_fields=False
):
    stopReason: str | None = None
    sessionId: str | None = None
    requestId: str | None = None
    num_turns: int | None = None
    usage: dict[str, Any] | None = None
    modelUsage: dict[str, Any] | None = None
    total_cost_usd: float | None = None
    total_cost_usd_ticks: int | None = None
    cost_is_partial: bool | None = None
    usage_is_incomplete: bool | None = None


class StreamErrorEvent(
    msgspec.Struct, tag="error", tag_field="type", forbid_unknown_fields=False
):
    message: str = ""
    sessionId: str | None = None
    requestId: str | None = None
    num_turns: int | None = None
    usage: dict[str, Any] | None = None
    modelUsage: dict[str, Any] | None = None
    total_cost_usd: float | None = None
    usage_is_incomplete: bool | None = None


class StreamToolCallEvent(
    msgspec.Struct, tag="tool_call", tag_field="type", forbid_unknown_fields=False
):
    toolCallId: str = ""
    title: str | None = None
    kind: str | None = None
    status: str | None = None
    toolName: str | None = None
    rawInput: dict[str, Any] = field(default_factory=dict)
    content: list[Any] = field(default_factory=list)
    locations: list[Any] = field(default_factory=list)


class StreamToolCallUpdateEvent(
    msgspec.Struct,
    tag="tool_call_update",
    tag_field="type",
    forbid_unknown_fields=False,
):
    toolCallId: str = ""
    status: str | None = None
    content: list[Any] = field(default_factory=list)
    rawOutput: Any = None
    locations: list[Any] = field(default_factory=list)


class StreamUsageEvent(
    msgspec.Struct, tag="usage", tag_field="type", forbid_unknown_fields=False
):
    usage: dict[str, Any] = field(default_factory=dict)


class StreamAvailableCommandsEvent(
    msgspec.Struct,
    tag="available_commands",
    tag_field="type",
    forbid_unknown_fields=False,
):
    tools: list[str] = field(default_factory=list)
    commands: list[str] = field(default_factory=list)


class StreamUnknownEvent(msgspec.Struct, forbid_unknown_fields=False):
    """Catch-all for unrecognized event types (forward compatibility).

    Decoded by ``decode_event`` when the ``type`` field does not match any
    known struct. The runner logs it at DEBUG — never WARNING — to avoid
    spam on new CLI event types.
    """

    type_name: str = ""


type GrokEvent = (
    StreamTextEvent
    | StreamThoughtEvent
    | StreamEndEvent
    | StreamErrorEvent
    | StreamToolCallEvent
    | StreamToolCallUpdateEvent
    | StreamUsageEvent
    | StreamAvailableCommandsEvent
    | StreamUnknownEvent
)

# Only tagged structs participate in the msgspec tagged-union decoder.
# ``StreamUnknownEvent`` is returned directly by ``decode_event`` for
# unrecognized types — it cannot be part of the tagged union (no ``type``
# field to dispatch on).
_TAGGED_EVENT = (
    StreamTextEvent
    | StreamThoughtEvent
    | StreamEndEvent
    | StreamErrorEvent
    | StreamToolCallEvent
    | StreamToolCallUpdateEvent
    | StreamUsageEvent
    | StreamAvailableCommandsEvent
)

_KNOWN_TYPES: frozenset[str] = frozenset(
    {
        "text",
        "thought",
        "end",
        "error",
        "tool_call",
        "tool_call_update",
        "usage",
        "available_commands",
    }
)

_DECODER = msgspec.json.Decoder(_TAGGED_EVENT)
_PEEK_DECODER = msgspec.json.Decoder(dict[str, Any])


def decode_event(line: str | bytes) -> GrokEvent:
    """Decode a JSONL line into a known event struct or ``StreamUnknownEvent``.

    Unknown-but-valid ``type`` values are tolerated (forward compatibility):
    they become ``StreamUnknownEvent`` instead of raising ``ValidationError``.
    Only genuinely malformed JSON raises.
    """
    obj = _PEEK_DECODER.decode(line)
    type_name = obj.get("type") if isinstance(obj, dict) else None
    if type_name in _KNOWN_TYPES:
        return _DECODER.decode(line)
    return StreamUnknownEvent(type_name=type_name or "")
