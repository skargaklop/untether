"""Tests for the Grok schema decoder and event structs."""

from __future__ import annotations

import msgspec
import pytest

from untether.schemas.grok import (
    StreamEndEvent,
    StreamErrorEvent,
    StreamTextEvent,
    StreamThoughtEvent,
    StreamToolCallEvent,
    StreamUnknownEvent,
    decode_event,
)


class TestDecodeEvent:
    def test_text_event(self) -> None:
        event = decode_event('{"type": "text", "data": "hello"}')
        assert isinstance(event, StreamTextEvent)
        assert event.data == "hello"

    def test_thought_event(self) -> None:
        event = decode_event('{"type": "thought", "data": "thinking"}')
        assert isinstance(event, StreamThoughtEvent)
        assert event.data == "thinking"

    def test_end_event(self) -> None:
        event = decode_event('{"type": "end", "stopReason": "end_turn"}')
        assert isinstance(event, StreamEndEvent)
        assert event.stopReason == "end_turn"

    def test_error_event(self) -> None:
        event = decode_event('{"type": "error", "message": "failed"}')
        assert isinstance(event, StreamErrorEvent)
        assert event.message == "failed"

    def test_tool_call_event(self) -> None:
        event = decode_event(
            '{"type": "tool_call", "toolCallId": "tc1", "toolName": "read_file"}'
        )
        assert isinstance(event, StreamToolCallEvent)
        assert event.toolCallId == "tc1"
        assert event.toolName == "read_file"

    def test_unknown_type_returns_unknown_event(self) -> None:
        event = decode_event('{"type": "future_event", "data": 123}')
        assert isinstance(event, StreamUnknownEvent)
        assert event.type_name == "future_event"

    def test_unknown_type_empty_type_name(self) -> None:
        event = decode_event('{"type": "another_unknown"}')
        assert isinstance(event, StreamUnknownEvent)
        assert event.type_name == "another_unknown"

    def test_malformed_json_raises(self) -> None:
        with pytest.raises(msgspec.DecodeError):
            decode_event("{invalid json")

    def test_end_event_with_session_id(self) -> None:
        event = decode_event(
            '{"type": "end", "stopReason": "end_turn", "sessionId": "sess-123"}'
        )
        assert isinstance(event, StreamEndEvent)
        assert event.sessionId == "sess-123"

    def test_end_event_with_usage(self) -> None:
        event = decode_event(
            '{"type": "end", "usage": {"input_tokens": 100, "output_tokens": 50}}'
        )
        assert isinstance(event, StreamEndEvent)
        assert event.usage is not None
        assert event.usage["input_tokens"] == 100
