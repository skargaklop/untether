"""Tests for Pi schema forward compatibility: PiUnknownEvent, decode_event,
floating delayMs, and malformed-known-event rejection.
"""

from __future__ import annotations

import msgspec
import pytest

from untether.schemas.pi import (
    _KNOWN_TYPES,
    AutoRetryStart,
    PiUnknownEvent,
    decode_event,
)


class TestDecodeUnknownEvent:
    def test_unknown_type_returns_pi_unknown_event(self) -> None:
        line = '{"type": "notice", "text": "hello"}'
        event = decode_event(line)
        assert isinstance(event, PiUnknownEvent)
        assert event.type_name == "notice"

    def test_unknown_type_preserves_type_name(self) -> None:
        line = '{"type": "future_event", "data": 123}'
        event = decode_event(line)
        assert isinstance(event, PiUnknownEvent)
        assert event.type_name == "future_event"

    def test_known_type_decodes_normally(self) -> None:
        from untether.schemas.pi import AgentStart

        line = '{"type": "agent_start"}'
        event = decode_event(line)
        assert isinstance(event, AgentStart)

    def test_session_header_decodes(self) -> None:
        from untether.schemas.pi import SessionHeader

        line = '{"type": "session", "id": "abc123"}'
        event = decode_event(line)
        assert isinstance(event, SessionHeader)
        assert event.id == "abc123"


class TestMalformedKnownEvent:
    def test_missing_required_field_raises(self) -> None:
        # tool_execution_start requires toolCallId
        line = '{"type": "tool_execution_start"}'
        with pytest.raises(msgspec.ValidationError):
            decode_event(line)

    def test_wrong_type_for_field_raises(self) -> None:
        # isError must be bool
        line = (
            '{"type": "tool_execution_end", "toolCallId": "x", "isError": "not a bool"}'
        )
        with pytest.raises(msgspec.ValidationError):
            decode_event(line)


class TestFloatingDelayMs:
    def test_integer_delayms(self) -> None:
        line = '{"type": "auto_retry_start", "delayMs": 1000}'
        event = decode_event(line)
        assert isinstance(event, AutoRetryStart)
        assert event.delayMs == 1000

    def test_floating_delayms(self) -> None:
        line = '{"type": "auto_retry_start", "delayMs": 1500.5}'
        event = decode_event(line)
        assert isinstance(event, AutoRetryStart)
        assert event.delayMs == 1500.5

    def test_null_delayms(self) -> None:
        line = '{"type": "auto_retry_start", "delayMs": null}'
        event = decode_event(line)
        assert isinstance(event, AutoRetryStart)
        assert event.delayMs is None


class TestKnownTypesSet:
    def test_contains_all_known_types(self) -> None:
        expected = {
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
        assert expected == _KNOWN_TYPES

    def test_does_not_contain_unknown_types(self) -> None:
        assert "notice" not in _KNOWN_TYPES
        assert "future_event" not in _KNOWN_TYPES
