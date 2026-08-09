"""Tests for the ACP (Agent Client Protocol) client and compact mixin.

These test the ACP JSON-RPC client using the in-memory FakeAcpTransport,
and the AcpCompactMixin's event emission, error handling, and capability
gating.
"""

from __future__ import annotations

import pytest

from untether.runners._acp import (
    AcpProtocolError,
    FakeAcpTransport,
    _extract_available_commands,
    _parse_message,
)


class TestParseMessage:
    def test_valid_json(self) -> None:
        msg = _parse_message(b'{"jsonrpc": "2.0", "id": 1}')
        assert msg is not None
        assert msg["jsonrpc"] == "2.0"

    def test_empty_bytes(self) -> None:
        assert _parse_message(b"") is None

    def test_whitespace_only(self) -> None:
        assert _parse_message(b"   \n  ") is None

    def test_not_a_dict(self) -> None:
        assert _parse_message(b"[1, 2, 3]") is None

    def test_malformed_json_raises(self) -> None:
        with pytest.raises(AcpProtocolError, match="malformed JSON"):
            _parse_message(b"{invalid json")

    def test_unicode_decode_error(self) -> None:
        assert _parse_message(b"\xff\xfe") is None


class TestExtractAvailableCommands:
    def test_correct_envelope(self) -> None:
        notification = {
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": [
                        {"name": "compact"},
                        {"name": "chat"},
                        {"name": "compact"},  # dedup
                    ],
                }
            },
        }
        result = _extract_available_commands(notification)
        assert result == {"compact", "chat"}

    def test_wrong_method(self) -> None:
        notification = {"method": "other/method", "params": {}}
        assert _extract_available_commands(notification) is None

    def test_wrong_session_update(self) -> None:
        notification = {
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "other_update"}},
        }
        assert _extract_available_commands(notification) is None

    def test_missing_params(self) -> None:
        notification = {"method": "session/update"}
        assert _extract_available_commands(notification) is None

    def test_non_dict_params(self) -> None:
        notification = {"method": "session/update", "params": "not dict"}
        assert _extract_available_commands(notification) is None

    def test_non_list_commands(self) -> None:
        notification = {
            "method": "session/update",
            "params": {"update": {"availableCommands": "not a list"}},
        }
        assert _extract_available_commands(notification) is None

    def test_filters_empty_names(self) -> None:
        notification = {
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": [
                        {"name": "compact"},
                        {"name": ""},
                        {"no_name": "missing"},
                        "not a dict",
                    ],
                }
            },
        }
        result = _extract_available_commands(notification)
        assert result == {"compact"}


class TestFakeAcpTransport:
    @pytest.mark.anyio
    async def test_start_close_noop(self) -> None:
        transport = FakeAcpTransport()
        await transport.start()
        await transport.close()

    @pytest.mark.anyio
    async def test_send_request_records(self) -> None:
        transport = FakeAcpTransport()
        transport.queue_response("initialize", {"protocolVersion": 1})
        result = await transport.send_request(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        )
        assert result == {"protocolVersion": 1}
        assert len(transport.requests) == 1
        assert transport.requests[0]["method"] == "initialize"

    @pytest.mark.anyio
    async def test_send_request_unknown_method_returns_empty(self) -> None:
        transport = FakeAcpTransport()
        result = await transport.send_request(
            {"jsonrpc": "2.0", "id": 1, "method": "unknown", "params": {}}
        )
        assert result == {}

    @pytest.mark.anyio
    async def test_read_notification(self) -> None:
        transport = FakeAcpTransport()
        transport.emit_notification("session/update", {"key": "value"})
        notification = await transport.read_notification()
        assert notification is not None
        assert notification["method"] == "session/update"
