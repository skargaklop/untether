from __future__ import annotations

import pytest

from untether.runners.acp.protocol import (
    ProtocolNegotiationError,
    V1Adapter,
    V2Adapter,
    negotiate,
)


def test_v1_and_v2_initialization_shapes() -> None:
    assert "clientInfo" in V1Adapter().initialize_params()
    assert "info" in V2Adapter().initialize_params()
    assert V1Adapter().prompt_params("s", "hi")["prompt"][0]["text"] == "hi"
    assert V2Adapter().config_key("model") == "configId"


def test_initialization_shape_matches_requested_protocol() -> None:
    assert "info" in V2Adapter().initialize_params()
    assert "clientInfo" in V1Adapter().initialize_params()


def test_v2_batch_normalizes_responses() -> None:
    adapter = V2Adapter()
    batch = [
        {"jsonrpc": "2.0", "id": 2, "result": {"ok": 2}},
        {"jsonrpc": "2.0", "id": 1, "result": {"ok": 1}},
    ]
    assert adapter.responses(batch) == {1: {"ok": 1}, 2: {"ok": 2}}


def test_controlled_negotiation_and_v1_fallback() -> None:
    assert negotiate("2", {"protocolVersion": 2}).version == 2
    assert negotiate("1", {"protocolVersion": 1}).version == 1
    assert negotiate("auto", {"protocolVersion": 1}, allow_v1=True).version == 1
    with pytest.raises(ProtocolNegotiationError):
        negotiate("2", {"protocolVersion": 1})
    with pytest.raises(ProtocolNegotiationError):
        negotiate("auto", {"protocolVersion": 1}, allow_v1=False)


def test_v1_resume_capability_prefers_resume_over_load() -> None:
    adapter = V1Adapter()
    result = {
        "agentCapabilities": {
            "loadSession": True,
            "sessionCapabilities": {"resume": True},
        }
    }
    assert adapter.resume_method(result) == "session/resume"


def test_v1_resume_capability_falls_back_to_load() -> None:
    adapter = V1Adapter()
    assert adapter.resume_method({"agentCapabilities": {"loadSession": True}}) == (
        "session/load"
    )


def test_v1_resume_capability_requires_load_or_resume() -> None:
    adapter = V1Adapter()
    with pytest.raises(ProtocolNegotiationError, match="load/resume"):
        adapter.resume_method({"agentCapabilities": {}})
