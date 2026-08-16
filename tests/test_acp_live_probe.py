from __future__ import annotations

import os
import shlex

import pytest

from untether.runners.acp.peer import AcpPeer
from untether.runners.acp.protocol import (
    ProtocolNegotiationError,
    V1Adapter,
    V2Adapter,
    negotiate,
)


@pytest.mark.skipif(
    not os.environ.get("UNTETHER_ACP_LIVE_PROBE"),
    reason="UNTETHER_ACP_LIVE_PROBE env var not set (never runs in CI)",
)
@pytest.mark.anyio
async def test_live_probe_negotiates_v2_then_v1_and_creates_session() -> None:
    """Probe a real ACP agent command end-to-end.

    The command comes from ``UNTETHER_ACP_LIVE_PROBE`` (a normal command line,
    e.g. ``"npx -y @anthropic-ai/agent-client-proxy --some-flag"``). We
    negotiate v2 first, and only fall back to v1 when the agent declines v2
    with a fresh connection. The session is then created with an absolute cwd.
    """
    command = shlex.split(os.environ["UNTETHER_ACP_LIVE_PROBE"])
    assert command, "UNTETHER_ACP_LIVE_PROBE must contain a command"

    async with AcpPeer(command[0], command[1:]) as peer:
        init_v2 = await peer.request("initialize", V2Adapter().initialize_params())
        try:
            adapter = negotiate("2", init_v2)
        except ProtocolNegotiationError:
            adapter = None
        cwd = os.getcwd()

    if adapter is None:
        # v2 not selected — retry as v1 on a fresh connection.
        async with AcpPeer(command[0], command[1:]) as peer:
            init_v1 = await peer.request("initialize", V1Adapter().initialize_params())
            adapter = negotiate("1", init_v1)
            cwd = os.getcwd()

    assert adapter.version in (1, 2), "a protocol version must be selected"

    session = await _create_session(command, adapter, cwd)
    assert session.get("sessionId"), "session/new must return a sessionId"


async def _create_session(command: list[str], adapter, cwd: str) -> dict:
    async with AcpPeer(command[0], command[1:]) as peer:
        init = await peer.request("initialize", adapter.initialize_params())
        assert negotiate(str(adapter.version), init).version == adapter.version
        return await peer.request("session/new", {"cwd": cwd})
