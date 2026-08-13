from __future__ import annotations

import sys

import pytest

from untether.runners.acp.peer import AcpPeer, AcpProtocolError


def fixture(code: str) -> list[str]:
    return ["-u", "-c", code]


@pytest.mark.anyio
async def test_peer_correlates_interleaved_notification_and_reverse_request() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','method':'session/update','params':{'n':1}}), flush=True)
print(json.dumps({'jsonrpc':'2.0','id':'reverse','method':'client/ask','params':{}}), flush=True)
reverse = json.loads(sys.stdin.readline())
assert reverse['result']['ok'] is True
print(json.dumps({'jsonrpc':'2.0','id':req['id'],'result':{'ok':True}}), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:
        peer.register_handler("client/ask", lambda _p: {"ok": True})
        result = await peer.request("hello", {})
        assert result == {"ok": True}
        notification = await peer.next_notification()
        assert notification["method"] == "session/update"


@pytest.mark.anyio
async def test_peer_eof_and_malformed_input_are_protocol_errors() -> None:
    for output, match in [("", "EOF"), ("not json\n", "malformed")]:
        code = f"import sys; sys.stdout.write({output!r}); sys.stdout.flush()"
        async with AcpPeer(sys.executable, fixture(code)) as peer:
            with pytest.raises(AcpProtocolError, match=match):
                await peer.request("x", {})


@pytest.mark.anyio
async def test_peer_timeout_tears_down_process() -> None:
    code = "import sys; sys.stdin.readline(); import time; time.sleep(30)"
    peer = AcpPeer(sys.executable, fixture(code), request_timeout_s=0.01)
    await peer.start()
    with pytest.raises(TimeoutError):
        await peer.request("slow", {})
    assert peer.closed
    await peer.close()


@pytest.mark.anyio
async def test_encode_rejects_unbounded_newline_frame() -> None:
    peer = AcpPeer("unused", [], max_frame_bytes=10)
    with pytest.raises(ValueError, match="frame"):
        peer.encode({"x": "0123456789"})
