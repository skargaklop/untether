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
async def test_unknown_reverse_method_returns_json_rpc_error() -> None:
    code = """
import json, sys
request = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','id':'reverse','method':'client/unknown','params':{}}), flush=True)
reply = json.loads(sys.stdin.readline())
assert reply['error']['code'] == -32601
print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':{}}), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:
        assert await peer.request("hello", {}) == {}


@pytest.mark.anyio
async def test_async_reverse_handler_does_not_block_reader() -> None:
    code = """
import json, sys, time
request = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','id':'reverse','method':'client/wait','params':{}}), flush=True)
time.sleep(0.05)
print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':{'live':True}}), flush=True)
reverse = json.loads(sys.stdin.readline())
assert reverse['result']['done'] is True
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:

        async def handler(_params):
            await anyio.sleep(0.2)
            return {"done": True}

        import anyio

        peer.register_handler("client/wait", handler)
        assert await peer.request("hello", {}) == {"live": True}


@pytest.mark.anyio
async def test_peer_eof_and_malformed_input_are_protocol_errors() -> None:
    for output, match in [("", "EOF"), ("not json\n", "malformed")]:
        code = f"import sys; sys.stdout.write({output!r}); sys.stdout.flush()"
        async with AcpPeer(sys.executable, fixture(code)) as peer:
            with pytest.raises(AcpProtocolError, match=match):
                await peer.request("x", {})


@pytest.mark.anyio
async def test_chatty_stderr_is_drained_bounded_and_redacted_without_blocking() -> None:
    code = """
import json, sys
for i in range(1000):
    print('/home/secret/api-key-' + str(i), file=sys.stderr, flush=True)
request = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':{}}), flush=True)
"""
    peer = AcpPeer(sys.executable, fixture(code), request_timeout_s=1)
    await peer.start()
    assert await peer.request("hello", {}) == {}
    assert len(peer.stderr_tail) <= 20
    assert all("/home/secret" not in line for line in peer.stderr_tail)
    await peer.close()


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
