from __future__ import annotations

import asyncio
import sys

import anyio
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


# ---------------------------------------------------------------------------
# Phase 1 — peer hardening
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_oversize_frame_reports_frame_limit_not_eof() -> None:
    code = "import sys; sys.stdout.write('x' * 200); sys.stdout.flush()"
    peer = AcpPeer(sys.executable, fixture(code), max_frame_bytes=64)
    await peer.start()
    with pytest.raises(AcpProtocolError, match="frame limit") as exc_info:
        await peer.request("x", {})
    assert "EOF" not in str(exc_info.value)
    await peer.close()


@pytest.mark.anyio
async def test_clean_close_with_partial_buffer_is_eof() -> None:
    code = "import sys; sys.stdout.write('partial'); sys.stdout.flush()"
    peer = AcpPeer(sys.executable, fixture(code))
    await peer.start()
    with pytest.raises(AcpProtocolError, match="reached EOF"):
        await peer.request("x", {})
    await peer.close()


@pytest.mark.anyio
async def test_v1_peer_rejects_batch() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
batch = [
    {'jsonrpc':'2.0','method':'session/update','params':{'n':1}},
    {'jsonrpc':'2.0','id':req['id'],'result':{'ok':True}},
]
print(json.dumps(batch), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:
        with pytest.raises(AcpProtocolError, match="batch"):
            await peer.request("hello", {})


@pytest.mark.anyio
async def test_v2_peer_processes_both_batch_entries() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
batch = [
    {'jsonrpc':'2.0','id':req['id'],'result':{'ok':True}},
    {'jsonrpc':'2.0','method':'session/update','params':{'n':2}},
]
print(json.dumps(batch), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code), allow_batches=True) as peer:
        assert await peer.request("hello", {}) == {"ok": True}
        notification = await peer.next_notification()
        assert notification["method"] == "session/update"


@pytest.mark.anyio
async def test_notification_queue_overflow_fails_run() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
for _ in range(3):
    print(json.dumps({'jsonrpc':'2.0','method':'n','params':{}}), flush=True)
"""
    peer = AcpPeer(sys.executable, fixture(code), queue_size=1)
    await peer.start()
    with anyio.fail_after(5):
        with pytest.raises(AcpProtocolError, match="queue overflow"):
            await peer.request("x", {})
    await peer.close()


@pytest.mark.anyio
async def test_reverse_handler_error_returns_internal_error() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','id':'r','method':'client/boom','params':{}}), flush=True)
reply = json.loads(sys.stdin.readline())
assert reply['error']['code'] == -32603
print(json.dumps({'jsonrpc':'2.0','id':req['id'],'result':{}}), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:

        def handler(_params):
            raise ValueError("bad handler")

        peer.register_handler("client/boom", handler)
        assert await peer.request("hello", {}) == {}


@pytest.mark.anyio
async def test_cancelled_reverse_handler_returns_cancelled_error() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','id':'r','method':'client/slow','params':{}}), flush=True)
reply = json.loads(sys.stdin.readline())
assert reply['error']['code'] == -32800
print(json.dumps({'jsonrpc':'2.0','id':req['id'],'result':{}}), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:

        async def handler(_params):
            await anyio.sleep(30)

        peer.register_handler("client/slow", handler)
        request_task = asyncio.create_task(peer.request("hello", {}))
        while not peer._reverse_tasks:  # internal: wait until the reverse task exists
            await anyio.sleep(0.01)
        reverse_task = next(iter(peer._reverse_tasks))
        reverse_task.cancel()
        assert await request_task == {}


@pytest.mark.anyio
async def test_cancel_request_cancels_running_handler() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','id':'long','method':'client/work','params':{}}), flush=True)
print(json.dumps({'jsonrpc':'2.0','method':'$/cancel_request','params':{'id':'long'}}), flush=True)
reply = json.loads(sys.stdin.readline())
assert reply['error']['code'] == -32800
print(json.dumps({'jsonrpc':'2.0','id':req['id'],'result':{'done':True}}), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:

        async def handler(_params):
            await anyio.sleep(30)

        peer.register_handler("client/work", handler)
        assert await peer.request("hello", {}) == {"done": True}


@pytest.mark.anyio
async def test_cancel_request_unknown_id_is_ignored() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
print(json.dumps({'jsonrpc':'2.0','method':'$/cancel_request','params':{'id':'nope'}}), flush=True)
print(json.dumps({'jsonrpc':'2.0','id':req['id'],'result':{}}), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code)) as peer:
        assert await peer.request("hello", {}) == {}


@pytest.mark.anyio
async def test_inbound_batch_gets_strictly_increasing_untether_seq() -> None:
    code = """
import json, sys
req = json.loads(sys.stdin.readline())
batch = [
    {'jsonrpc':'2.0','method':'session/update','params':{'n':1}},
    {'jsonrpc':'2.0','method':'session/update','params':{'n':2}},
    {'jsonrpc':'2.0','id':req['id'],'result':{'ok':True}},
]
print(json.dumps(batch), flush=True)
"""
    async with AcpPeer(sys.executable, fixture(code), allow_batches=True) as peer:
        assert await peer.request("hello", {}) == {"ok": True}
        first = await peer.next_notification()
        second = await peer.next_notification()
        assert first["_untether_seq"] < second["_untether_seq"]
        assert second["_untether_seq"] == first["_untether_seq"] + 1


@pytest.mark.anyio
async def test_malformed_json_fails_pending_and_reaps_process() -> None:
    code = "import sys, time; print('not json', flush=True); time.sleep(30)"
    peer = AcpPeer(sys.executable, fixture(code))
    await peer.start()
    proc = peer._proc
    assert proc is not None
    results = await asyncio.gather(
        peer.request("a", {}), peer.request("b", {}), return_exceptions=True
    )
    assert all(isinstance(item, AcpProtocolError) for item in results)
    assert all("malformed JSON" in str(item) for item in results)
