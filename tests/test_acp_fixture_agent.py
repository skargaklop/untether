from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

import anyio
import pytest

from untether.runners.acp.peer import AcpPeer, AcpProtocolError
from untether.runners.acp.protocol import V1Adapter, V2Adapter, negotiate

FIXTURE = Path(__file__).parent / "fixtures" / "acp_agent.py"

_V1_INIT = V1Adapter().initialize_params()
_V2_INIT = V2Adapter().initialize_params()


def _fixture_args(scenario: str) -> list[str]:
    return [str(FIXTURE), "--scenario", scenario]


async def _drain_until(peer: AcpPeer, pred, limit: int = 128) -> dict:
    """Read notifications until one satisfies ``pred``; fail otherwise."""
    for _ in range(limit):
        notification = await peer.next_notification()
        if pred(notification):
            return notification
    raise AssertionError("expected notification never arrived")


def _is_state_update(notification: dict) -> bool:
    return notification.get("params", {}).get("sessionUpdate") == "state_update"


def _is_idle_state_update(notification: dict) -> bool:
    params = notification.get("params", {})
    return (
        params.get("sessionUpdate") == "state_update" and params.get("state") == "idle"
    )


def _pid_alive(pid: int) -> bool:
    """Portable process-existence probe (os.kill(pid, 0) is unreliable on Windows)."""
    if os.name == "posix":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(0x1000 | 0x00100000, False, pid)  # query+sync
    if not handle:
        return False
    try:
        # WAIT_TIMEOUT (0x102) => still running; WAIT_OBJECT_0 (0) => exited.
        return kernel32.WaitForSingleObject(handle, 0) == 0x102
    finally:
        kernel32.CloseHandle(handle)


# ---------------------------------------------------------------------------
# negotiate + prompt flow smokes
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_v1_agent_negotiates_and_completes_turn() -> None:
    async with AcpPeer(sys.executable, _fixture_args("v1-agent")) as peer:
        init = await peer.request("initialize", _V1_INIT)
        assert negotiate("1", init).version == 1

        session = await peer.request("session/new", {"cwd": "/tmp"})
        assert session["sessionId"] == "sess-1"

        resp = await peer.request(
            "session/prompt", V1Adapter().prompt_params(session["sessionId"], "hi")
        )
        assert resp["stopReason"] == "end_turn"

        # v1 streams session/update notifications before completing.
        first = await peer.next_notification()
        assert first["method"] == "session/update"


@pytest.mark.anyio
async def test_v2_agent_streams_v2_variants_and_idle_completion() -> None:
    async with AcpPeer(sys.executable, _fixture_args("v2-agent")) as peer:
        init = await peer.request("initialize", _V2_INIT)
        assert negotiate("2", init).version == 2

        session = await peer.request("session/new", {"cwd": "/tmp"})
        assert session["sessionId"] == "sess-2"

        # v2 prompt response is an empty acknowledgement; the turn completes
        # through an idle state_update notification.
        ack = await peer.request(
            "session/prompt", V2Adapter().prompt_params(session["sessionId"], "hi")
        )
        assert ack == {}

        first = await peer.next_notification()
        assert first["params"].get("sessionUpdate") == "agent_thought_chunk"

        completion = await _drain_until(peer, _is_state_update)
        assert completion["params"]["stopReason"] == "end_turn"
        assert completion["params"]["state"] == "idle"


@pytest.mark.anyio
async def test_v2_nonstreaming_uses_full_agent_message_upserts() -> None:
    async with AcpPeer(sys.executable, _fixture_args("v2-nonstreaming")) as peer:
        init = await peer.request("initialize", _V2_INIT)
        assert negotiate("2", init).version == 2

        session = await peer.request("session/new", {"cwd": "/tmp"})
        ack = await peer.request(
            "session/prompt", V2Adapter().prompt_params(session["sessionId"], "hi")
        )
        assert ack == {}

        first = await peer.next_notification()
        # Non-streaming agent: a full message upsert, never a chunk.
        assert first["params"].get("sessionUpdate") == "agent_message"
        assert first["params"].get("content") == "Full non-streaming answer"

        completion = await _drain_until(peer, _is_state_update)
        assert completion["params"]["stopReason"] == "end_turn"


# ---------------------------------------------------------------------------
# reverse request round-trips
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_permission_request_selected_completes_turn() -> None:
    received: dict = {}

    def permission_handler(params: dict) -> dict:
        received["options"] = params.get("options", [])
        return {"outcome": "selected", "optionId": "allow"}

    async with AcpPeer(sys.executable, _fixture_args("permission-request")) as peer:
        init = await peer.request("initialize", _V2_INIT)
        assert negotiate("2", init).version == 2
        session = await peer.request("session/new", {"cwd": "/tmp"})

        peer.register_handler("session/request_permission", permission_handler)
        ack = await peer.request(
            "session/prompt", V2Adapter().prompt_params(session["sessionId"], "go")
        )
        assert ack == {}

        option_ids = [o.get("optionId") for o in received["options"]]
        assert option_ids == ["allow", "reject"]

        completion = await _drain_until(peer, _is_idle_state_update)
        assert completion["params"]["stopReason"] == "end_turn"


@pytest.mark.anyio
async def test_permission_request_cancelled_ends_turn_with_cancelled_reason() -> None:
    def permission_handler(_params: dict) -> dict:
        return {"outcome": "cancelled"}

    async with AcpPeer(sys.executable, _fixture_args("permission-request")) as peer:
        init = await peer.request("initialize", _V2_INIT)
        assert negotiate("2", init).version == 2
        session = await peer.request("session/new", {"cwd": "/tmp"})

        peer.register_handler("session/request_permission", permission_handler)
        ack = await peer.request(
            "session/prompt", V2Adapter().prompt_params(session["sessionId"], "go")
        )
        assert ack == {}
        completion = await _drain_until(peer, _is_idle_state_update)
        assert completion["params"]["stopReason"] == "cancelled"


@pytest.mark.anyio
async def test_elicitation_form_round_trip() -> None:
    received: dict = {}

    def elicitation_handler(params: dict) -> dict:
        received["form"] = params.get("form", {})
        return {"outcome": "accept", "content": {"name": "Ada"}}

    async with AcpPeer(sys.executable, _fixture_args("elicitation-form")) as peer:
        init = await peer.request("initialize", _V2_INIT)
        assert negotiate("2", init).version == 2
        session = await peer.request("session/new", {"cwd": "/tmp"})

        peer.register_handler("elicitation/create", elicitation_handler)
        ack = await peer.request(
            "session/prompt", V2Adapter().prompt_params(session["sessionId"], "form")
        )
        assert ack == {}

        schema = received["form"].get("schema", {})
        assert schema.get("type") == "object"

        completion = await _drain_until(peer, _is_state_update)
        assert completion["params"]["stopReason"] == "end_turn"


# ---------------------------------------------------------------------------
# transport failure modes
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_malformed_line_fails_with_malformed_json() -> None:
    peer = AcpPeer(sys.executable, _fixture_args("malformed-line"))
    await peer.start()
    with anyio.fail_after(5):
        with pytest.raises(AcpProtocolError, match="malformed"):
            await peer.request("initialize", _V1_INIT)
    await peer.close()


@pytest.mark.anyio
async def test_oversize_frame_reports_frame_limit() -> None:
    peer = AcpPeer(sys.executable, _fixture_args("oversize-frame"))
    await peer.start()
    with anyio.fail_after(5):
        with pytest.raises(AcpProtocolError, match="frame limit"):
            await peer.request("initialize", _V1_INIT)
    await peer.close()


@pytest.mark.anyio
async def test_v1_batch_is_rejected() -> None:
    async with AcpPeer(sys.executable, _fixture_args("v1-batch")) as peer:
        init = await peer.request("initialize", _V1_INIT)
        assert negotiate("1", init).version == 1
        with pytest.raises(AcpProtocolError, match="batch"):
            await peer.request("hello", {})


@pytest.mark.anyio
async def test_notify_flood_overflows_queue() -> None:
    peer = AcpPeer(sys.executable, _fixture_args("notify-flood"), queue_size=1)
    await peer.start()
    with anyio.fail_after(5):
        with pytest.raises(AcpProtocolError, match="queue overflow"):
            await peer.request("n", {})
    await peer.close()


@pytest.mark.anyio
async def test_print_pid_is_reaped_on_close() -> None:
    peer = AcpPeer(sys.executable, _fixture_args("print-pid"))
    proc = None
    async with peer:
        proc = peer._proc
        assert proc is not None
        # The fixture reports its own PID on stderr; poll until the stderr
        # drain task has read it. NOTE: proc.pid is the shim/launcher pid on
        # Windows (uv python re-execs), so identity is asserted only on POSIX
        # where they coincide; the tree-dead invariant below is the real check.
        pid_line = None
        with anyio.fail_after(5):
            while pid_line is None:
                pid_line = next(
                    (line for line in peer.stderr_tail if line.startswith("acp-pid")),
                    None,
                )
                if pid_line is None:
                    await anyio.sleep(0.05)
        assert pid_line is not None, f"no pid line in stderr tail: {peer.stderr_tail!r}"
        if os.name == "posix":
            assert int(pid_line.split()[1]) == proc.pid

        init = await peer.request("initialize", _V1_INIT)
        assert negotiate("1", init).version == 1

    # context exit closed and reaped the child: the reported PID is gone.
    for _ in range(20):
        if not _pid_alive(proc.pid):
            break
        await anyio.sleep(0.05)
    assert not _pid_alive(proc.pid), "subprocess PID still alive after peer.close()"
