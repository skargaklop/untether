import pytest

from untether.model import CompletedEvent, StartedEvent
from untether.runners.acp.runner import AcpRunner


class FakePeer:
    def __init__(self, *, version=1, session_id="s1", fail=False):
        self.version = version
        self.session_id = session_id
        self.fail = fail
        self.closed = False
        self.requests = []

    async def start(self):
        return None

    async def request(self, method, params):
        self.requests.append((method, params))
        if self.fail:
            raise RuntimeError("peer failed")
        if method == "initialize":
            return {"protocolVersion": self.version}
        if method in {"session/new", "session/resume"}:
            return {"sessionId": self.session_id}
        if method == "session/prompt":
            return {"stopReason": "end_turn"}
        return {}

    async def next_notification(self):
        raise RuntimeError("no notifications")

    async def close(self):
        self.closed = True


@pytest.mark.anyio
async def test_runner_emits_three_event_contract_for_new_and_resume():
    peer = FakePeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    assert isinstance(events[0], StartedEvent)
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].ok
    assert [method for method, _ in peer.requests] == [
        "initialize",
        "session/new",
        "session/prompt",
        "session/close",
    ]


@pytest.mark.anyio
async def test_runner_failure_before_session_is_one_failed_completion():
    peer = FakePeer(fail=True)
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    assert len(events) == 1
    assert isinstance(events[0], CompletedEvent)
    assert not events[0].ok
    assert peer.closed
