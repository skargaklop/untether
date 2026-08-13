import pytest

from untether.model import CompletedEvent, ResumeToken, StartedEvent
from untether.runners.acp.runner import AcpRunner
from untether.runners.run_options import EngineRunOptions, apply_run_options


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
            return {
                "sessionId": self.session_id,
                "configOptions": [
                    {
                        "id": "model",
                        "category": "model_config",
                        "options": ["m1", "m2"],
                    },
                    {
                        "id": "thinking",
                        "category": "thought_level",
                        "options": ["low", "high"],
                    },
                    {"id": "approval_policy", "category": "other", "options": ["safe"]},
                    {"id": "mode", "category": "other", "options": ["plan"]},
                ],
            }
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
async def test_runner_maps_explicit_options_before_prompt():
    peer = FakePeer()
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        config_option_map={"permission_mode": "approval_policy", "plan": "mode"},
    )
    with apply_run_options(
        EngineRunOptions(
            model="m1", reasoning="high", permission_mode="safe", plan=True
        )
    ):
        events = [event async for event in runner.run("hello", None)]
    methods = [method for method, _ in peer.requests]
    assert methods[2:6] == [
        "session/set_config_option",
        "session/set_config_option",
        "session/set_config_option",
        "session/set_config_option",
    ]
    assert methods[6] == "session/prompt"
    assert events[-1].ok


@pytest.mark.anyio
async def test_runner_rejects_unmappable_override_before_prompt():
    peer = FakePeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    with apply_run_options(EngineRunOptions(permission_mode="safe")):
        events = [event async for event in runner.run("hello", None)]
    assert not events[-1].ok
    assert "configure" in (events[-1].error or "")
    assert "session/prompt" not in [method for method, _ in peer.requests]


@pytest.mark.anyio
async def test_resume_rejects_model_absent_from_returned_options():
    peer = FakePeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    with apply_run_options(EngineRunOptions(model="missing")):
        events = [
            event async for event in runner.run("hello", ResumeToken("acp_test", "old"))
        ]
    assert not events[-1].ok
    assert "model" in (events[-1].error or "")
    assert "session/prompt" not in [method for method, _ in peer.requests]


@pytest.mark.anyio
async def test_runner_failure_before_session_is_one_failed_completion():
    peer = FakePeer(fail=True)
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    assert len(events) == 1
    assert isinstance(events[0], CompletedEvent)
    assert not events[0].ok
    assert peer.closed
