import asyncio

import pytest

from untether.model import ActionEvent, CompletedEvent, ResumeToken, StartedEvent
from untether.runners.acp.interactions import InteractionBroker
from untether.runners.acp.runner import AcpRunner
from untether.runners.run_options import EngineRunOptions, apply_run_options


class FakePeer:
    def __init__(
        self,
        *,
        version=1,
        session_id="s1",
        fail=False,
        close_capability=True,
        capabilities=None,
    ):
        self.version = version
        self.session_id = session_id
        self.fail = fail
        self.capabilities = capabilities
        self.close_capability = close_capability
        self.closed = False
        self.requests = []

    async def start(self):
        return None

    async def request(self, method, params, **kwargs):
        self.requests.append((method, params))
        if self.fail:
            raise RuntimeError("peer failed")
        if method == "initialize":
            capabilities = self.capabilities
            if capabilities is None:
                capabilities = (
                    {"sessionCapabilities": {"close": True, "resume": True}}
                    if self.close_capability
                    else {}
                )
            return {
                "protocolVersion": self.version,
                "agentCapabilities": capabilities,
            }
        if method in {"session/new", "session/load", "session/resume"}:
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

    async def notify(self, method, params):
        self.requests.append((method, params))

    async def close(self):
        self.closed = True


@pytest.mark.anyio
async def test_runner_emits_three_event_contract_for_new_and_resume():
    peer = FakePeer()
    peer.close_capability = True
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
async def test_v1_without_close_capability_skips_session_close():
    peer = FakePeer(capabilities={})
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    assert "session/close" not in [method for method, _ in peer.requests]


@pytest.mark.anyio
async def test_close_request_uses_close_timeout_and_preserves_success_on_failure():
    class CloseFailPeer(FakePeer):
        async def request(self, method, params, **kwargs):
            if method == "session/close":
                assert kwargs == {"timeout_s": 0.25}
                raise RuntimeError("close failed")
            return await super().request(method, params)

    peer = CloseFailPeer()
    runner = AcpRunner(
        engine="acp_test", command="unused", peer_factory=lambda: peer, close_timeout_s=0.25
    )
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    assert events[-1].error is None


@pytest.mark.anyio
async def test_v1_resume_uses_session_resume_when_capability_is_advertised():
    peer = FakePeer(
        capabilities={
            "sessionCapabilities": {"resume": True, "close": True},
        }
    )
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [
        event
        async for event in runner.run("hello", ResumeToken("acp_test", "old"))
    ]
    assert events[-1].ok
    assert [method for method, _ in peer.requests] == [
        "initialize",
        "session/resume",
        "session/prompt",
        "session/close",
    ]


@pytest.mark.anyio
async def test_v1_resume_falls_back_to_session_load():
    peer = FakePeer(capabilities={"loadSession": True})
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [
        event
        async for event in runner.run("hello", ResumeToken("acp_test", "old"))
    ]
    assert events[-1].ok
    assert [method for method, _ in peer.requests][1] == "session/load"


@pytest.mark.anyio
async def test_v1_resume_fails_before_prompt_without_resume_capability():
    peer = FakePeer(capabilities={})
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [
        event
        async for event in runner.run("hello", ResumeToken("acp_test", "old"))
    ]
    assert not events[-1].ok
    assert "load/resume" in (events[-1].error or "")
    assert "session/prompt" not in [method for method, _ in peer.requests]


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
async def test_runner_advertises_auth_methods_and_authenticates_before_session():
    peer = AuthPeer(auth_methods=[{"id": "device"}])
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        auth_method="device",
    )
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    assert [method for method, _ in peer.requests][:3] == [
        "initialize",
        "authenticate",
        "session/new",
    ]
    assert peer.requests[1][1] == {"methodId": "device"}


@pytest.mark.anyio
@pytest.mark.parametrize("resumed", [False, True])
async def test_runner_auth_required_retries_session_operation_once(resumed):
    peer = AuthPeer(auth_methods=["device"], auth_required_once=True)
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        auto_auth=True,
    )
    resume = ResumeToken("acp_test", "old") if resumed else None
    events = [event async for event in runner.run("hello", resume)]
    assert events[-1].ok
    methods = [method for method, _ in peer.requests]
    operation = "session/resume" if resumed else "session/new"
    assert methods.count(operation) == 2
    assert methods.count("authenticate") == 1


@pytest.mark.anyio
async def test_runner_second_auth_required_fails_without_retry_loop():
    peer = AuthPeer(auth_methods=["device"], auth_required_always=True)
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        auto_auth=True,
    )
    events = [event async for event in runner.run("hello", None)]
    assert not events[-1].ok
    assert [method for method, _ in peer.requests].count("session/new") == 2
    assert [method for method, _ in peer.requests].count("authenticate") == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "version, auth_method, expected",
    [
        (1, "device", "authenticate"),
        (2, "device", "auth/login"),
    ],
)
async def test_runner_uses_version_specific_auth_method(version, auth_method, expected):
    peer = AuthPeer(version=version, auth_methods=[auth_method])
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        auth_method=auth_method,
    )
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    assert expected in [method for method, _ in peer.requests]


class AuthRequired(RuntimeError):
    code = "auth_required"


class AuthPeer(FakePeer):
    def __init__(
        self,
        *,
        auth_methods=None,
        auth_required_once=False,
        auth_required_always=False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.auth_methods = auth_methods or []
        self.auth_required_once = auth_required_once
        self.auth_required_always = auth_required_always
        self.authenticated = False

    async def request(self, method, params):
        if method == "initialize":
            self.requests.append((method, params))
            return {
                "protocolVersion": self.version,
                "authMethods": self.auth_methods,
                "agentCapabilities": {
                    "sessionCapabilities": {"resume": True, "close": True}
                },
            }
        if method in {"authenticate", "auth/login"}:
            self.requests.append((method, params))
            self.authenticated = True
            return {}
        if method in {"session/new", "session/load", "session/resume"}:
            self.requests.append((method, params))
            if self.auth_required_always or (
                self.auth_required_once and not self.authenticated
            ):
                self.auth_required_once = False
                raise AuthRequired("auth_required")
            return {"sessionId": self.session_id, "configOptions": []}
        return await super().request(method, params)


@pytest.mark.anyio
async def test_runner_consumes_updates_while_prompt_is_pending_and_drains_v1_order():
    class PendingPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self._updates = asyncio.Queue()

        async def request(self, method, params):
            if method == "session/prompt":
                await self._updates.put(
                    {
                        "method": "session/update",
                        "params": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "call-1",
                            "title": "shell",
                            "status": "completed",
                        },
                    }
                )
                await asyncio.sleep(0.01)
            return await super().request(method, params)

        async def next_notification(self):
            return await self._updates.get()

    peer = PendingPeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    assert any(isinstance(event, ActionEvent) for event in events)
    assert events[-1].ok
    assert (
        events.index(next(event for event in events if isinstance(event, ActionEvent)))
        < len(events) - 1
    )


@pytest.mark.anyio
async def test_runner_routes_reverse_permission_to_broker_while_prompt_pending():
    class PermissionPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self.handler_registered = asyncio.Event()
            self.permission_seen = asyncio.Event()
            self.cancelled = False

        def register_handler(self, method, handler):
            assert method == "session/request_permission"
            self.handler = handler
            self.handler_registered.set()

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await self.handler_registered.wait()
                interaction = asyncio.create_task(
                    self.handler({"tool": "shell", "options": [{"id": "allow"}]})
                )
                while broker.pending_count == 0:
                    await asyncio.sleep(0)
                pending = next(iter(broker._pending.values()))
                await broker.resolve("s1", pending.nonce, {"approved": True})
                result = await interaction
                assert result["approved"] is True
                self.permission_seen.set()
                return {"stopReason": "end_turn"}
            return await super().request(method, params, **kwargs)

    broker = InteractionBroker(timeout_s=1)
    peer = PermissionPeer()
    runner = AcpRunner(
        engine="acp_test", command="unused", peer_factory=lambda: peer, broker=broker
    )
    events = [event async for event in runner.run("hello", None)]
    permission = next(event for event in events if isinstance(event, ActionEvent))
    assert permission.action.kind == "tool"
    assert permission.action.detail["nonce"]
    assert permission.action.detail["options"] == [{"id": "allow"}]
    assert peer.permission_seen.is_set()
    assert broker.pending_count == 0


@pytest.mark.anyio
async def test_runner_cancels_broker_interactions_on_teardown():
    broker = InteractionBroker(timeout_s=1)
    peer = FakePeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer, broker=broker)
    pending = await broker.open("s1", "permission", {})
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    with pytest.raises(RuntimeError, match="cancelled"):
        await pending.wait()


@pytest.mark.anyio
async def test_runner_turn_timeout_is_distinct_from_peer_request_timeout():
    class SlowPromptPeer(FakePeer):
        async def request(self, method, params):
            if method == "session/prompt":
                await asyncio.sleep(0.05)
            return await super().request(method, params)

    peer = SlowPromptPeer()
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        turn_timeout_s=0.01,
        cancel_grace_s=0.01,
    )
    events = [event async for event in runner.run("hello", None)]
    assert isinstance(events[-1], CompletedEvent)
    assert not events[-1].ok
    assert "cancel grace" in (events[-1].error or "")


@pytest.mark.anyio
async def test_runner_turn_timeout_cancels_session_and_accepts_cancelled_completion():
    class CancelledPromptPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self.cancelled = asyncio.Event()

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await self.cancelled.wait()
                return {"stopReason": "cancelled"}
            return await super().request(method, params)

        async def notify(self, method, params):
            await super().notify(method, params)
            if method == "session/cancel":
                self.cancelled.set()

    peer = CancelledPromptPeer()
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        turn_timeout_s=0.01,
        cancel_grace_s=0.1,
    )
    events = [event async for event in runner.run("hello", None)]

    assert events[-1].ok
    assert [method for method, _ in peer.requests].count("session/cancel") == 1
    assert peer.closed


@pytest.mark.anyio
async def test_runner_turn_timeout_tears_down_after_cancel_grace_expires():
    class UnresponsivePromptPeer(FakePeer):
        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await asyncio.sleep(1)
            return await super().request(method, params)

    peer = UnresponsivePromptPeer()
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        turn_timeout_s=0.01,
        cancel_grace_s=0.01,
    )
    events = [event async for event in runner.run("hello", None)]

    assert not events[-1].ok
    assert "cancel grace" in (events[-1].error or "")
    assert [method for method, _ in peer.requests].count("session/cancel") == 1
    assert peer.closed


@pytest.mark.anyio
async def test_runner_failure_before_session_is_one_failed_completion():
    peer = FakePeer(fail=True)
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    assert len(events) == 1
    assert isinstance(events[0], CompletedEvent)
    assert not events[0].ok
    assert peer.closed


@pytest.mark.anyio
async def test_v2_prompt_completes_only_after_post_prompt_running_and_idle():
    class V2Peer(FakePeer):
        def __init__(self):
            super().__init__(version=2)
            self._updates = asyncio.Queue()
            self.prompt_called = False

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                self.prompt_called = True

                async def publish_updates():
                    await asyncio.sleep(0.01)
                    await self._updates.put(
                        {
                            "method": "session/update",
                            "params": {
                                "sessionUpdate": "state_update",
                                "state": "running",
                            },
                        }
                    )
                    await self._updates.put(
                        {
                            "method": "session/update",
                            "params": {
                                "sessionUpdate": "agent_message_chunk",
                                "content": "answer",
                            },
                        }
                    )
                    await self._updates.put(
                        {
                            "method": "session/update",
                            "params": {
                                "sessionUpdate": "state_update",
                                "state": "idle",
                                "stopReason": "completed",
                            },
                        }
                    )

                self._publisher = asyncio.create_task(publish_updates())
                return {}
            return await super().request(method, params, **kwargs)

        async def next_notification(self):
            return await self._updates.get()

    peer = V2Peer()
    runner = AcpRunner(engine="acp_v2", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]

    assert events[-1].ok
    assert events[-1].answer == "answer"
    assert peer.prompt_called


@pytest.mark.anyio
async def test_v2_stale_idle_before_prompt_does_not_complete_turn():
    class V2Peer(FakePeer):
        def __init__(self):
            super().__init__(version=2)
            self._updates = asyncio.Queue()
            self._updates.put_nowait(
                {
                    "method": "session/update",
                    "params": {
                        "sessionUpdate": "state_update",
                        "state": "idle",
                        "stopReason": "completed",
                    },
                }
            )

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await asyncio.sleep(0.02)
                await self._updates.put(
                    {
                        "method": "session/update",
                        "params": {"sessionUpdate": "state_update", "state": "running"},
                    }
                )
                await self._updates.put(
                    {
                        "method": "session/update",
                        "params": {
                            "sessionUpdate": "state_update",
                            "state": "idle",
                            "stopReason": "completed",
                        },
                    }
                )
                return {}
            return await super().request(method, params, **kwargs)

        async def next_notification(self):
            return await self._updates.get()

    peer = V2Peer()
    runner = AcpRunner(
        engine="acp_v2", command="unused", peer_factory=lambda: peer, turn_timeout_s=1
    )
    events = [event async for event in runner.run("hello", None)]

    assert events[-1].ok
    assert events[-1].answer == ""
