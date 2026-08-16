import asyncio

import anyio
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
async def test_auto_negotiation_starts_with_v2_initialize_shape():
    peer = FakePeer(version=2)
    runner = AcpRunner(engine="acp_v2", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    init_params = peer.requests[0][1]
    assert init_params["protocolVersion"] == 2
    assert "info" in init_params
    assert "capabilities" in init_params
    assert events[-1].ok


@pytest.mark.anyio
async def test_auto_negotiation_retries_v1_on_clean_rejected_connection():
    class RejectV2Peer(FakePeer):
        async def request(self, method, params, **kwargs):
            if method == "initialize":
                self.requests.append((method, params))
                raise RuntimeError("ACP JSON-RPC error: unsupported protocol")
            return await super().request(method, params, **kwargs)

    first = RejectV2Peer(version=2)
    second = FakePeer(version=1)
    peers = iter([first, second])
    runner = AcpRunner(
        engine="acp_v1",
        command="unused",
        peer_factory=lambda: next(peers),
    )
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    assert first.requests[0][1]["protocolVersion"] == 2
    assert second.requests[0][1]["protocolVersion"] == 1
    assert "clientInfo" in second.requests[0][1]


@pytest.mark.anyio
async def test_runner_advertises_enabled_v1_facilities():
    from untether.runners.acp.facilities import AcpClientFacilities, RootFilesystem

    peer = FakePeer()
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        protocol="1",
        facilities=AcpClientFacilities(filesystem=RootFilesystem(["."])),
    )
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    assert peer.requests[0][1]["clientCapabilities"]["fs"]


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
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        close_timeout_s=0.25,
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
        event async for event in runner.run("hello", ResumeToken("acp_test", "old"))
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
        event async for event in runner.run("hello", ResumeToken("acp_test", "old"))
    ]
    assert events[-1].ok
    assert [method for method, _ in peer.requests][1] == "session/load"


@pytest.mark.anyio
async def test_v1_resume_fails_before_prompt_without_resume_capability():
    peer = FakePeer(capabilities={})
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [
        event async for event in runner.run("hello", ResumeToken("acp_test", "old"))
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

    async def request(self, method, params, **kwargs):
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

        async def request(self, method, params, **kwargs):
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
                    self.handler(
                        {
                            "tool": "shell",
                            "options": [{"optionId": "allow", "name": "Allow"}],
                        }
                    )
                )
                while broker.pending_count == 0:
                    await asyncio.sleep(0)
                pending = next(iter(broker._pending.values()))
                await broker.resolve(
                    "s1", pending.nonce, {"outcome": "selected", "optionId": "allow"}
                )
                result = await interaction
                assert result == {"outcome": "selected", "optionId": "allow"}
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
    assert permission.action.detail["options"] == [
        {"optionId": "allow", "name": "Allow"}
    ]
    assert peer.permission_seen.is_set()
    assert broker.pending_count == 0


@pytest.mark.anyio
async def test_runner_cancels_broker_interactions_on_teardown():
    broker = InteractionBroker(timeout_s=1)
    peer = FakePeer()
    runner = AcpRunner(
        engine="acp_test", command="unused", peer_factory=lambda: peer, broker=broker
    )
    pending = await broker.open("s1", "permission", {})
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok
    with pytest.raises(RuntimeError, match="cancelled"):
        await pending.wait()


@pytest.mark.anyio
async def test_runner_turn_timeout_is_distinct_from_peer_request_timeout():
    class SlowPromptPeer(FakePeer):
        async def request(self, method, params, **kwargs):
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


@pytest.mark.anyio
async def test_runner_surfaces_acp_commands_meta_and_gates_steering_on_message_command():
    class CommandPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self._updates = asyncio.Queue()
            self._updates.put_nowait(
                {
                    "method": "session/update",
                    "params": {
                        "sessionUpdate": "available_commands_update",
                        "availableCommands": [
                            {"name": "message:ls"},
                            {"name": "status"},
                        ],
                    },
                }
            )

        async def next_notification(self):
            return await self._updates.get()

    peer = CommandPeer()
    runner = AcpRunner(
        engine="acp_test", command="unused", peer_factory=lambda: peer, turn_timeout_s=1
    )
    events = [event async for event in runner.run("hello", None)]
    started = events[0]
    assert isinstance(started, StartedEvent)
    assert started.meta is not None
    assert isinstance(started.meta["acp_commands"], list)
    control = started.meta["control"]
    assert control.can_steer is True


@pytest.mark.anyio
async def test_runner_does_not_gate_steering_without_message_command():
    peer = FakePeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    control = events[0].meta["control"]
    assert control.can_steer is False


@pytest.mark.anyio
async def test_stream_turn_yields_both_updates_before_completed():
    class StreamingPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self._updates = asyncio.Queue()

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await self._updates.put(
                    {
                        "method": "session/update",
                        "params": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "c1",
                            "title": "ls",
                            "status": "completed",
                        },
                    }
                )
                await self._updates.put(
                    {
                        "method": "session/update",
                        "params": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "c2",
                            "title": "cat",
                            "status": "completed",
                        },
                    }
                )
                await asyncio.sleep(0.02)
            return await super().request(method, params)

        async def next_notification(self):
            return await self._updates.get()

    peer = StreamingPeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hi", None)]
    actions = [event for event in events if isinstance(event, ActionEvent)]
    assert len(actions) == 2
    assert events[-1].ok
    done_idx = len(events) - 1
    assert all(events.index(action) < done_idx for action in actions)


@pytest.mark.anyio
async def test_stream_turn_first_update_observable_while_turn_open():
    class GatePeer(FakePeer):
        def __init__(self):
            super().__init__()
            self._updates = asyncio.Queue()
            self.release = asyncio.Event()

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await self._updates.put(
                    {
                        "method": "session/update",
                        "params": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "c1",
                            "title": "ls",
                            "status": "completed",
                        },
                    }
                )
                await self.release.wait()
                return {"stopReason": "end_turn"}
            return await super().request(method, params)

        async def next_notification(self):
            return await self._updates.get()

    peer = GatePeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    gen = runner.run("hi", None)
    first = await gen.__anext__()
    assert isinstance(first, StartedEvent)
    # Request is still gated (turn open); the action must surface before it
    # completes via the streaming generator.
    second = await gen.__anext__()
    assert isinstance(second, ActionEvent)
    assert peer.release.is_set() is False
    peer.release.set()
    rest = [event async for event in gen]
    assert isinstance(rest[-1], CompletedEvent)
    assert rest[-1].ok


@pytest.mark.anyio
async def test_v2_prompt_acceptance_uses_request_timeout_s():
    class NeverAckPeer(FakePeer):
        def __init__(self):
            super().__init__(version=2)
            self.prompt_timeout = None

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                self.prompt_timeout = kwargs.get("timeout_s")
                raise TimeoutError("ACP prompt request timed out")
            return await super().request(method, params)

    peer = NeverAckPeer()
    runner = AcpRunner(
        engine="acp_v2",
        command="unused",
        peer_factory=lambda: peer,
        request_timeout_s=0.5,
    )
    events = [event async for event in runner.run("hello", None)]
    assert peer.prompt_timeout == 0.5
    assert isinstance(events[-1], CompletedEvent)
    assert not events[-1].ok
    assert "timed out" in (events[-1].error or "").lower()


@pytest.mark.anyio
async def test_v1_prompt_unchanged_uses_turn_timeout_s():
    class RecordingPeer(FakePeer):
        def __init__(self):
            super().__init__(version=1)
            self.prompt_timeout = None

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                self.prompt_timeout = kwargs.get("timeout_s")
            return await super().request(method, params)

    peer = RecordingPeer()
    runner = AcpRunner(
        engine="acp_v1",
        command="unused",
        peer_factory=lambda: peer,
        turn_timeout_s=123.0,
        request_timeout_s=0.5,
    )
    events = [event async for event in runner.run("hello", None)]
    assert peer.prompt_timeout == 123.0
    assert events[-1].ok


@pytest.mark.anyio
async def test_initialize_uses_startup_timeout_s():
    class RecordingInitPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self.init_timeouts = []

        async def request(self, method, params, **kwargs):
            if method == "initialize":
                self.init_timeouts.append(kwargs.get("timeout_s"))
            return await super().request(method, params)

    peer = RecordingInitPeer()
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        startup_timeout_s=1.5,
    )
    events = [event async for event in runner.run("hello", None)]
    assert peer.init_timeouts == [1.5]
    assert events[-1].ok


@pytest.mark.anyio
async def test_startup_timeout_fails_run_within_bound():
    class SleepingInitPeer(FakePeer):
        def __init__(self, timeout_s):
            super().__init__()
            self.timeout_s = timeout_s
            self.init_timeouts = []

        async def request(self, method, params, **kwargs):
            if method == "initialize":
                self.init_timeouts.append(kwargs.get("timeout_s"))
                await asyncio.sleep(self.timeout_s + 0.3)
                raise TimeoutError("ACP initialize timed out")
            return await super().request(method, params)

    peer = SleepingInitPeer(0.05)
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        startup_timeout_s=0.05,
    )
    events = [event async for event in runner.run("hello", None)]
    assert peer.init_timeouts == [0.05]
    assert isinstance(events[-1], CompletedEvent)
    assert not events[-1].ok
    assert "timed out" in (events[-1].error or "").lower()


@pytest.mark.anyio
async def test_session_new_sends_mcp_servers_when_configured():
    mcp_servers = [{"name": "tools", "command": "mcp", "args": ["--x"]}]
    peer = FakePeer()
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: peer,
        mcp_servers=mcp_servers,
    )
    events = [event async for event in runner.run("hello", None)]
    new_params = next(
        params for method, params in peer.requests if method == "session/new"
    )
    assert new_params["mcpServers"] == mcp_servers
    assert events[-1].ok


@pytest.mark.anyio
async def test_session_new_omits_mcp_servers_when_unconfigured():
    peer = FakePeer()
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=lambda: peer)
    events = [event async for event in runner.run("hello", None)]
    new_params = next(
        params for method, params in peer.requests if method == "session/new"
    )
    assert "mcpServers" not in new_params
    assert events[-1].ok


@pytest.mark.anyio
async def test_resume_engine_mismatch_fails_before_peer_launch():
    calls: list[int] = []

    def make_peer():
        calls.append(1)
        return FakePeer()

    token = ResumeToken("other_engine", "s1")
    runner = AcpRunner(engine="acp_test", command="unused", peer_factory=make_peer)
    events = [event async for event in runner.run("hello", token)]
    assert calls == []
    assert len(events) == 1
    assert isinstance(events[0], CompletedEvent)
    assert not events[0].ok
    assert "engine" in (events[0].error or "")


@pytest.mark.anyio
async def test_permission_action_carries_inline_keyboard_and_outcome_wire_shape():
    class PermissionKeyboardPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self.handler_registered = asyncio.Event()
            self.outcome: dict | None = None

        def register_handler(self, method, handler):
            assert method == "session/request_permission"
            self.handler = handler
            self.handler_registered.set()

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await self.handler_registered.wait()
                interaction = asyncio.create_task(
                    self.handler(
                        {
                            "title": "Allow file access?",
                            "options": [
                                {"optionId": "allow", "name": "Allow"},
                                {"optionId": "reject", "name": "Reject"},
                            ],
                        }
                    )
                )
                while broker.pending_count == 0:
                    await asyncio.sleep(0)
                pending = next(iter(broker._pending.values()))
                await broker.resolve(
                    "s1",
                    pending.nonce,
                    {"outcome": "selected", "optionId": "allow"},
                )
                self.outcome = await interaction
                return {"stopReason": "end_turn"}
            return await super().request(method, params, **kwargs)

    broker = InteractionBroker(timeout_s=1)
    peer = PermissionKeyboardPeer()
    runner = AcpRunner(
        engine="acp_test", command="unused", peer_factory=lambda: peer, broker=broker
    )
    events = [event async for event in runner.run("hello", None)]
    assert events[-1].ok

    action_events = [event for event in events if isinstance(event, ActionEvent)]
    assert action_events
    started = action_events[0]
    assert started.action.kind == "tool"
    assert started.action.title == "Allow file access?"
    keyboard = started.action.detail["inline_keyboard"]
    buttons = keyboard["buttons"]
    assert len(buttons) == 2
    assert buttons[0][0]["callback_data"] == f"acp_control:{started.action.id}:allow"
    assert buttons[1][0]["callback_data"] == f"acp_control:{started.action.id}:reject"
    assert peer.outcome == {"outcome": "selected", "optionId": "allow"}
    same_id = [event for event in action_events if event.action.id == started.action.id]
    assert len(same_id) == 2


@pytest.mark.anyio
async def test_permission_timeout_returns_cancelled_outcome():
    class SlowPermissionPeer(FakePeer):
        def __init__(self):
            super().__init__()
            self.handler_registered = asyncio.Event()
            self.outcome: dict | None = None

        def register_handler(self, method, handler):
            self.handler = handler
            self.handler_registered.set()

        async def request(self, method, params, **kwargs):
            if method == "session/prompt":
                await self.handler_registered.wait()
                self.outcome = await self.handler(
                    {"options": [{"optionId": "allow", "name": "Allow"}]}
                )
                return {"stopReason": "end_turn"}
            return await super().request(method, params, **kwargs)

    broker = InteractionBroker(timeout_s=0.01)
    peer = SlowPermissionPeer()
    runner = AcpRunner(
        engine="acp_test", command="unused", peer_factory=lambda: peer, broker=broker
    )
    events = [event async for event in runner.run("hello", None)]
    assert peer.outcome == {"outcome": "cancelled"}
    assert events[-1].ok


@pytest.mark.anyio
async def test_teardown_mid_turn_pending_permission_does_not_hang():
    """RunnerBridge aclose regression (real subprocess): aborting a run while
    a permission interaction is pending must resolve it, send the agent an
    error outcome, not hang, and reap the subprocess."""
    import sys as _sys
    from pathlib import Path as _Path

    from untether.runners.acp.peer import AcpPeer as _RealPeer

    fixture = str(_Path(__file__).parent / "fixtures" / "acp_agent.py")
    peer_factory = lambda: _RealPeer(  # noqa: E731
        _sys.executable, [fixture, "--scenario", "permission-request"]
    )
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=peer_factory,
        turn_timeout_s=30,
    )

    with anyio.fail_after(10):
        agen = runner.run("hello", None)
        started_seen = False
        action_seen = False
        async for event in agen:
            if isinstance(event, StartedEvent):
                started_seen = True
            if isinstance(event, ActionEvent) and not action_seen:
                action_seen = True
                break
        await agen.aclose()

    assert started_seen
    assert action_seen
    assert runner.broker.pending_count == 0


@pytest.mark.anyio
async def test_e2e_fixture_permission_flow_resolves_via_nonce():
    """Full E2E: real fixture agent requests permission mid-turn; the runner
    surfaces an inline keyboard; resolving the nonce via the broker (as
    acp_control does) completes the turn with the selected outcome on the
    wire (fixture ends with stopReason end_turn on selection)."""
    import sys as _sys
    from pathlib import Path as _Path

    from untether.runners.acp.interactions import resolve_nonce
    from untether.runners.acp.peer import AcpPeer as _RealPeer

    fixture = str(_Path(__file__).parent / "fixtures" / "acp_agent.py")
    runner = AcpRunner(
        engine="acp_test",
        command="unused",
        peer_factory=lambda: _RealPeer(
            _sys.executable, [fixture, "--scenario", "permission-request"]
        ),
        turn_timeout_s=30,
    )

    events = []
    with anyio.fail_after(15):
        async for event in runner.run("hello", None):
            events.append(event)
            if isinstance(event, ActionEvent) and event.action.detail.get(
                "inline_keyboard"
            ):
                nonce = event.action.id
                entry = resolve_nonce(nonce)
                assert entry is not None
                broker, owner = entry
                await broker.resolve(
                    owner, nonce, {"outcome": "selected", "optionId": "allow"}
                )

    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok
    assert runner.broker.pending_count == 0
