from __future__ import annotations

from typing import Any

import anyio
import pytest

from untether.model import CompletedEvent, StartedEvent
from untether.runners.codex import (
    AppServerCodexRunner,
    _AppServerTurnControl,
    build_runner,
)
from untether.runners.run_options import EngineRunOptions, apply_run_options


class FakeAppServer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.messages: list[dict[str, Any]] = [
            {"method": "turn/started", "params": {"turn": {"id": "turn-1"}}},
            {
                "method": "turn/plan/updated",
                "params": {"plan": [{"step": "inspect", "status": "in_progress"}]},
            },
            {
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "phase": "final_answer",
                        "text": "completed answer",
                    }
                },
            },
            {"method": "turn/completed", "params": {"turn": {"status": "completed"}}},
        ]
        self.closed = False

    async def start(self) -> None:
        self.calls.append(("initialize", {}))

    async def thread_start(self, params: dict) -> dict:
        self.calls.append(("thread/start", params))
        return {"thread": {"id": "thread-1"}}

    async def ensure_thread_loaded(self, thread_id: str) -> None:
        self.calls.append(("thread/resume", {"threadId": thread_id}))

    async def turn_start(self, thread_id: str, params: dict) -> dict:
        self.calls.append(("turn/start", {"threadId": thread_id, **params}))
        return {"turn": {"id": "turn-1"}}

    async def turn_steer(self, thread_id: str, turn_id: str, text: str) -> None:
        self.calls.append(
            (
                "turn/steer",
                {
                    "threadId": thread_id,
                    "expectedTurnId": turn_id,
                    "input": [{"type": "text", "text": text}],
                },
            )
        )

    async def turn_interrupt(self, thread_id: str, turn_id: str) -> bool:
        self.calls.append(
            ("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
        )
        return True

    async def subscribe_turn(self, turn_id: str):
        send, receive = anyio.create_memory_object_stream(128)
        for message in self.messages:
            await send.send(message)
        await send.aclose()
        return receive

    async def unsubscribe_turn(self, turn_id: str) -> None:
        self.calls.append(("unsubscribe", {"turnId": turn_id}))

    async def close(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_app_server_emits_plan_and_goal_and_steering_protocol(
    monkeypatch,
) -> None:
    fake = FakeAppServer()
    monkeypatch.setattr("untether.runners.codex._AppServerClient", lambda **_: fake)
    runner = AppServerCodexRunner(codex_cmd="codex", extra_args=[])
    with apply_run_options(EngineRunOptions(plan=True)):
        events = [event async for event in runner.run("do thing", None)]
    started = next(event for event in events if isinstance(event, StartedEvent))
    completed = next(event for event in events if isinstance(event, CompletedEvent))
    assert completed.answer == "completed answer"
    assert started.meta is not None
    control = started.meta["control"]
    assert isinstance(control, _AppServerTurnControl)
    await control.steer("goal follow-up")
    await control.interrupt()
    turn = next(payload for method, payload in fake.calls if method == "turn/start")
    assert turn["input"][0]["text"].startswith("[Untether plan mode]")
    assert any(
        method == "turn/steer" and payload["expectedTurnId"] == "turn-1"
        for method, payload in fake.calls
    )
    assert any(
        method == "turn/interrupt"
        and payload == {"threadId": "thread-1", "turnId": "turn-1"}
        for method, payload in fake.calls
    )


def test_codex_defaults_to_app_server_and_retains_exec_fallback(tmp_path) -> None:
    runner = build_runner({}, tmp_path)
    assert isinstance(runner, AppServerCodexRunner)
    assert build_runner({"mode": "exec"}, tmp_path).__class__.__name__ == "CodexRunner"


@pytest.mark.anyio
async def test_app_server_subscribe_drains_notification_arriving_before_subscription() -> (
    None
):
    from untether.runners.codex import _AppServerClient

    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    client._pending_by_turn["turn-1"] = [
        {
            "method": "turn/completed",
            "params": {"turnId": "turn-1", "answer": "early answer"},
        }
    ]
    stream = await client.subscribe_turn("turn-1")
    message = await stream.receive()
    assert message["params"]["answer"] == "early answer"
    await client.unsubscribe_turn("turn-1")


@pytest.mark.anyio
async def test_app_server_pending_overflow_is_bounded_and_terminal() -> None:
    from untether.runners.codex import _APP_PENDING_CAP, _AppServerClient

    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    turn_id = "overflow-turn"
    async with client._state_lock:
        for _index in range(_APP_PENDING_CAP + 1):
            pending = client._pending_by_turn.setdefault(turn_id, [])
            if len(pending) < _APP_PENDING_CAP:
                pending.append(
                    {"method": "turn/plan/updated", "params": {"turnId": turn_id}}
                )
            else:
                client._pending_overflow.add(turn_id)
        assert len(client._pending_by_turn[turn_id]) == _APP_PENDING_CAP
    with pytest.raises(RuntimeError, match="notification buffer overflow"):
        await client.subscribe_turn(turn_id)
    assert turn_id not in client._pending_overflow


@pytest.mark.anyio
async def test_app_server_replays_large_pending_turn_and_closes_client(
    monkeypatch,
) -> None:
    fake = FakeAppServer()
    fake.messages = [
        {"method": "turn/plan/updated", "params": {"turnId": "turn-1", "plan": []}}
        for _ in range(40)
    ] + [
        {
            "method": "turn/completed",
            "params": {"turnId": "turn-1", "answer": "large answer"},
        }
    ]
    monkeypatch.setattr("untether.runners.codex._AppServerClient", lambda **_: fake)
    events = [
        event
        async for event in AppServerCodexRunner(codex_cmd="codex", extra_args=[]).run(
            "do thing", None
        )
    ]
    completed = next(event for event in events if isinstance(event, CompletedEvent))
    assert not completed.ok and completed.answer == ""
    assert completed.error == "codex turn failed"
    assert fake.closed


@pytest.mark.anyio
async def test_app_server_eof_before_completion_is_error(monkeypatch) -> None:
    fake = FakeAppServer()
    fake.messages = [{"method": "turn/started", "params": {"turn": {"id": "turn-1"}}}]
    monkeypatch.setattr("untether.runners.codex._AppServerClient", lambda **_: fake)
    events = [
        event
        async for event in AppServerCodexRunner(codex_cmd="codex", extra_args=[]).run(
            "do thing", None
        )
    ]
    completed = next(event for event in events if isinstance(event, CompletedEvent))
    assert not completed.ok
    assert completed.error


@pytest.mark.anyio
async def test_app_server_subscribed_buffer_overflow_is_terminal() -> None:
    from untether.runners.codex import _APP_PENDING_CAP, _AppServerClient

    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    stream = await client.subscribe_turn("turn-buffer")
    sender = client._subscriptions["turn-buffer"]
    for index in range(_APP_PENDING_CAP):
        sender.send_nowait(
            {
                "method": "turn/plan/updated",
                "params": {"turnId": "turn-buffer", "index": index},
            }
        )
    with pytest.raises(anyio.WouldBlock):
        sender.send_nowait(
            {"method": "turn/plan/updated", "params": {"turnId": "turn-buffer"}}
        )
    async with client._state_lock:
        client._pending_overflow.add("turn-buffer")
        client._subscriptions.pop("turn-buffer", None)
    await stream.aclose()
    with pytest.raises(RuntimeError, match="notification buffer overflow"):
        await client.subscribe_turn("turn-buffer")


@pytest.mark.anyio
async def test_app_server_starts_and_closes_stderr_drainer(monkeypatch) -> None:
    from untether.runners.codex import _AppServerClient

    class Stream:
        async def aclose(self) -> None:
            return None

    class Proc:
        pid = 123
        returncode = 0
        stdin = stdout = stderr = Stream()

        def terminate(self) -> None:
            return None

        async def wait(self) -> int:
            return 0

    proc = Proc()
    drained = anyio.Event()

    async def fake_drain(*args, **kwargs) -> None:
        _ = args, kwargs
        drained.set()
        await anyio.sleep_forever()

    async def fake_read_loop() -> None:
        await anyio.sleep_forever()

    async def fake_open_process(*args, **kwargs):
        _ = args, kwargs
        return proc

    monkeypatch.setattr("untether.runners.codex.anyio.open_process", fake_open_process)
    monkeypatch.setattr("untether.runners.codex.drain_stderr", fake_drain)
    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    monkeypatch.setattr(client, "_read_loop", fake_read_loop)

    async def fake_request(*args, **kwargs):
        return {}

    monkeypatch.setattr(client, "request", fake_request)

    async def fake_notify(*args, **kwargs) -> None:
        _ = args, kwargs
        await anyio.sleep(0)  # noqa: ASYNC115

    monkeypatch.setattr(client, "notify", fake_notify)
    await client.start()
    with anyio.fail_after(1):
        await drained.wait()
    assert client._reader_tg is not None
    await client.close()
    assert client._reader_tg is None


@pytest.mark.anyio
async def test_app_server_close_forces_non_exiting_process(monkeypatch) -> None:
    from untether.runners.codex import _AppServerClient

    class Stream:
        async def aclose(self) -> None:
            return None

    class Proc:
        pid = 456
        returncode = None
        stdin = stdout = stderr = Stream()
        terminated = False
        killed = False

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9

        async def wait(self) -> int:
            return self.returncode or 0

    proc = Proc()
    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    client._proc = proc
    client._reader_tg = None

    async def fake_wait(_proc, timeout: float) -> bool:
        assert timeout == 2.0
        return True

    async def fake_tree(_proc) -> None:
        proc.killed = True
        proc.returncode = -9

    monkeypatch.setattr("untether.runners.codex.wait_for_process", fake_wait)
    monkeypatch.setattr("untether.runners.codex.kill_process_tree", fake_tree)
    await client.close()
    assert proc.terminated and proc.killed and client._proc is None


@pytest.mark.anyio
async def test_app_server_server_request_response(monkeypatch) -> None:
    from untether.runners.codex import _AppServerClient

    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    writes: list[dict] = []
    client._proc = type("Proc", (), {"stdout": object()})()

    async def fake_write(payload):
        writes.append(payload)

    monkeypatch.setattr(client, "_write", fake_write)

    async def fake_lines(_stream):
        yield b'{"id":7,"method":"item/commandExecution/requestApproval","params":{}}'

    monkeypatch.setattr("untether.runners.codex.iter_bytes_lines", fake_lines)

    async def fake_fail_all(_exc: BaseException) -> None:
        await anyio.sleep(0)  # noqa: ASYNC115

    monkeypatch.setattr(client, "_fail_all", fake_fail_all)
    await client._read_loop()
    assert writes == [{"id": 7, "result": {"decision": "accept"}}]


def test_codex_rejects_invalid_mode(tmp_path) -> None:
    from untether.config import ConfigError

    with pytest.raises(ConfigError, match=r"codex\.mode"):
        build_runner({"mode": "ap-server"}, tmp_path)


@pytest.mark.anyio
async def test_app_server_reader_routes_normal_notification(monkeypatch) -> None:
    from untether.runners.codex import _AppServerClient

    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    stream = await client.subscribe_turn("turn-route")
    client._proc = type("Proc", (), {"stdout": object()})()

    async def fake_lines(_stream):
        yield b'{"method":"turn/completed","params":{"turnId":"turn-route","answer":"ok"}}'

    monkeypatch.setattr("untether.runners.codex.iter_bytes_lines", fake_lines)

    async def fake_fail_all(_exc: BaseException) -> None:
        await anyio.sleep(0)  # noqa: ASYNC115

    monkeypatch.setattr(client, "_fail_all", fake_fail_all)
    await client._read_loop()
    message = await stream.receive()
    assert message["params"]["answer"] == "ok"


@pytest.mark.anyio
async def test_app_server_pending_delivery_precedes_live_notification() -> None:
    from untether.runners.codex import _APP_PENDING_CAP, _AppServerClient

    client = _AppServerClient(codex_cmd="codex", extra_args=[])
    turn_id = "ordered-turn"
    client._pending_by_turn[turn_id] = [
        {"method": "turn/plan/updated", "params": {"turnId": turn_id, "index": i}}
        for i in range(_APP_PENDING_CAP)
    ]
    stream = await client.subscribe_turn(turn_id)
    live = {"method": "turn/completed", "params": {"turnId": turn_id, "answer": "live"}}
    sender = client._subscriptions[turn_id]
    sender.send_nowait(live)
    received = [await stream.receive() for _ in range(_APP_PENDING_CAP + 1)]
    assert [item["params"].get("index") for item in received[:-1]] == list(
        range(_APP_PENDING_CAP)
    )
    assert received[-1] == live


@pytest.mark.anyio
async def test_app_server_initialize_failure_closes_process_and_streams(
    monkeypatch,
) -> None:
    from untether.runners.codex import _AppServerClient

    class Stream:
        async def aclose(self) -> None:
            return None

    class Proc:
        pid = 987
        returncode = None
        stdin = stdout = stderr = Stream()
        terminated = False
        killed = False

        def terminate(self) -> None:
            self.terminated = True

        async def wait(self) -> int:
            self.returncode = -15
            return self.returncode

    proc = Proc()
    monkeypatch.setattr(
        "untether.runners.codex.anyio.open_process", lambda *a, **k: _return(proc)
    )
    monkeypatch.setattr(
        "untether.runners.codex.wait_for_process", lambda *_a, **_k: _done()
    )
    monkeypatch.setattr(
        "untether.runners.codex.close_process_streams", lambda *_a, **_k: _closed()
    )
    client = _AppServerClient(codex_cmd="codex", extra_args=[])

    async def fail_request(*_args, **_kwargs):
        raise RuntimeError("initialize failed")

    monkeypatch.setattr(client, "request", fail_request)
    with pytest.raises(RuntimeError, match="initialize failed"):
        await client.start()
    assert proc.terminated
    assert client._proc is None


async def _return(value):
    return value


async def _done(*_args, **_kwargs):
    return False


async def _closed(*_args, **_kwargs):
    return None


@pytest.mark.anyio
async def test_app_server_runner_turn_overflow_is_single_terminal_error(
    monkeypatch,
) -> None:
    class OverflowSubscription:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise RuntimeError("codex app-server notification buffer overflow")

        async def aclose(self) -> None:
            return None

    class OverflowClient:
        async def start(self) -> None:
            return None

        async def thread_start(self, _params):
            return {"thread": {"id": "thread-overflow"}}

        async def turn_start(self, _thread_id, _params):
            return {"turn": {"id": "turn-overflow"}}

        async def subscribe_turn(self, _turn_id):
            return OverflowSubscription()

        async def unsubscribe_turn(self, _turn_id) -> None:
            return None

        async def close(self) -> None:
            return None

    client = OverflowClient()
    monkeypatch.setattr("untether.runners.codex._AppServerClient", lambda **_: client)
    events = [
        event
        async for event in AppServerCodexRunner(codex_cmd="codex", extra_args=[]).run(
            "prompt", None
        )
    ]
    completed = [event for event in events if isinstance(event, CompletedEvent)]
    assert len(completed) == 1
    assert not completed[0].ok
    assert completed[0].error is not None
    assert "notification buffer overflow" in completed[0].error


@pytest.mark.anyio
async def test_app_server_agent_message_final_answer_is_retained(monkeypatch) -> None:
    fake = FakeAppServer()
    fake.messages = [
        {
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "agentMessage",
                    "phase": "final_answer",
                    "text": "final response",
                }
            },
        },
        {"method": "turn/completed", "params": {"turn": {"status": "completed"}}},
    ]
    monkeypatch.setattr("untether.runners.codex._AppServerClient", lambda **_: fake)
    events = [
        event
        async for event in AppServerCodexRunner(codex_cmd="codex", extra_args=[]).run(
            "prompt", None
        )
    ]
    completed = next(event for event in events if isinstance(event, CompletedEvent))
    assert completed.ok and completed.answer == "final response"


@pytest.mark.anyio
async def test_app_server_failed_turn_preserves_final_answer(monkeypatch) -> None:
    fake = FakeAppServer()
    fake.messages = [
        {
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "agentMessage",
                    "phase": "final_answer",
                    "text": "partial answer",
                }
            },
        },
        {
            "method": "turn/completed",
            "params": {
                "turn": {"status": "failed", "error": {"message": "tool failed"}}
            },
        },
    ]
    monkeypatch.setattr("untether.runners.codex._AppServerClient", lambda **_: fake)
    events = [
        event
        async for event in AppServerCodexRunner(codex_cmd="codex", extra_args=[]).run(
            "prompt", None
        )
    ]
    completed = next(event for event in events if isinstance(event, CompletedEvent))
    assert not completed.ok and completed.answer == "partial answer"
    assert completed.error == "tool failed"
