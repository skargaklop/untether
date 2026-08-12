from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import anyio
import pytest

import untether.runners.agy as agy_runner
from untether.model import ActionEvent, CompletedEvent, ResumeToken, StartedEvent
from untether.runners.agy import ENGINE, AgyRunner, parse_conversation_id
from untether.runners.run_options import EngineRunOptions, apply_run_options


class _LinesStream:
    """ByteReceiveStream that yields the given lines as a newline-delimited blob."""

    def __init__(self, *lines: str) -> None:
        self._buf = (
            (b"\n".join(line.encode("utf-8") for line in lines) + b"\n")
            if lines
            else b""
        )

    async def receive(self, max_bytes: int = -1) -> bytes:
        if not self._buf:
            raise anyio.EndOfStream
        if max_bytes < 0:
            chunk, self._buf = self._buf, b""
        else:
            chunk, self._buf = self._buf[:max_bytes], self._buf[max_bytes:]
        return chunk


def _fake_manager(stdout_lines: list[str], rc: int = 0) -> Any:
    class FakeProc:
        stdout = _LinesStream(*stdout_lines)
        stderr = _LinesStream()
        stdin = None
        pid = 42
        _rc = rc

        async def wait(self) -> int:
            return self._rc

    class FakeManager:
        async def __aenter__(self) -> FakeProc:
            return FakeProc()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    return FakeManager()


def test_agy_resume_format_and_extract() -> None:
    runner = AgyRunner(agy_cmd="agy")
    token = ResumeToken(engine=ENGINE, value="sid-123")

    assert runner.format_resume(token) == "`agy --conversation sid-123`"
    assert runner.extract_resume("`agy --conversation sid-123`") == token
    assert runner.extract_resume("agy --conversation=other") == ResumeToken(
        engine=ENGINE, value="other"
    )
    assert runner.extract_resume("agy -c other") is None
    assert runner.extract_resume("`claude --resume sid`") is None
    assert runner.is_resume_line("agy --conversation=sid")
    assert not runner.is_resume_line("agy -c sid")


def test_agy_build_args_new_and_resumed_sessions() -> None:
    runner = AgyRunner(
        agy_cmd="agy",
        model="gemini-3-pro",
        yolo=True,
        sandbox=True,
        extra_args=["--extra"],
    )
    args = runner.build_args("hello", None)
    assert args[:1] == ["--extra"]
    assert args[args.index("--model") + 1] == "gemini-3-pro"
    assert "--sandbox" in args
    assert "--dangerously-skip-permissions" in args
    assert args[-2:] == ["-p", "hello"]

    resumed = runner.build_args("continue", ResumeToken(engine=ENGINE, value="conv-1"))
    assert resumed[resumed.index("--conversation") + 1] == "conv-1"


def test_agy_goal_overrides_plan_and_plan_blocks_yolo() -> None:
    runner = AgyRunner(yolo=True, mode="normal")
    with apply_run_options(EngineRunOptions(plan=True, goal="finish safely")):
        goal_args = runner.build_args("hello", None)
    assert goal_args[goal_args.index("-p") + 1].startswith(
        "(autonomous goal — work until: finish safely)"
    )
    assert goal_args[goal_args.index("--mode") + 1] == "normal"
    assert "--dangerously-skip-permissions" in goal_args

    with apply_run_options(EngineRunOptions(plan=True)):
        plan_args = runner.build_args("hello", None)
    assert plan_args[plan_args.index("--mode") + 1] == "plan"
    assert "--dangerously-skip-permissions" not in plan_args


def test_agy_state_promotes_only_new_sessions() -> None:
    runner = AgyRunner()
    new_state = runner.new_state("hi", None)
    UUID(new_state.resume.value)
    runner._maybe_promote(new_state, "promoted")
    assert new_state.resume == ResumeToken(engine=ENGINE, value="promoted")
    assert new_state.allow_id_promotion is False
    runner._maybe_promote(new_state, "other")
    assert new_state.resume.value == "promoted"

    resumed_state = runner.new_state("hi", ResumeToken(engine=ENGINE, value="existing"))
    runner._maybe_promote(resumed_state, "other")
    assert resumed_state.resume.value == "existing"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "Created conversation aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ),
        (
            "Resume with: agy --conversation bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ),
        (
            "one aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa then bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ),
        ("", None),
        (None, None),
    ],
)
def test_parse_agy_conversation_id(text: str | None, expected: str | None) -> None:
    assert parse_conversation_id(text) == expected


def test_agy_backend_builds_configured_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(agy_runner, "_default_agy_cmd", lambda: "default-agy")
    runner = cast(
        AgyRunner,
        agy_runner.build_runner(
            {
                "cmd": " configured-agy ",
                "model": "model",
                "dangerously_skip_permissions": False,
                "sandbox": True,
                "mode": "plan",
                "extra_args": ["--flag"],
            },
            Path("untether.toml"),
        ),
    )
    assert runner.agy_cmd == "configured-agy"
    assert runner.model == "model"
    assert runner.yolo is False
    assert runner.sandbox is True
    assert runner.mode == "plan"
    assert runner.extra_args == ["--flag"]


@pytest.mark.anyio
async def test_agy_run_transient_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blob = 'Internal error: {"message":"capacity temporarily unavailable","http_status":503}'

    class FakeProc:
        stdout = _LinesStream()
        stderr = object()
        stdin = None
        pid = 42

        async def wait(self) -> int:
            return 1

    class FakeManager:
        async def __aenter__(self) -> FakeProc:
            return FakeProc()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    async def fake_drain(
        _self: AgyRunner, _stream: object, state: object, _tag: str
    ) -> None:
        cast(agy_runner.AgyStreamState, state).stderr_tail.append(blob)

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_drain_stderr_capture", fake_drain)

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    assert isinstance(events[0], StartedEvent)
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.answer == blob
    assert completed.error is not None
    assert "temporarily unavailable" in completed.error
    assert "{" not in completed.error


@pytest.mark.anyio
async def test_agy_started_meta_prefers_run_option_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProc:
        stdout = _LinesStream()
        stderr = object()
        stdin = None
        pid = 42

        async def wait(self) -> int:
            return 0

    class FakeManager:
        async def __aenter__(self) -> FakeProc:
            return FakeProc()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    async def fake_drain(
        _self: AgyRunner, _stream: object, _state: object, _tag: str
    ) -> None:
        return None

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_drain_stderr_capture", fake_drain)

    with apply_run_options(EngineRunOptions(model="gemini-3-pro")):
        events = [
            event
            async for event in AgyRunner(model="gemini-2.5-flash").run_impl(
                "hello", None
            )
        ]

    assert isinstance(events[0], StartedEvent)
    assert events[0].meta["model"] == "gemini-3-pro"


@pytest.mark.anyio
async def test_agy_started_meta_omits_unknown_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProc:
        stdout = _LinesStream()
        stderr = object()
        stdin = None
        pid = 42

        async def wait(self) -> int:
            return 0

    class FakeManager:
        async def __aenter__(self) -> FakeProc:
            return FakeProc()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    async def fake_drain(
        _self: AgyRunner, _stream: object, _state: object, _tag: str
    ) -> None:
        return None

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_drain_stderr_capture", fake_drain)

    events = [event async for event in AgyRunner().run_impl("hello", None)]

    assert isinstance(events[0], StartedEvent)
    assert "model" not in events[0].meta


@pytest.mark.anyio
async def test_agy_failure_without_scraped_conversation_has_no_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProc:
        stdout = _LinesStream()
        stderr = object()
        stdin = None
        pid = 42

        async def wait(self) -> int:
            return 1

    class FakeManager:
        async def __aenter__(self) -> FakeProc:
            return FakeProc()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    async def fake_drain(
        _self: AgyRunner, _stream: object, _state: object, _tag: str
    ) -> None:
        return None

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_drain_stderr_capture", fake_drain)

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.resume is None


@pytest.mark.anyio
async def test_agy_failure_with_scraped_conversation_keeps_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraped_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

    class FakeProc:
        stdout = _LinesStream()
        stderr = object()
        stdin = None
        pid = 42

        async def wait(self) -> int:
            return 1

    class FakeManager:
        async def __aenter__(self) -> FakeProc:
            return FakeProc()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    async def fake_drain(
        _self: AgyRunner, _stream: object, state: object, _tag: str
    ) -> None:
        cast(agy_runner.AgyStreamState, state).stderr_tail.append(
            f"Created conversation {scraped_id}"
        )

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_drain_stderr_capture", fake_drain)

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.resume is not None
    assert completed.resume.value == scraped_id


def test_agy_build_args_includes_stream_json() -> None:
    runner = AgyRunner(agy_cmd="agy")
    args = runner.build_args("hello", None)
    assert "--output-format" in args
    assert args[args.index("--output-format") + 1] == "stream-json"


@pytest.mark.anyio
async def test_agy_stream_init_promotes_provisional_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lines = [json.dumps({"event": "init", "conversation_id": real_id, "init": {}})]
    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_a, **_k: _fake_manager(lines)
    )

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is True
    assert completed.resume is not None
    assert completed.resume.value == real_id


@pytest.mark.anyio
async def test_agy_stream_tool_lifecycle_emits_paired_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = {
        "event": "step_update",
        "step_update": {
            "conversation_id": "c1",
            "step_index": 4,
            "state": "ACTIVE",
            "step_type": "tool",
            "tool_name": "write_to_file",
            "tool_info": {
                "name": "write_to_file",
                "parameters": {"TargetFile": "/tmp/a.txt"},
            },
        },
    }
    done = {
        "event": "step_update",
        "step_update": {
            "conversation_id": "c1",
            "step_index": 4,
            "state": "DONE",
            "step_type": "tool",
            "tool_name": "write_to_file",
            "tool_info": {
                "name": "write_to_file",
                "parameters": {"TargetFile": "/tmp/a.txt"},
            },
        },
    }
    monkeypatch.setattr(
        agy_runner,
        "manage_subprocess",
        lambda *_a, **_k: _fake_manager([json.dumps(active), json.dumps(done)]),
    )

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    actions = [event for event in events if isinstance(event, ActionEvent)]
    assert [action.phase for action in actions] == ["started", "completed"]
    assert all(action.action.id == "tool-4" for action in actions)
    assert actions[0].action.kind == "file_change"
    assert actions[1].ok is True


@pytest.mark.anyio
async def test_agy_stream_tool_error_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = {
        "event": "step_update",
        "step_update": {
            "conversation_id": "c1",
            "step_index": 1,
            "state": "ACTIVE",
            "step_type": "tool",
            "tool_name": "list_dir",
            "tool_info": {
                "name": "list_dir",
                "parameters": {"DirectoryPath": "/nope"},
            },
        },
    }
    failed = {
        "event": "step_update",
        "step_update": {
            "conversation_id": "c1",
            "step_index": 1,
            "state": "ERROR",
            "step_type": "tool",
            "tool_name": "list_dir",
            "tool_info": {
                "name": "list_dir",
                "parameters": {"DirectoryPath": "/nope"},
                "error": {
                    "type": "TOOL_ERROR",
                    "message": "cannot list: " + "x" * 2000,
                },
            },
        },
    }
    monkeypatch.setattr(
        agy_runner,
        "manage_subprocess",
        lambda *_a, **_k: _fake_manager([json.dumps(active), json.dumps(failed)]),
    )

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    actions = [event for event in events if isinstance(event, ActionEvent)]
    assert actions[0].phase == "started"
    assert actions[0].action.id == "tool-1"
    assert actions[1].phase == "completed"
    assert actions[1].ok is False
    assert actions[1].message is not None
    assert len(actions[1].message) <= 500


@pytest.mark.anyio
async def test_agy_stream_result_captures_answer_usage_status_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    result = {
        "event": "result",
        "result": {
            "conversation_id": real_id,
            "status": "SUCCESS",
            "response": "done!\n",
            "duration_seconds": 5.0,
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "thinking_tokens": 2,
                "cache_read_tokens": 3,
                "total_tokens": 15,
            },
        },
    }
    monkeypatch.setattr(
        agy_runner,
        "manage_subprocess",
        lambda *_a, **_k: _fake_manager([json.dumps(result)]),
    )

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is True
    assert completed.answer == "done!\n"
    assert completed.usage is not None
    assert completed.usage["usage"]["input_tokens"] == 10
    assert completed.usage["usage"]["output_tokens"] == 5
    assert completed.usage["usage"]["thinking_tokens"] == 2
    assert completed.usage["usage"]["cache_read_tokens"] == 3
    assert completed.usage["duration_ms"] == 5000
    assert completed.resume is not None
    assert completed.resume.value == real_id


@pytest.mark.anyio
async def test_agy_stream_result_captures_error_on_error_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    result = {
        "event": "result",
        "result": {
            "conversation_id": real_id,
            "status": "ERROR",
            "error": "timeout waiting for response",
            "response": "partial answer",
            "duration_seconds": 5.0,
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
            },
        },
    }
    monkeypatch.setattr(
        agy_runner,
        "manage_subprocess",
        lambda *_a, **_k: _fake_manager([json.dumps(result)]),
    )

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.error == "timeout waiting for response"
    assert completed.answer == "partial answer"


@pytest.mark.anyio
async def test_agy_stream_error_status_without_error_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = {
        "event": "result",
        "result": {
            "status": "ERROR",
            "response": "",
        },
    }
    monkeypatch.setattr(
        agy_runner,
        "manage_subprocess",
        lambda *_a, **_k: _fake_manager([json.dumps(result)]),
    )

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.error == "agy result status: ERROR"


@pytest.mark.anyio
async def test_agy_stream_ignores_malformed_unknown_non_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lines = [
        "this is not json at all",
        "12345",
        '{"event": "mystery", "data": 1}',
        json.dumps(
            {
                "event": "step_update",
                "step_update": {
                    "conversation_id": "c1",
                    "step_index": 7,
                    "state": "DONE",
                    "step_type": "agent_response",
                    "text_delta": "hi",
                },
            }
        ),
    ]
    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_a, **_k: _fake_manager(lines)
    )

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    assert isinstance(events[0], StartedEvent)
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].ok is True
    assert all(not isinstance(event, ActionEvent) for event in events)
