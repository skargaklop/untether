from __future__ import annotations

from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

import untether.runners.agy as agy_runner
from untether.model import CompletedEvent, ResumeToken, StartedEvent
from untether.runners.agy import ENGINE, AgyRunner, parse_conversation_id
from untether.runners.run_options import EngineRunOptions, apply_run_options


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
        stdout = object()
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

    async def fake_collect(_self: AgyRunner, _stdout: object) -> str:
        return ""

    async def fake_drain(
        _self: AgyRunner, _stream: object, state: object, _tag: str
    ) -> None:
        cast(agy_runner.AgyStreamState, state).stderr_tail.append(blob)

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_collect_stdout", fake_collect)
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
        stdout = object()
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

    async def fake_collect(_self: AgyRunner, _stdout: object) -> str:
        return "ok"

    async def fake_drain(
        _self: AgyRunner, _stream: object, _state: object, _tag: str
    ) -> None:
        return None

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_collect_stdout", fake_collect)
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
async def test_agy_started_meta_reports_auto_when_model_is_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProc:
        stdout = object()
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

    async def fake_collect(_self: AgyRunner, _stdout: object) -> str:
        return "ok"

    async def fake_drain(
        _self: AgyRunner, _stream: object, _state: object, _tag: str
    ) -> None:
        return None

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_collect_stdout", fake_collect)
    monkeypatch.setattr(AgyRunner, "_drain_stderr_capture", fake_drain)

    events = [event async for event in AgyRunner().run_impl("hello", None)]

    assert isinstance(events[0], StartedEvent)
    assert events[0].meta["model"] == "auto"


@pytest.mark.anyio
async def test_agy_failure_without_scraped_conversation_has_no_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProc:
        stdout = object()
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

    async def fake_collect(_self: AgyRunner, _stdout: object) -> str:
        return ""

    async def fake_drain(
        _self: AgyRunner, _stream: object, _state: object, _tag: str
    ) -> None:
        return None

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_collect_stdout", fake_collect)
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
        stdout = object()
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

    async def fake_collect(_self: AgyRunner, _stdout: object) -> str:
        return ""

    async def fake_drain(
        _self: AgyRunner, _stream: object, state: object, _tag: str
    ) -> None:
        cast(agy_runner.AgyStreamState, state).stderr_tail.append(
            f"Created conversation {scraped_id}"
        )

    monkeypatch.setattr(
        agy_runner, "manage_subprocess", lambda *_args, **_kwargs: FakeManager()
    )
    monkeypatch.setattr(AgyRunner, "_collect_stdout", fake_collect)
    monkeypatch.setattr(AgyRunner, "_drain_stderr_capture", fake_drain)

    events = [event async for event in AgyRunner().run_impl("hello", None)]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.resume is not None
    assert completed.resume.value == scraped_id
