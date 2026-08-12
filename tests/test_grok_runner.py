"""Focused Grok failure safety and shared retry regressions."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from untether.model import CompletedEvent, ResumeToken, StartedEvent, UntetherEvent
from untether.runners.grok import GrokRunner, GrokStreamState
from untether.runners.run_options import EngineRunOptions, apply_run_options
from untether.schemas import grok as grok_schema


def _grok_state() -> GrokStreamState:
    return GrokStreamState(resume=ResumeToken(engine="grok", value="session"))


def test_grok_process_error_includes_bounded_sanitized_stderr() -> None:
    runner = GrokRunner(extra_args=[])
    events = runner.process_error_events(
        1,
        resume=None,
        found_session=None,
        state=_grok_state(),
        stderr_lines=[
            'Internal error: {"message":"capacity","http_status":503}',
            "See https://secret.example.invalid/token",
        ],
    )
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.error is not None
    assert "Internal error" in completed.error
    assert "503" in completed.error
    assert "secret.example.invalid" not in completed.error


class _RetryGrok(GrokRunner):
    attempts = 0

    async def _run_single_attempt_events(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]:
        state = self.new_state(prompt, resume)
        self.attempts += 1
        for event in self.process_error_events(
            1,
            resume=resume,
            found_session=None,
            state=state,
            stderr_lines=[
                'Internal error: {"message":"admission capacity temporarily unavailable",'
                '"http_status": 503}'
            ],
        ):
            yield event


@pytest.mark.anyio
async def test_shared_retry_retries_grok_transient_before_visible_progress() -> None:
    runner = _RetryGrok(extra_args=[])
    runner.retry_max_attempts = 2
    runner.retry_base_delay_s = 0
    events = [event async for event in runner.run_impl("hello", None)]
    assert runner.attempts == 2
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].error is not None
    assert "Admission capacity" in events[-1].error
    assert '"http_status"' not in events[-1].error
    assert "HTTP 503" in events[-1].error


class _NonTransientGrok(_RetryGrok):
    async def _run_single_attempt_events(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]:
        _ = prompt, resume
        self.attempts += 1
        yield CompletedEvent(
            engine="grok",
            ok=False,
            answer="",
            resume=None,
            error="authentication failed",
        )


@pytest.mark.anyio
async def test_shared_retry_keeps_nontransient_grok_failure_to_one_attempt() -> None:
    runner = _NonTransientGrok(extra_args=[])
    runner.retry_max_attempts = 3
    runner.retry_base_delay_s = 0
    events = [event async for event in runner.run_impl("hello", None)]
    assert runner.attempts == 1
    assert isinstance(events[-1], CompletedEvent)
    assert events[-1].error == "authentication failed"


def _decode(payload: bytes) -> grok_schema.GrokEvent:
    return grok_schema.decode_event(payload)


def _run_events(payloads: list[bytes]) -> tuple[list[UntetherEvent], GrokStreamState]:
    from untether.runners.grok import translate_grok_event

    state = _grok_state()
    events: list[UntetherEvent] = []
    for payload in payloads:
        events.extend(translate_grok_event(_decode(payload), title="grok", state=state))
    return events, state


def _translate_events(
    runner: GrokRunner,
    payloads: list[bytes],
    *,
    run_options: EngineRunOptions | None = None,
) -> tuple[list[UntetherEvent], GrokStreamState]:
    state = _grok_state()
    events: list[UntetherEvent] = []
    if run_options is not None:
        ctx = apply_run_options(run_options)
    else:
        from contextlib import nullcontext

        ctx = nullcontext()
    with ctx:
        for payload in payloads:
            events.extend(
                runner.translate(
                    grok_schema.decode_event(payload),
                    state=state,
                    resume=None,
                    found_session=None,
                )
            )
    return events, state


def test_grok_build_args_honours_configuration_and_resume() -> None:
    runner = GrokRunner(
        extra_args=["--no-auto-update"],
        model="grok-build",
        reasoning_effort="high",
        tools=["read_file", "grep"],
        disallowed_tools="write",
        max_turns=7,
    )
    state = _grok_state()
    args = runner.build_args("hello", None, state=state)

    assert args[:3] == ["--no-auto-update", "--output-format", "streaming-json"]
    assert args[args.index("-p") + 1] == "hello"
    assert args[args.index("-m") + 1] == "grok-build"
    assert args[args.index("--effort") + 1] == "high"
    assert args[args.index("--tools") + 1] == "read_file,grep"
    assert args[args.index("--disallowed-tools") + 1] == "write"
    assert args[args.index("--max-turns") + 1] == "7"
    assert args[args.index("--session-id") + 1] == "session"

    resumed = runner.build_args(
        "continue", ResumeToken(engine="grok", value="prior"), state=state
    )
    assert resumed[resumed.index("--resume") + 1] == "prior"
    assert "--session-id" not in resumed


def test_grok_goal_mode_takes_precedence_over_plan_mode() -> None:
    from untether.runners.run_options import EngineRunOptions, apply_run_options

    runner = GrokRunner(tools=["write"], disallowed_tools="read_file")
    with apply_run_options(EngineRunOptions(plan=True, goal="finish safely")):
        args = runner.build_args("hello", None, state=_grok_state())

    assert "--permission-mode" not in args
    assert args[args.index("--tools") + 1] == "write"
    assert args[args.index("--disallowed-tools") + 1] == "read_file"
    assert "/goal finish safely" in args[args.index("-p") + 1]


def test_grok_plan_mode_disables_yolo_and_configured_tools() -> None:
    from untether.runners.run_options import EngineRunOptions, apply_run_options

    runner = GrokRunner(tools=["write"], disallowed_tools="read_file")
    with apply_run_options(EngineRunOptions(plan=True)):
        args = runner.build_args("hello", None, state=_grok_state())

    assert args[args.index("--permission-mode") + 1] == "plan"
    assert args[args.index("--tools") + 1] == "read_file,list_dir,grep,web_search"
    assert "--yolo" not in args
    assert "--disallowed-tools" not in args


def test_grok_translates_tool_lifecycle_and_usage() -> None:
    from untether.model import ActionEvent

    events, _state = _run_events(
        [
            b'{"type":"tool_call","toolCallId":"call-1","toolName":"read_file","rawInput":{"target_file":"foo.txt"}}',
            b'{"type":"tool_call_update","toolCallId":"call-1","status":"completed"}',
            b'{"type":"usage","usage":{"input_tokens":100,"output_tokens":5}}',
            b'{"type":"end","stopReason":"EndTurn","sessionId":"session","usage":{"input_tokens":999}}',
        ]
    )
    actions = [event for event in events if isinstance(event, ActionEvent)]
    assert [(event.phase, event.action.id, event.ok) for event in actions] == [
        ("started", "call-1", None),
        ("completed", "call-1", True),
    ]
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is True
    assert completed.usage is not None
    assert completed.usage["stopReason"] == "EndTurn"
    assert completed.usage["usage"] == {"input_tokens": 999}
    assert completed.usage["mid_stream_usage"] == {
        "input_tokens": 100,
        "output_tokens": 5,
    }


def test_grok_unknown_and_available_events_are_suppressed() -> None:
    from untether.model import StartedEvent

    events, _state = _run_events(
        [
            b'{"type":"available_commands","tools":["bash"]}',
            b'{"type":"future_event","data":"ignored"}',
            b'{"type":"end","stopReason":"EndTurn","sessionId":"session"}',
        ]
    )
    assert len(events) == 2
    assert isinstance(events[0], StartedEvent)
    assert isinstance(events[-1], CompletedEvent)


def test_grok_plan_cancellation_salvages_all_text() -> None:
    state = _grok_state()
    state.plan_mode = True
    from untether.runners.grok import translate_grok_event

    events: list[UntetherEvent] = []
    for payload in (
        b'{"type":"text","data":"inspect files"}',
        b'{"type":"thought","data":"then decide"}',
        b'{"type":"text","data":"final plan"}',
        b'{"type":"end","stopReason":"cancelled","sessionId":"session"}',
    ):
        events.extend(translate_grok_event(_decode(payload), title="grok", state=state))

    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is True
    assert completed.error is None
    assert "inspect files" in completed.answer
    assert "final plan" in completed.answer
    assert "nothing was executed" in completed.answer


def test_grok_plan_cancellation_without_text_is_an_error() -> None:
    state = _grok_state()
    state.plan_mode = True
    from untether.runners.grok import translate_grok_event

    events = translate_grok_event(
        _decode(b'{"type":"end","stopReason":"cancelled","sessionId":"session"}'),
        title="grok",
        state=state,
    )
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert "forbidden write/execute" in (completed.error or "")


def test_grok_error_event_preserves_answer_and_session() -> None:
    events, _state = _run_events(
        [
            b'{"type":"text","data":"partial"}',
            b'{"type":"error","message":"auth failed","sessionId":"replacement"}',
        ]
    )
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.answer == "partial"
    assert completed.error == "auth failed"
    assert completed.resume == ResumeToken(engine="grok", value="replacement")


def test_grok_stream_end_without_end_event_starts_and_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "untether.runners.grok._probe_grok_default_model",
        lambda _cmd: "grok-default",
    )
    runner = GrokRunner(extra_args=[])
    events = runner.stream_end_events(
        resume=None, found_session=None, state=_grok_state()
    )
    assert isinstance(events[0], StartedEvent)
    assert events[0].meta is not None
    assert events[0].meta["model"] == "grok-default"
    completed = events[-1]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.error == "grok finished without an end event"


def test_grok_started_meta_prefers_run_option_model() -> None:
    runner = GrokRunner(extra_args=[], model="grok-configured")
    events, _ = _translate_events(
        runner,
        [b'{"type":"text","data":"hi"}'],
        run_options=EngineRunOptions(model="grok-override"),
    )
    assert isinstance(events[0], StartedEvent)
    assert events[0].meta is not None
    assert events[0].meta["model"] == "grok-override"


def test_grok_started_meta_omits_unknown_model(monkeypatch: pytest.MonkeyPatch) -> None:
    # No override, no configured model, and the CLI probe yields nothing
    # (missing CLI / timeout / schema drift): the model key must be omitted,
    # never claimed as "auto".
    import untether.runners.grok as grok_mod

    monkeypatch.setattr(grok_mod, "_probe_grok_default_model", lambda cmd: None)
    runner = GrokRunner(extra_args=[])
    events, _ = _translate_events(runner, [b'{"type":"text","data":"hi"}'])
    assert isinstance(events[0], StartedEvent)
    assert not events[0].meta or "model" not in events[0].meta


# --- `grok models` default-model probe parsing ---


def test_grok_parse_models_default_line() -> None:
    from untether.runners.grok import _parse_grok_models_default

    output = (
        "You are logged in with grok.com.\n\n"
        "Default model: glm-5.2-1m-combined\n\n"
        "Available models:\n"
        "  - grok-4.5\n"
        "  * glm-5.2-1m-combined (default)\n"
    )
    assert _parse_grok_models_default(output) == "glm-5.2-1m-combined"


def test_grok_parse_models_default_asterisk_marker() -> None:
    from untether.runners.grok import _parse_grok_models_default

    output = "Available models:\n  - grok-4.5\n  * grok-4.5 (default)\n"
    assert _parse_grok_models_default(output) == "grok-4.5"


def test_grok_parse_models_default_schema_drift_is_none() -> None:
    from untether.runners.grok import _parse_grok_models_default

    assert _parse_grok_models_default("") is None
    assert (
        _parse_grok_models_default("Available models:\n  - grok-4.5\n  - grok-3\n")
        is None
    )
    assert _parse_grok_models_default("?garbage output {not a model list") is None


# --- `grok models` default-model probe caching + failure handling ---


def test_grok_default_model_probe_caches_per_executable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import untether.runners.grok as grok_mod

    calls: list[list[str]] = []

    def fake_run(cmd: str, **_: object) -> str | None:
        calls.append([cmd])
        return "Default model: glm-1\n"

    monkeypatch.setattr(grok_mod, "_run_grok_models", fake_run)
    monkeypatch.setattr(grok_mod, "_DEFAULT_MODEL_CACHE", {})
    probe = grok_mod._probe_grok_default_model
    assert probe("grok") == "glm-1"
    assert probe("grok") == "glm-1"  # cache hit: no second subprocess
    assert probe("grok-custom") == "glm-1"
    assert [call[0] for call in calls] == ["grok", "grok-custom"]


def test_grok_default_model_probe_omits_on_missing_or_bad_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import untether.runners.grok as grok_mod

    # None simulates a missing CLI / timeout / nonzero exit; the other two are
    # schema-drift outputs. Each omission is cached per executable.
    bad_outputs: list[str | None] = [
        None,
        "Available models:\n  - grok-4.5\n",
        "unparseable output",
    ]
    for bad in bad_outputs:
        monkeypatch.setattr(
            grok_mod,
            "_run_grok_models",
            lambda cmd, bad=bad, **_: bad,
        )
        monkeypatch.setattr(grok_mod, "_DEFAULT_MODEL_CACHE", {})
        assert grok_mod._probe_grok_default_model("grok") is None


# --- precedence + stream-start metadata ---


def test_grok_started_meta_prefers_configured_over_probed_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import untether.runners.grok as grok_mod

    monkeypatch.setattr(
        grok_mod, "_probe_grok_default_model", lambda cmd: "probed-default"
    )
    runner = GrokRunner(extra_args=[], model="grok-configured")
    events, _ = _translate_events(runner, [b'{"type":"text","data":"hi"}'])
    assert isinstance(events[0], StartedEvent)
    assert events[0].meta is not None
    assert events[0].meta["model"] == "grok-configured"


def test_grok_started_meta_shows_probed_cli_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import untether.runners.grok as grok_mod

    monkeypatch.setattr(grok_mod, "_probe_grok_default_model", lambda cmd: "glm-probed")
    runner = GrokRunner(extra_args=[])
    events, _ = _translate_events(runner, [b'{"type":"text","data":"hi"}'])
    assert isinstance(events[0], StartedEvent)
    assert events[0].meta is not None
    assert events[0].meta["model"] == "glm-probed"


def test_grok_probed_default_never_passed_as_model_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Display-only: the probed CLI default must never become a ``-m`` argument.
    import untether.runners.grok as grok_mod

    monkeypatch.setattr(
        grok_mod, "_probe_grok_default_model", lambda cmd: "probed-default"
    )
    runner = GrokRunner(extra_args=[])
    args = runner.build_args("hi", None, state=_grok_state())
    assert "-m" not in args
