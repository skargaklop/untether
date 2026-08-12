import os
import sys
from pathlib import Path, PureWindowsPath
from typing import Any, cast
from unittest.mock import patch

import anyio
import pytest

from untether.model import ActionEvent, CompletedEvent, ResumeToken, StartedEvent
from untether.runners.pi import (
    ENGINE,
    PiRunner,
    PiStreamState,
    _default_session_dir,
    translate_pi_event,
)
from untether.schemas import pi as pi_schema


def _load_fixture(name: str) -> list[pi_schema.PiEvent]:
    path = Path(__file__).parent / "fixtures" / name
    events: list[pi_schema.PiEvent] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            decoded = pi_schema.decode_event(line)
        except Exception as exc:
            raise AssertionError(f"{name} contained unparseable line: {line}") from exc
        events.append(decoded)
    return events


def test_pi_resume_format_and_extract(tmp_path: Path) -> None:
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    token = ResumeToken(engine=ENGINE, value="session.jsonl")

    assert runner.format_resume(token) == "`pi --session session.jsonl`"
    assert runner.extract_resume("`pi --session session.jsonl`") == token
    assert runner.extract_resume('pi --session "session.jsonl"') == token
    assert runner.extract_resume("`codex resume sid`") is None

    spaced = ResumeToken(engine=ENGINE, value="pi session.jsonl")
    assert runner.format_resume(spaced) == '`pi --session "pi session.jsonl"`'
    assert runner.extract_resume('`pi --session "pi session.jsonl"`') == spaced


def test_translate_success_fixture() -> None:
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    events: list = []
    for event in _load_fixture("pi_stream_success.jsonl"):
        events.extend(translate_pi_event(event, title="pi", meta=None, state=state))

    assert isinstance(events[0], StartedEvent)
    started = next(evt for evt in events if isinstance(evt, StartedEvent))
    assert started.meta is None

    action_events = [evt for evt in events if isinstance(evt, ActionEvent)]
    assert len(action_events) == 4

    started_actions = {
        (evt.action.id, evt.phase): evt
        for evt in action_events
        if evt.phase == "started"
    }
    assert started_actions[("tool_1", "started")].action.kind == "command"
    write_action = started_actions[("tool_2", "started")].action
    assert write_action.kind == "file_change"
    assert write_action.detail["changes"][0]["path"] == "notes.md"

    completed_actions = {
        (evt.action.id, evt.phase): evt
        for evt in action_events
        if evt.phase == "completed"
    }
    assert completed_actions[("tool_1", "completed")].ok is True
    assert completed_actions[("tool_2", "completed")].ok is True

    completed = next(evt for evt in events if isinstance(evt, CompletedEvent))
    assert events[-1] == completed
    assert completed.ok is True
    assert completed.resume == started.resume
    assert completed.answer == "Done. Added notes.md."


def test_started_event_gains_model_from_message_end_when_no_config_override() -> None:
    """#225: when no --model override and no pi.model in untether.toml,
    meta["model"] was empty and the footer showed only 'dir: pi-test'.
    Pi's message_end event carries the actual model (e.g. 'gpt-4o-mini');
    the runner now emits a supplementary StartedEvent so the footer picks
    it up via ProgressTracker.note_event's meta merge.
    """
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    events: list = []
    # Simulate the default-config path: meta=None (no model available yet).
    for event in _load_fixture("pi_stream_success.jsonl"):
        events.extend(translate_pi_event(event, title="pi", meta=None, state=state))

    started_events = [evt for evt in events if isinstance(evt, StartedEvent)]
    # Two StartedEvents: the initial (from SessionHeader, meta=None) and the
    # supplementary (from message_end, meta={"model": ...}).
    assert len(started_events) == 2, f"got {len(started_events)}: {started_events!r}"
    assert started_events[0].meta is None
    assert started_events[1].meta is not None
    assert started_events[1].meta["model"] == "gpt-4o-mini"
    assert started_events[1].meta.get("provider") == "openai"
    # Same resume token on both — this is the same session.
    assert started_events[0].resume == started_events[1].resume
    # Latch prevents further supplementary StartedEvents on subsequent message_ends.
    assert state.jsonl_model_emitted is True


def test_started_event_skips_jsonl_model_when_config_model_present() -> None:
    """#225: when the user has set pi.model in config, meta already carries
    the model at SessionHeader time. Don't override with the JSONL-reported
    value (user-configured > engine-reported)."""
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    events: list = []
    config_meta: dict = {"model": "user-configured-model", "provider": "openai"}
    for event in _load_fixture("pi_stream_success.jsonl"):
        events.extend(
            translate_pi_event(event, title="pi", meta=config_meta, state=state)
        )

    started_events = [evt for evt in events if isinstance(evt, StartedEvent)]
    # Only the initial StartedEvent — no supplementary emission.
    assert len(started_events) == 1
    assert started_events[0].meta == config_meta
    assert state.jsonl_model_emitted is False


def test_translate_error_fixture() -> None:
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    events: list = []
    for event in _load_fixture("pi_stream_error.jsonl"):
        events.extend(translate_pi_event(event, title="pi", meta=None, state=state))

    completed = next(evt for evt in events if isinstance(evt, CompletedEvent))
    assert completed.ok is False
    assert completed.error == "Upstream error"
    assert completed.answer == "Request failed."


def test_session_id_promotion_from_stdout() -> None:
    state = PiStreamState(
        resume=ResumeToken(engine=ENGINE, value="session.jsonl"),
        allow_id_promotion=True,
    )
    events = translate_pi_event(
        pi_schema.SessionHeader(
            id="ccd569e0-4e1b-4c7d-a981-637ed4107310",
            version=3,
            timestamp="2026-01-13T00:33:34.702Z",
            cwd="/tmp",
        ),
        title="pi",
        meta=None,
        state=state,
    )
    started = next(evt for evt in events if isinstance(evt, StartedEvent))
    assert started.resume.value == "ccd569e0-4e1b-4c7d-a981-637ed4107310"


def test_extract_resume_keeps_session_path(tmp_path: Path) -> None:
    session_path = tmp_path / "session.jsonl"
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    token = runner.extract_resume(f"pi --session {session_path}")
    assert token is not None
    assert token.value == str(session_path)


@pytest.mark.anyio
async def test_run_keeps_resume_path(tmp_path: Path) -> None:
    session_path = tmp_path / "session.jsonl"
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    seen_resume: ResumeToken | None = None

    async def run_stub(_prompt: str, resume: ResumeToken | None):
        nonlocal seen_resume
        seen_resume = resume
        yield CompletedEvent(
            engine=ENGINE,
            resume=resume,
            ok=True,
            answer="ok",
        )

    runner.run_impl = cast(Any, run_stub)
    resume = ResumeToken(engine=ENGINE, value=str(session_path))
    async for _event in runner.run("test", resume):
        pass
    assert seen_resume is not None
    assert seen_resume.value == str(session_path)


@pytest.mark.anyio
async def test_run_serializes_same_session() -> None:
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    gate = anyio.Event()
    in_flight = 0
    max_in_flight = 0

    async def run_stub(*_args, **_kwargs):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        try:
            await gate.wait()
            yield CompletedEvent(
                engine=ENGINE,
                resume=ResumeToken(engine=ENGINE, value="session.jsonl"),
                ok=True,
                answer="ok",
            )
        finally:
            in_flight -= 1

    runner.run_impl = cast(Any, run_stub)

    async def drain(prompt: str, resume: ResumeToken | None) -> None:
        async for _event in runner.run(prompt, resume):
            pass

    token = ResumeToken(engine=ENGINE, value="session.jsonl")
    async with anyio.create_task_group() as tg:
        tg.start_soon(drain, "a", token)
        tg.start_soon(drain, "b", token)
        await anyio.lowlevel.checkpoint()  # ty: ignore[unresolved-attribute]
        gate.set()
    assert max_in_flight == 1


def test_session_path_prefers_run_base_dir(tmp_path: Path) -> None:
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    project_cwd = Path("/project")
    session_root = tmp_path / "sessions"

    with (
        patch("untether.runners.pi.get_run_base_dir", return_value=project_cwd),
        patch(
            "untether.runners.pi._default_session_dir",
            return_value=session_root,
        ) as default_session_dir,
    ):
        session_path = runner._new_session_path()

    default_session_dir.assert_called_once_with(project_cwd)
    assert str(session_root) in session_path
    # Windows does not expose POSIX directory permission bits.
    assert session_root.exists()
    if os.name != "nt":
        assert (session_root.stat().st_mode & 0o777) == 0o700


def test_session_path_tightens_existing_dir_perms(tmp_path: Path) -> None:
    """#207: pre-existing dir with looser perms gets chmod'd to 0o700."""
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    project_cwd = Path("/project")
    session_root = tmp_path / "sessions"
    session_root.mkdir(mode=0o755)
    if os.name != "nt":
        assert (session_root.stat().st_mode & 0o777) == 0o755

    with (
        patch("untether.runners.pi.get_run_base_dir", return_value=project_cwd),
        patch(
            "untether.runners.pi._default_session_dir",
            return_value=session_root,
        ),
    ):
        runner._new_session_path()

    if os.name != "nt":
        assert (session_root.stat().st_mode & 0o777) == 0o700


def test_session_path_sanitizes_windows_separators() -> None:
    cwd = PureWindowsPath("C:\\foo\\bar")
    session_dir = _default_session_dir(cwd)
    name = session_dir.name
    assert "\\" not in name
    assert ":" not in name


# ---------------------------------------------------------------------------
# Issue #147 — /continue should allow session ID promotion
# ---------------------------------------------------------------------------


def test_continue_allows_id_promotion() -> None:
    """new_state() with is_continue=True sets allow_id_promotion=True."""
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    continue_token = ResumeToken(engine=ENGINE, value="", is_continue=True)
    state = runner.new_state("prompt", continue_token)
    assert state.allow_id_promotion is True


def test_normal_resume_does_not_allow_id_promotion() -> None:
    """new_state() with a normal resume token keeps allow_id_promotion=False."""
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    resume_token = ResumeToken(engine=ENGINE, value="ses_existing")
    state = runner.new_state("prompt", resume_token)
    assert state.allow_id_promotion is False


def test_continue_session_id_promoted_from_header() -> None:
    """During /continue, SessionHeader promotes the session ID into resume token."""
    continue_token = ResumeToken(engine=ENGINE, value="", is_continue=True)
    state = PiStreamState(resume=continue_token, allow_id_promotion=True)

    events = translate_pi_event(
        pi_schema.SessionHeader(
            id="ccd569e0-4e1b-4c7d-a981-637ed4107310",
            version=3,
            timestamp="2026-01-13T00:33:34.702Z",
            cwd="/tmp",
        ),
        title="pi",
        meta=None,
        state=state,
    )
    started = next(e for e in events if isinstance(e, StartedEvent))
    assert started.resume.value == "ccd569e0-4e1b-4c7d-a981-637ed4107310"
    assert started.resume.value != ""


# ---------------------------------------------------------------------------
# #565 — surface Pi stderr / diagnose silent rc=0 no-agent_end exits
# ---------------------------------------------------------------------------


def _pi_runner() -> PiRunner:
    return PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)


def test_stream_end_zero_events_reports_startup_failure() -> None:
    """#565: rc=0 with no translated events (state.started False) is a
    startup/early-exit crash, not a truncated stream — say so, and on a resumed
    run hint that the session may have failed to load (the real, transient cause
    behind the original report: MCP servers cold during resume rehydrate)."""
    runner = _pi_runner()
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    # state.started stays False → Pi produced nothing.
    events = runner.stream_end_events(
        resume=ResumeToken(engine=ENGINE, value="session.jsonl"),
        found_session=None,
        state=state,
        stderr_lines=["Error: MCP server 'foo' failed to connect", "exiting"],
    )
    assert len(events) == 1
    completed = events[0]
    assert isinstance(completed, CompletedEvent)
    assert completed.ok is False
    assert completed.error is not None
    assert "produced no events" in completed.error
    # resume was non-None → resumed hint present
    assert "failed to load on resume" in completed.error
    # stderr tail surfaced (the whole point of the fix)


def test_stream_end_with_events_reports_truncated_stream() -> None:
    """#565: events were seen but no agent_end → keep the 'truncated stream'
    wording, not the zero-events startup message."""
    runner = _pi_runner()
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    state.started = True  # at least one event translated
    events = runner.stream_end_events(
        resume=None,
        found_session=None,
        state=state,
        stderr_lines=None,
    )
    completed = events[0]
    assert isinstance(completed, CompletedEvent)
    assert completed.error is not None
    assert "finished without an agent_end event" in completed.error
    assert "produced no events" not in completed.error


def test_stream_end_appends_stderr_excerpt_when_present() -> None:
    runner = _pi_runner()
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    state.started = True
    events = runner.stream_end_events(
        resume=None,
        found_session=None,
        state=state,
        stderr_lines=["traceback line 1", "RuntimeError: kaboom"],
    )
    assert isinstance(events[0], CompletedEvent)
    assert events[0].error is not None
    assert "RuntimeError: kaboom" in events[0].error


def test_build_args_resume_uses_session_path_verbatim() -> None:
    """#565 regression guard: the resume value passed to --session must be the
    session *path* Untether owns, NOT a mangled short id (the rejected fix A).
    """
    runner = _pi_runner()
    path = "/home/nathan/.pi/agent/sessions/--proj--/2026_abc.jsonl"
    token = ResumeToken(engine=ENGINE, value=path)
    state = PiStreamState(resume=token)
    args = runner.build_args("hello", token, state=state)
    assert "--session" in args
    assert args[args.index("--session") + 1] == path


# ---------------------------------------------------------------------------
# #460 — AutoRetry event translation
# ---------------------------------------------------------------------------


def _started_state() -> PiStreamState:
    state = PiStreamState(resume=ResumeToken(engine=ENGINE, value="session.jsonl"))
    state.started = True  # skip the implicit StartedEvent so we isolate actions
    return state


def test_auto_retry_start_translates_to_note_action() -> None:
    state = _started_state()
    events = translate_pi_event(
        pi_schema.AutoRetryStart(attempt=2, maxAttempts=5, delayMs=1500),
        title="pi",
        meta=None,
        state=state,
    )
    assert len(events) == 1
    evt = events[0]
    assert isinstance(evt, ActionEvent)
    assert evt.phase == "started"
    assert evt.action.kind == "note"
    assert evt.action.id == "retry_1"
    assert "attempt 2/5" in evt.action.title
    assert "~1.5s delay" in evt.action.title
    assert state.retry_action_id == "retry_1"


def test_auto_retry_start_omits_null_fields_gracefully() -> None:
    state = _started_state()
    events = translate_pi_event(
        pi_schema.AutoRetryStart(),
        title="pi",
        meta=None,
        state=state,
    )
    assert isinstance(events[0], ActionEvent)
    assert events[0].action.title == "retrying provider"


def test_auto_retry_end_success_completes_same_action() -> None:
    state = _started_state()
    translate_pi_event(
        pi_schema.AutoRetryStart(attempt=1), title="pi", meta=None, state=state
    )
    events = translate_pi_event(
        pi_schema.AutoRetryEnd(success=True), title="pi", meta=None, state=state
    )
    assert len(events) == 1
    evt = events[0]
    assert isinstance(evt, ActionEvent)
    assert evt.phase == "completed"
    assert evt.ok is True
    assert evt.action.id == "retry_1"  # stable across start/end
    assert evt.action.title == "retry succeeded"
    assert state.retry_action_id is None


def test_auto_retry_end_failure_includes_final_error() -> None:
    state = _started_state()
    translate_pi_event(pi_schema.AutoRetryStart(), title="pi", meta=None, state=state)
    events = translate_pi_event(
        pi_schema.AutoRetryEnd(success=False, finalError="503 from provider"),
        title="pi",
        meta=None,
        state=state,
    )
    evt = events[0]
    assert isinstance(evt, ActionEvent)
    assert evt.phase == "completed"
    assert evt.ok is False
    assert evt.action.title == "retry exhausted: 503 from provider"


def test_multiple_retries_have_stable_distinct_ids() -> None:
    state = _started_state()
    s1 = translate_pi_event(
        pi_schema.AutoRetryStart(attempt=1), title="pi", meta=None, state=state
    )
    e1 = translate_pi_event(
        pi_schema.AutoRetryEnd(success=False), title="pi", meta=None, state=state
    )
    s2 = translate_pi_event(
        pi_schema.AutoRetryStart(attempt=2), title="pi", meta=None, state=state
    )
    e2 = translate_pi_event(
        pi_schema.AutoRetryEnd(success=True), title="pi", meta=None, state=state
    )
    assert isinstance(s1[0], ActionEvent)
    assert isinstance(e1[0], ActionEvent)
    assert isinstance(s2[0], ActionEvent)
    assert isinstance(e2[0], ActionEvent)
    assert s1[0].action.id == "retry_1"
    assert e1[0].action.id == "retry_1"
    assert s2[0].action.id == "retry_2"
    assert e2[0].action.id == "retry_2"


# ---------------------------------------------------------------------------
# PiRunner.command() — regression: must not raise NotImplementedError.
# The base JsonlSubprocessRunner.command() raises NotImplementedError; every
# concrete runner overrides it. Pi lost this override during the Takopi port,
# so spawning Pi crashed at runner.py command_args() resolution.
# ---------------------------------------------------------------------------


def test_pi_command_does_not_raise() -> None:
    """PiRunner.command() must return a resolved executable string.

    On Windows the Takopi contract is ``pi.cmd``; elsewhere ``pi``. The
    override must never fall through to the base ``NotImplementedError``.
    """
    runner = PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None)
    cmd = runner.command()
    assert isinstance(cmd, str)
    assert cmd  # non-empty


def test_pi_command_default_matches_platform() -> None:
    """Default command mirrors the Takopi contract: pi.cmd on Windows."""
    from untether.runners.pi import _default_pi_cmd

    cmd = _default_pi_cmd()
    if sys.platform == "win32":
        assert cmd == "pi.cmd"
    else:
        assert cmd == "pi"


def test_pi_build_runner_honors_cmd_override() -> None:
    """``[pi] cmd = "..."`` overrides the default executable resolution."""
    from pathlib import Path

    from untether.runners.pi import PiRunner, build_runner

    runner = cast(
        PiRunner, build_runner({"cmd": "/usr/local/bin/pi-special"}, Path("/x.toml"))
    )
    # build_runner returns a Runner (cast); the concrete type is PiRunner.
    assert runner.command() == "/usr/local/bin/pi-special"


def test_pi_build_runner_default_when_no_cmd() -> None:
    """Without a cmd override, the platform default is used."""
    from pathlib import Path

    from untether.runners.pi import PiRunner, build_runner

    runner = cast(PiRunner, build_runner({}, Path("/x.toml")))
    cmd = runner.command()
    if sys.platform == "win32":
        assert cmd == "pi.cmd"
    else:
        assert cmd == "pi"
