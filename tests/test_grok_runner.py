"""Focused Grok failure safety and shared retry regressions."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from untether.model import CompletedEvent, ResumeToken, UntetherEvent
from untether.runners.grok import GrokRunner, GrokStreamState


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
            engine="grok", ok=False, answer="", resume=None,
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
