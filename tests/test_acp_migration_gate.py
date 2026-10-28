"""Migration-gate behaviour of ``AcpRunner.run`` (untether-native-migration-review).

Covers three contracts of the gate wrap around the resumed/new session
stream, using a subclass that overrides ``_run`` with a fake async
generator (no subprocess, no network):

1. A resumed session whose gate is already held never enters ``_run``.
2. A newly-created session acquires the gate on ``StartedEvent`` while
   the stream keeps yielding.
3. Closing the stream's async generator releases the gate file.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from untether.events import EventFactory
from untether.model import ResumeToken, StartedEvent
from untether.runners.acp.runner import AcpRunner
from untether.session_migration.session_gate import (
    SourceBusy,
    _gate_path,
    session_gate,
)

ENGINE = "gate-test-engine"


@dataclass(slots=True)
class FakeRunner(AcpRunner):
    """AcpRunner whose ``_run`` replays canned events — no subprocess."""

    events: list[Any] = field(default_factory=list)
    entered: bool = False
    closed: bool = False

    async def _run(self, prompt: str, resume: ResumeToken | None) -> AsyncIterator[Any]:
        self.entered = True
        try:
            for event in self.events:
                yield event
        finally:
            self.closed = True

    def _peer(self) -> Any:  # pragma: no cover - must never be reached
        raise AssertionError("FakeRunner must not spawn a peer process")


def _runner(*events: Any) -> FakeRunner:
    return FakeRunner(
        engine=ENGINE,
        command="unused",
        peer_factory=None,
        events=list(events),
    )


async def _drain(runner: AcpRunner, resume: ResumeToken | None) -> list[Any]:
    seen: list[Any] = []
    with contextlib.suppress(StopAsyncIteration):
        seen.extend([event async for event in runner.run("hello", resume)])
    return seen


@pytest.mark.anyio
async def test_resumed_gate_held_prevents_run_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the resumed token's gate is taken, SourceBusy fires before _run."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path))
    resume = ResumeToken(engine=ENGINE, value="native-123")
    fake = _runner("never-yielded")

    with (
        session_gate(root=tmp_path, engine_id=ENGINE, native_id=resume.value),
        pytest.raises(SourceBusy),
    ):
        await _drain(fake, resume)

    assert fake.entered is False, "gate-busy resume must not enter _run"


@pytest.mark.anyio
async def test_started_event_gate_held_while_stream_yields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """New sessions acquire the gate on StartedEvent; it stays held during the stream."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path))
    token = ResumeToken(engine=ENGINE, value="native-new")
    started = EventFactory(ENGINE).started(token)
    gate_file = _gate_path(tmp_path, ENGINE, token.value)

    assert not gate_file.exists(), "gate must not exist before the stream starts"
    fake = _runner(started, "second-event")

    seen: list[Any] = []
    async for event in fake.run("hello", None):
        seen.append(event)
        assert gate_file.exists(), "gate must be held while the stream yields"

    assert seen == [started, "second-event"]
    assert fake.entered is True


@pytest.mark.anyio
async def test_generator_close_releases_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Explicitly closing the run generator releases the gate file."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path))
    token = ResumeToken(engine=ENGINE, value="native-close")
    started = EventFactory(ENGINE).started(token)
    gate_file = _gate_path(tmp_path, ENGINE, token.value)
    fake = _runner(started, "tail-event")

    agen = fake.run("hello", None)
    try:
        first = await agen.__anext__()
        assert isinstance(first, StartedEvent)
        assert gate_file.exists()
    finally:
        await agen.aclose()

    assert fake.closed, "aclose must tear down the inner stream"
    assert not gate_file.exists(), "aclose must release the migration gate"
    # Fail-closed release is complete: the same identity can re-acquire.
    with session_gate(root=tmp_path, engine_id=ENGINE, native_id=token.value):
        pass
