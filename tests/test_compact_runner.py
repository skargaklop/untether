"""Tests for the compact mixins (SlashCompactMixin, HandoffCompactMixin).

These test that the mixins correctly delegate to ``run()`` with the right
prompt, that instructions are accepted/dropped correctly, and that events
flow through.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import pytest

from untether.events import EventFactory
from untether.model import CompletedEvent, EngineId, ResumeToken, UntetherEvent
from untether.runners._compact_mixin import (
    HandoffCompactMixin,
    SlashCompactMixin,
)


@dataclass
class MockSlashRunner(SlashCompactMixin):
    """Runner that records the prompt it receives."""

    engine: EngineId = "codex"
    calls: list[str] = field(default_factory=list)

    async def run(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]:
        self.calls.append(prompt)
        factory = EventFactory(self.engine)
        if resume is not None:
            yield factory.started(resume, title="compact")
            yield factory.completed_ok(answer="done", resume=resume)


@dataclass
class MockHandoffRunner(HandoffCompactMixin):
    engine: EngineId = "agy"
    calls: list[str] = field(default_factory=list)

    async def run(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]:
        self.calls.append(prompt)
        factory = EventFactory(self.engine)
        if resume is not None:
            yield factory.started(resume, title="handoff")
            yield factory.completed_ok(answer="summary", resume=resume)


class TestSlashCompactMixin:
    def test_compact_support(self) -> None:
        runner = MockSlashRunner()
        support = runner.compact_support()
        assert support.mode == "slash_prompt"
        assert support.accepts_instructions is True
        assert support.true_compaction is True

    @pytest.mark.anyio
    async def test_compact_no_instructions(self) -> None:
        runner = MockSlashRunner()
        resume = ResumeToken(engine="codex", value="sess-123")
        events = [e async for e in runner.compact(resume, None)]
        assert runner.calls == ["/compact"]
        assert len(events) == 2  # started + completed
        assert events[0].type == "started"
        assert events[1].type == "completed"
        last = events[1]
        assert isinstance(last, CompletedEvent)
        assert last.ok is True

    @pytest.mark.anyio
    async def test_compact_with_instructions(self) -> None:
        runner = MockSlashRunner()
        resume = ResumeToken(engine="codex", value="sess-123")
        async for _ in runner.compact(resume, "focus on tests"):
            pass
        assert runner.calls == ["/compact focus on tests"]

    @pytest.mark.anyio
    async def test_instructions_dropped_when_not_accepted(self) -> None:
        runner = MockSlashRunner()
        runner.compact_accepts_instructions = False
        resume = ResumeToken(engine="codex", value="sess-123")
        async for _ in runner.compact(resume, "focus on tests"):
            pass
        # Instructions should be dropped — prompt is just "/compact"
        assert runner.calls == ["/compact"]


class TestHandoffCompactMixin:
    def test_compact_support(self) -> None:
        runner = MockHandoffRunner()
        support = runner.compact_support()
        assert support.mode == "handoff_only"
        assert support.accepts_instructions is True
        assert support.true_compaction is False
        assert "Handoff summary" in (support.note or "")

    @pytest.mark.anyio
    async def test_compact_uses_handoff_prompt(self) -> None:
        runner = MockHandoffRunner()
        resume = ResumeToken(engine="agy", value="sess-456")
        async for _ in runner.compact(resume, None):
            pass
        assert len(runner.calls) == 1
        assert "Create a handoff summary" in runner.calls[0]

    @pytest.mark.anyio
    async def test_compact_with_instructions(self) -> None:
        runner = MockHandoffRunner()
        resume = ResumeToken(engine="agy", value="sess-456")
        async for _ in runner.compact(resume, "focus on API"):
            pass
        assert "User focus:" in runner.calls[0]
        assert "focus on API" in runner.calls[0]
