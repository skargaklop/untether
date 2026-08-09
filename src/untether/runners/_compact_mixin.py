"""Mixin for runners that compact via ``/compact`` slash prompt.

Runners like ``claude``, ``pi``, and ``codex`` support ``/compact`` as a
native slash command. This mixin delegates ``compact()`` to ``run()``,
optionally passing instructions as part of the prompt.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from ..compact import CompactSupport, compact_prompt, handoff_prompt
from ..model import ResumeToken, UntetherEvent


@runtime_checkable
class _CompactRunner(Protocol):
    """Structural contract for runners using the compact mixins."""

    compact_accepts_instructions: bool

    def run(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]: ...


class SlashCompactMixin:
    """Delegate compaction to ``run("/compact [instructions]", resume)``."""

    compact_accepts_instructions: bool = True
    compact_true_compaction: bool = True

    def compact_support(self) -> CompactSupport:
        return CompactSupport(
            mode="slash_prompt",
            accepts_instructions=self.compact_accepts_instructions,
            true_compaction=self.compact_true_compaction,
        )

    async def compact(
        self: _CompactRunner,
        resume: ResumeToken,
        instructions: str | None = None,
    ) -> AsyncIterator[UntetherEvent]:
        if instructions and not self.compact_accepts_instructions:
            instructions = None
        async for event in self.run(compact_prompt(instructions), resume):
            yield event


class HandoffCompactMixin:
    """Delegate compaction to ``run(handoff_prompt(instructions), resume)``.

    Used by runners without native compact (agy, omp, grok). Produces a
    handoff summary, not real context reduction.
    """

    compact_accepts_instructions: bool = True
    compact_handoff_note: str = "Handoff summary only; not real compaction"

    def compact_support(self) -> CompactSupport:
        return CompactSupport(
            mode="handoff_only",
            accepts_instructions=self.compact_accepts_instructions,
            true_compaction=False,
            note=self.compact_handoff_note,
        )

    async def compact(
        self: _CompactRunner,
        resume: ResumeToken,
        instructions: str | None = None,
    ) -> AsyncIterator[UntetherEvent]:
        async for event in self.run(handoff_prompt(instructions), resume):
            yield event
