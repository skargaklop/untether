"""Compact support model and helpers.

Runners MAY implement ``compact_support()`` and ``compact()`` to participate
in Untether's ``/compact`` command. The :func:`get_compact_support` helper
gracefully handles runners (including third-party plugins) that do not
implement these methods, returning :data:`COMPACT_NONE`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from .model import EngineId, ResumeToken, UntetherEvent

CompactMode = Literal["slash_prompt", "native_api", "acp", "handoff_only", "none"]


@dataclass(frozen=True, slots=True)
class CompactSupport:
    """Declares a runner's compaction capability."""

    mode: CompactMode
    accepts_instructions: bool
    true_compaction: bool
    note: str | None = None


COMPACT_NONE = CompactSupport(
    mode="none",
    accepts_instructions=False,
    true_compaction=False,
    note="compaction is not supported by this runner",
)


class CompactUnsupportedError(RuntimeError):
    """Raised when a runner cannot compact."""


@runtime_checkable
class CompactRunner(Protocol):
    def compact_support(self) -> CompactSupport: ...

    def compact(
        self,
        resume: ResumeToken,
        instructions: str | None = None,
    ) -> AsyncIterator[UntetherEvent]: ...


def normalize_instructions(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


def compact_prompt(instructions: str | None) -> str:
    """Build the ``/compact`` slash prompt with optional instructions."""
    text = normalize_instructions(instructions)
    return "/compact" if text is None else f"/compact {text}"


def handoff_prompt(instructions: str | None) -> str:
    """Build a handoff-summary prompt (not real compaction).

    Used for runners like ``agy`` that have no native compact command.
    """
    base = """Create a handoff summary for continuing this session.

This is not real context compaction. Do not claim that the session was compacted.

Preserve:
- current user goal and latest instruction
- active project and relevant paths
- decisions already made
- files changed or inspected
- commands run and verification results
- open blockers, risks, and next steps

Write a concise handoff summary for the next agent turn."""
    text = normalize_instructions(instructions)
    return base if text is None else f"{base}\n\nUser focus:\n{text}"


def handoff_seed_prompt(summary: str) -> str:
    """Build the seed prompt that starts a NEW session with a handoff summary.

    The full summary is embedded verbatim (never truncated). The new agent
    is asked to acknowledge briefly and wait for the user.
    """
    return (
        "You are continuing work from a previous session. "
        "The handoff summary below is your memory of that work. "
        "Reply with a one-line acknowledgement and wait "
        "for the user's next instruction.\n\n"
        "--- handoff summary ---\n"
        f"{summary}\n"
        "--- end summary ---"
    )


def warn_if_dropping_instructions(
    engine: EngineId, instructions: str | None
) -> str | None:
    """Return a user-visible warning when instructions will be dropped."""
    text = normalize_instructions(instructions)
    if text is None:
        return None
    return (
        f"{engine} compact instructions are not supported yet; "
        "running compact without the supplied instructions."
    )


def get_compact_support(runner: object) -> CompactSupport:
    """Return the runner's compact support, or :data:`COMPACT_NONE`.

    Uses ``getattr`` so older third-party runner plugins without
    ``compact_support()`` still behave as ``mode="none"``.
    """
    method = getattr(runner, "compact_support", None)
    if method is None:
        return COMPACT_NONE
    result = method()
    if not isinstance(result, CompactSupport):
        raise TypeError("compact_support() must return CompactSupport")
    return result
