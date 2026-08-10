"""Tests for the compact support model and helpers.

Exercises the protocol and helper functions directly, without any subprocess,
runner, or Telegram loop.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import cast

import pytest

from untether.compact import (
    COMPACT_NONE,
    CompactRunner,
    CompactSupport,
    compact_prompt,
    get_compact_support,
    handoff_prompt,
    handoff_seed_prompt,
    normalize_instructions,
    warn_if_dropping_instructions,
)
from untether.model import ResumeToken, UntetherEvent


class TestNormalizeInstructions:
    def test_none(self) -> None:
        assert normalize_instructions(None) is None

    def test_empty(self) -> None:
        assert normalize_instructions("") is None

    def test_whitespace(self) -> None:
        assert normalize_instructions("   \n\t  ") is None

    def test_strips(self) -> None:
        assert normalize_instructions("  hello  ") == "hello"


class TestCompactPrompt:
    def test_no_instructions(self) -> None:
        assert compact_prompt(None) == "/compact"

    def test_empty_instructions(self) -> None:
        assert compact_prompt("") == "/compact"

    def test_with_instructions(self) -> None:
        assert compact_prompt("focus on tests") == "/compact focus on tests"

    def test_strips_whitespace(self) -> None:
        assert compact_prompt("  focus  ") == "/compact focus"


class TestHandoffPrompt:
    def test_no_instructions(self) -> None:
        result = handoff_prompt(None)
        assert "Create a handoff summary" in result
        assert "not real context compaction" in result

    def test_with_instructions(self) -> None:
        result = handoff_prompt("focus on API design")
        assert "Create a handoff summary" in result
        assert "User focus:" in result
        assert "focus on API design" in result


class TestHandoffSeedPrompt:
    def test_embeds_summary(self) -> None:
        summary = "## Session Summary\n\nWe worked on tests."
        result = handoff_seed_prompt(summary)
        assert "You are continuing work" in result
        assert summary in result
        assert "--- handoff summary ---" in result
        assert "--- end summary ---" in result

    def test_acknowledgement_instruction(self) -> None:
        result = handoff_seed_prompt("summary")
        assert "one-line acknowledgement" in result
        assert "wait" in result


class TestWarnIfDroppingInstructions:
    def test_none_instructions(self) -> None:
        assert warn_if_dropping_instructions("codex", None) is None

    def test_empty_instructions(self) -> None:
        assert warn_if_dropping_instructions("codex", "") is None

    def test_with_instructions(self) -> None:
        result = warn_if_dropping_instructions("codex", "focus")
        assert result is not None
        assert "codex" in result
        assert "not supported yet" in result


class TestGetCompactSupport:
    def test_none_attribute(self) -> None:
        class NoCompact:
            pass

        assert get_compact_support(NoCompact()) == COMPACT_NONE

    def test_returns_compact_support(self) -> None:
        class WithCompact:
            def compact_support(self) -> CompactSupport:
                return CompactSupport(
                    mode="slash_prompt",
                    accepts_instructions=True,
                    true_compaction=True,
                )

        result = get_compact_support(WithCompact())
        assert result.mode == "slash_prompt"
        assert result.accepts_instructions is True
        assert result.true_compaction is True

    def test_wrong_return_type_raises(self) -> None:
        class BadCompact:
            def compact_support(self) -> str:
                return "not a CompactSupport"

        with pytest.raises(TypeError, match="must return CompactSupport"):
            get_compact_support(BadCompact())


class TestCompactRunnerProtocol:
    def test_structural_check(self) -> None:
        @dataclass
        class MyRunner:
            def compact_support(self) -> CompactSupport:
                return CompactSupport(
                    mode="slash_prompt",
                    accepts_instructions=True,
                    true_compaction=True,
                )

            async def compact(
                self,
                resume: ResumeToken,
                instructions: str | None = None,
            ) -> AsyncIterator[UntetherEvent]:
                if False:
                    yield cast(UntetherEvent, None)

        assert isinstance(MyRunner(), CompactRunner)

    def test_missing_methods_not_compact_runner(self) -> None:
        class NotARunner:
            pass

        assert not isinstance(NotARunner(), CompactRunner)
