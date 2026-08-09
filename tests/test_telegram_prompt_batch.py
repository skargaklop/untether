"""Pure unit tests for prompt_batch decision helpers.

These test the batching decision logic without any Telegram loop, asyncio,
or subprocess — just input text and the boolean/assembly contract.
"""

from __future__ import annotations

from untether.telegram.prompt_batch import (
    PromptBatchPart,
    PromptBatchSettings,
    is_sticky_goal_args,
    is_sticky_plan_args,
    join_prompt_parts,
    should_batch_text,
)


def _settings(*, enabled: bool = True) -> PromptBatchSettings:
    return PromptBatchSettings(
        enabled=enabled,
        max_messages=8,
        max_chars=120_000,
        separator="blank_line",
    )


class TestShouldBatchText:
    def test_disabled_never_batches(self) -> None:
        assert should_batch_text("hello", settings=_settings(enabled=False)) is False

    def test_empty_text_never_batches(self) -> None:
        assert should_batch_text("", settings=_settings()) is False
        assert should_batch_text("   ", settings=_settings()) is False

    def test_plain_text_batches(self) -> None:
        assert should_batch_text("hello world", settings=_settings()) is True

    def test_control_command_never_batches(self) -> None:
        for cmd in (
            "/cancel",
            "/new",
            "/ctx",
            "/agent",
            "/model claude",
            "/reasoning",
            "/listen all",
            "/file put foo.txt",
            "/topic off",
            "/restart",
            "/verbose on",
            "/stats",
            "/planmode on",
            "/health",
            "/ping",
            "/config",
            "/export json",
            "/browse",
            "/auth codex",
            "/at 5m do stuff",
        ):
            assert should_batch_text(cmd, settings=_settings()) is False, cmd

    def test_sticky_plan_batches(self) -> None:
        assert should_batch_text("/plan", settings=_settings()) is False
        assert should_batch_text("/plan on", settings=_settings()) is False
        assert should_batch_text("/plan off", settings=_settings()) is False
        assert should_batch_text("/plan clear", settings=_settings()) is False
        assert should_batch_text("/plan show", settings=_settings()) is False

    def test_plan_prompt_batches(self) -> None:
        assert should_batch_text("/plan design the API", settings=_settings()) is True
        assert (
            should_batch_text("/plan /agy implement feature", settings=_settings())
            is True
        )

    def test_sticky_goal_batches(self) -> None:
        assert should_batch_text("/goal", settings=_settings()) is False
        assert should_batch_text("/goal   ", settings=_settings()) is False

    def test_goal_prompt_batches(self) -> None:
        assert should_batch_text("/goal all tests pass", settings=_settings()) is True

    def test_engine_directive_with_text_batches(self) -> None:
        assert should_batch_text("/claude fix the bug", settings=_settings()) is True
        assert (
            should_batch_text("/codex review the changes", settings=_settings()) is True
        )

    def test_engine_directive_without_text_does_not_batch(self) -> None:
        assert should_batch_text("/claude", settings=_settings()) is False
        assert should_batch_text("/codex", settings=_settings()) is False

    def test_plugin_command_with_text_batches(self) -> None:
        assert (
            should_batch_text("/custom some prompt text", settings=_settings()) is True
        )

    def test_plugin_command_without_text_does_not_batch(self) -> None:
        assert should_batch_text("/custom", settings=_settings()) is False


class TestJoinPromptParts:
    def test_single_part(self) -> None:
        parts = [PromptBatchPart(message_id=1, text="hello")]
        assert join_prompt_parts(parts, separator="blank_line") == "hello"

    def test_multiple_parts_blank_line_separator(self) -> None:
        parts = [
            PromptBatchPart(message_id=2, text="second"),
            PromptBatchPart(message_id=1, text="first"),
            PromptBatchPart(message_id=3, text="third"),
        ]
        assert (
            join_prompt_parts(parts, separator="blank_line")
            == "first\n\nsecond\n\nthird"
        )

    def test_multiple_parts_newline_separator(self) -> None:
        parts = [
            PromptBatchPart(message_id=1, text="a"),
            PromptBatchPart(message_id=2, text="b"),
        ]
        assert join_prompt_parts(parts, separator="newline") == "a\nb"

    def test_empty_parts(self) -> None:
        assert join_prompt_parts([], separator="blank_line") == ""


class TestStickyHelpers:
    def test_sticky_plan_args_empty(self) -> None:
        assert is_sticky_plan_args("") is True

    def test_sticky_plan_args_action(self) -> None:
        for action in ("on", "off", "clear", "show"):
            assert is_sticky_plan_args(action) is True
            assert is_sticky_plan_args(action.upper()) is True

    def test_sticky_plan_args_prompt(self) -> None:
        assert is_sticky_plan_args("design the API") is False
        assert is_sticky_plan_args("/agy implement") is False

    def test_sticky_goal_args_empty(self) -> None:
        assert is_sticky_goal_args("") is True
        assert is_sticky_goal_args("   ") is True

    def test_sticky_goal_args_condition(self) -> None:
        assert is_sticky_goal_args("all tests pass") is False
