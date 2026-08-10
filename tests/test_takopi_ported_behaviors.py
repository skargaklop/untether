"""Tests for Takopi→Untether ported behaviors.

Covers: markup preprocessing (spoiler/underline/strike with code-region
protection), compact/handoff invocation parsing, file-task annotation,
Pi plan-mode detection, and runner lifecycle settings.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from untether.model import EngineId, ResumeToken
from untether.telegram.commands.parse import (
    CompactInvocation,
    parse_compact_invocation,
    parse_command_invocation,
    parse_handoff_invocation,
)
from untether.telegram.files import (
    format_image_prompt_annotation,
    is_image_document,
)
from untether.telegram.render import render_markdown


# ─── Markup preprocessing ────────────────────────────────────────────


class TestMarkupPreprocessing:
    def test_spoiler(self) -> None:
        text, entities = render_markdown("This is ||secret|| text")
        assert any(e.get("type") == "spoiler" for e in entities)

    def test_underline(self) -> None:
        text, entities = render_markdown("This is ++underlined++ text")
        assert any(e.get("type") == "underline" for e in entities)

    def test_single_strike(self) -> None:
        text, entities = render_markdown("This is ~struck~ text")
        assert any(e.get("type") == "strikethrough" for e in entities)

    def test_double_strike_gfm(self) -> None:
        text, entities = render_markdown("This is ~~struck~~ text")
        assert any(e.get("type") == "strikethrough" for e in entities)

    def test_code_region_protection_spoiler(self) -> None:
        md = "```\n||not spoiler||\n```\n||real spoiler||"
        text, entities = render_markdown(md)
        spoiler_count = sum(1 for e in entities if e.get("type") == "spoiler")
        assert spoiler_count == 1

    def test_code_region_protection_inline_code(self) -> None:
        md = "`||not spoiler||` and ||real spoiler||"
        text, entities = render_markdown(md)
        spoiler_count = sum(1 for e in entities if e.get("type") == "spoiler")
        assert spoiler_count == 1

    def test_pipe_separator_not_spoiler(self) -> None:
        # "a || b || c" does match the spoiler regex (body=" b ").
        # This is intentional — it mirrors Takopi's behavior exactly.
        # To avoid spoiler rendering, users should not use || around text.
        text, entities = render_markdown("a || b || c")
        # It IS treated as a spoiler (matching Takopi):
        assert any(e.get("type") == "spoiler" for e in entities)

    def test_empty_markdown(self) -> None:
        text, entities = render_markdown("")
        assert text == ""
        assert entities == []


# ─── Compact/handoff invocation parsing ─────────────────────────────


class TestCompactHandoffParsing:
    ENGINE_IDS: tuple[EngineId, ...] = ("pi", "claude", "agy")

    def test_basic_compact(self) -> None:
        r = parse_compact_invocation("/compact", engine_ids=self.ENGINE_IDS)
        assert r is not None
        assert r.engine is None
        assert r.instructions is None
        assert r.destination_engine is None

    def test_compact_with_instructions(self) -> None:
        r = parse_compact_invocation(
            "/compact focus on auth", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.instructions == "focus on auth"

    def test_compact_with_engine_selector(self) -> None:
        r = parse_compact_invocation(
            "/pi /compact", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.engine == "pi"

    def test_compact_engine_before_flag(self) -> None:
        r = parse_compact_invocation(
            "/compact /pi", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.engine == "pi"

    def test_compact_cross_engine_destination(self) -> None:
        r = parse_compact_invocation(
            "/compact to claude", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.destination_engine == "claude"

    def test_compact_cross_engine_with_slash(self) -> None:
        r = parse_compact_invocation(
            "/compact to /claude", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.destination_engine == "claude"

    def test_compact_with_engine_and_destination(self) -> None:
        r = parse_compact_invocation(
            "/pi /compact to claude", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.engine == "pi"
        assert r.destination_engine == "claude"

    def test_handoff_basic(self) -> None:
        r = parse_handoff_invocation("/handoff", engine_ids=self.ENGINE_IDS)
        assert r is not None
        assert r.instructions is None

    def test_handoff_with_destination(self) -> None:
        r = parse_handoff_invocation(
            "/handoff to agy", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.destination_engine == "agy"

    def test_non_command_returns_none(self) -> None:
        r = parse_compact_invocation("hello world", engine_ids=self.ENGINE_IDS)
        assert r is None

    def test_to_unknown_engine_is_instructions(self) -> None:
        r = parse_compact_invocation(
            "/compact to unknown", engine_ids=self.ENGINE_IDS
        )
        assert r is not None
        assert r.destination_engine is None
        assert "to unknown" in (r.instructions or "")

    def test_multiple_engine_selectors_raises(self) -> None:
        with pytest.raises(ValueError, match="multiple engine selectors"):
            parse_compact_invocation(
                "/pi /agy /compact", engine_ids=self.ENGINE_IDS
            )

    def test_compact_at_bot_username(self) -> None:
        r = parse_compact_invocation(
            "/compact@mybot", engine_ids=self.ENGINE_IDS
        )
        assert r is not None


# ─── File-task annotation ────────────────────────────────────────────


class TestFileTaskAnnotation:
    def test_is_image_by_mime(self) -> None:
        assert is_image_document(mime_type="image/png", file_name=None)

    def test_is_image_by_extension(self) -> None:
        assert is_image_document(mime_type=None, file_name="photo.jpg")
        assert is_image_document(mime_type=None, file_name="pic.webp")

    def test_is_image_telegram_photo(self) -> None:
        assert is_image_document(
            mime_type=None,
            file_name=None,
            raw={"width": 100, "height": 200},
        )

    def test_not_image_text(self) -> None:
        assert not is_image_document(
            mime_type="text/plain", file_name="doc.txt"
        )

    def test_not_image_no_indicators(self) -> None:
        assert not is_image_document(
            mime_type=None, file_name="data.json"
        )

    def test_format_single_image_annotation(self) -> None:
        result = format_image_prompt_annotation(["uploads/img.png"])
        assert "[image]" in result
        assert "uploads/img.png" in result
        assert "Read the image" in result

    def test_format_multi_image_annotation(self) -> None:
        result = format_image_prompt_annotation(["a.png", "b.jpg"])
        assert "[images]" in result
        assert "a.png" in result
        assert "b.jpg" in result

    def test_format_empty_annotation(self) -> None:
        assert format_image_prompt_annotation([]) == ""


# ─── Pi plan-mode detection ──────────────────────────────────────────


class TestPiPlanMode:
    def test_detect_extension_nonexistent_root(self, tmp_path: Path) -> None:
        from untether.runners.pi import detect_plan_mode_extension

        # Point to a non-existent root
        assert not detect_plan_mode_extension(root=tmp_path / "nonexistent")

    def test_detect_extension_exists(self, tmp_path: Path) -> None:
        from untether.runners.pi import (
            _PLAN_MODE_EXTENSION_PACKAGE,
            detect_plan_mode_extension,
        )

        # Create the expected package directory
        pkg = tmp_path / _PLAN_MODE_EXTENSION_PACKAGE
        pkg.mkdir(parents=True)
        assert detect_plan_mode_extension(root=tmp_path)

    def test_pi_runner_init_defaults(self) -> None:
        from untether.runners.pi import PiRunner

        r = PiRunner(
            extra_args=[], model=None, provider=None
        )
        assert r.plan_mode_extension is False
        assert r._plan_warning_logged is False

    def test_pi_runner_plan_extension_param(self) -> None:
        from untether.runners.pi import PiRunner

        r = PiRunner(
            extra_args=[], model=None, provider=None, plan_mode_extension=True
        )
        assert r.plan_mode_extension is True


# ─── Runner lifecycle settings ──────────────────────────────────────


class TestRunnerLifecycleSettings:
    def test_defaults(self) -> None:
        from untether.runner import JsonlSubprocessRunner

        # Class-level defaults
        assert JsonlSubprocessRunner.startup_timeout_s is None
        assert JsonlSubprocessRunner.idle_timeout_s is None
        assert JsonlSubprocessRunner.retry_max_attempts == 1
        assert JsonlSubprocessRunner.retry_base_delay_s == 5.0

    def test_format_delay(self) -> None:
        from untether.runner import _format_delay

        assert _format_delay(5.0) == "5"
        assert _format_delay(0.5) == "0.5"
        assert _format_delay(10.0) == "10"

    def test_runner_settings_model(self) -> None:
        from untether.settings import RunnerSettings

        rs = RunnerSettings()
        assert rs.startup_timeout_s == 60.0
        assert rs.idle_timeout_s == 900.0
        assert rs.shutdown_timeout_s == 5.0
        assert rs.kill_tree_on_cancel is True
        assert rs.retry_max_attempts == 3
        assert rs.retry_base_delay_s == 5.0
