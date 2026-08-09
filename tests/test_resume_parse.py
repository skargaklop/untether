"""Tests for universal resume-session parsing helpers."""

from __future__ import annotations

from untether.resume_parse import (
    parse_bare_resume,
    parse_engine_resume_alias,
    strip_engine_resume_prefix,
    strip_resume_lines,
)


class TestParseBareResume:
    def test_none(self) -> None:
        assert parse_bare_resume(None) is None

    def test_empty(self) -> None:
        assert parse_bare_resume("") is None

    def test_resume_keyword(self) -> None:
        result = parse_bare_resume("resume sess-123")
        assert result == ("sess-123", "")

    def test_resume_with_rest(self) -> None:
        result = parse_bare_resume("resume sess-123 do something")
        assert result == ("sess-123", "do something")

    def test_double_dash_resume(self) -> None:
        result = parse_bare_resume("--resume sess-456")
        assert result is not None
        assert result[0] == "sess-456"

    def test_session_flag(self) -> None:
        result = parse_bare_resume("--session sess-789")
        assert result is not None
        assert result[0] == "sess-789"

    def test_short_flags(self) -> None:
        assert parse_bare_resume("-r sess-r") is not None
        assert parse_bare_resume("-s sess-s") is not None

    def test_no_match(self) -> None:
        assert parse_bare_resume("just a prompt") is None


class TestParseEngineResumeAlias:
    def test_none(self) -> None:
        assert parse_engine_resume_alias(None) is None

    def test_empty(self) -> None:
        assert parse_engine_resume_alias("") is None

    def test_single_alias(self) -> None:
        result = parse_engine_resume_alias("codex resume sess-123")
        assert result == ("codex", "sess-123")

    def test_backtick_wrapped(self) -> None:
        result = parse_engine_resume_alias("`codex resume sess-456`")
        assert result == ("codex", "sess-456")

    def test_double_dash_form(self) -> None:
        result = parse_engine_resume_alias("agy --resume conv-789")
        assert result == ("agy", "conv-789")

    def test_short_flag_form(self) -> None:
        result = parse_engine_resume_alias("claude -r sess-r")
        assert result == ("claude", "sess-r")

    def test_last_match_wins(self) -> None:
        text = "codex resume first\nagy resume second"
        result = parse_engine_resume_alias(text)
        assert result == ("agy", "second")

    def test_no_match(self) -> None:
        assert parse_engine_resume_alias("just a prompt") is None


class TestStripEngineResumePrefix:
    def test_bare_resume_stripped(self) -> None:
        result = strip_engine_resume_prefix("resume sess-123 do the work")
        assert result == "do the work"

    def test_engine_resume_stripped(self) -> None:
        result = strip_engine_resume_prefix("codex resume sess-123\ndo the work")
        assert "sess-123" not in result
        assert "do the work" in result

    def test_with_engine_hint(self) -> None:
        result = strip_engine_resume_prefix(
            "codex resume sess-123\ndo the work", engine="codex"
        )
        assert "sess-123" not in result
        assert "do the work" in result

    def test_no_resume_returns_unchanged(self) -> None:
        result = strip_engine_resume_prefix("just a prompt")
        assert result == "just a prompt"

    def test_empty(self) -> None:
        assert strip_engine_resume_prefix("") == ""


class TestStripResumeLines:
    def test_strips_matching_lines(self) -> None:
        text = "line1\nresume sess-123\nline3"
        result = strip_resume_lines(
            text, is_resume_line=lambda line: line.startswith("resume ")
        )
        assert "resume" not in result
        assert "line1" in result
        assert "line3" in result

    def test_no_matching_lines(self) -> None:
        text = "line1\nline2"
        result = strip_resume_lines(text, is_resume_line=lambda _: False)
        assert result == "line1\nline2"

    def test_all_stripped(self) -> None:
        text = "resume a\nresume b"
        result = strip_resume_lines(
            text, is_resume_line=lambda line: line.startswith("resume ")
        )
        assert result == ""
