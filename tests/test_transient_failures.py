"""Tests for transient failure classification and formatting."""

from __future__ import annotations

from untether.utils.transient_failures import (
    TransientFailure,
    classify_transient_failure,
    format_transient_failure,
)


class TestClassifyTransientFailure:
    def test_empty(self) -> None:
        assert classify_transient_failure("") is None
        assert classify_transient_failure(None) is None  # type: ignore[arg-type]

    def test_whitespace_only(self) -> None:
        assert classify_transient_failure("   ") is None

    def test_non_transient(self) -> None:
        assert classify_transient_failure("auth error: unauthorized") is None
        assert classify_transient_failure("invalid request: bad input") is None

    def test_http_429(self) -> None:
        result = classify_transient_failure("HTTP 429 Too Many Requests")
        assert result is not None
        assert result.http_status == 429

    def test_http_503(self) -> None:
        result = classify_transient_failure("status 503 service unavailable")
        assert result is not None
        assert result.http_status == 503

    def test_bare_503_prefix(self) -> None:
        result = classify_transient_failure(
            "503 Chat admission capacity is temporarily unavailable."
        )
        assert result is not None
        assert result.http_status == 503

    def test_bare_429_prefix(self) -> None:
        result = classify_transient_failure("429 rate limit exceeded")
        assert result is not None
        assert result.http_status == 429

    def test_transient_phrase(self) -> None:
        result = classify_transient_failure("server is overloaded")
        assert result is not None
        assert result.http_status is None

    def test_grok_internal_error_json(self) -> None:
        result = classify_transient_failure(
            'Internal error: {"message": "admission capacity temporarily unavailable", "http_status": 503}'
        )
        assert result is not None
        assert result.http_status == 503
        assert "admission capacity" in result.message.lower()

    def test_malformed_json_falls_back(self) -> None:
        result = classify_transient_failure(
            "Internal error: {broken json} rate limit exceeded"
        )
        assert result is not None

    def test_non_transient_429_with_auth(self) -> None:
        """HTTP 429 is transient even if 'auth' appears in the text."""
        result = classify_transient_failure("HTTP 429 rate limit: auth retry")
        assert result is not None
        assert result.http_status == 429

    def test_cleans_omp_suffix(self) -> None:
        result = classify_transient_failure(
            "503 Chat admission capacity is temporarily unavailable. "
            "Retry shortly. retry-after-ms=2000 "
            "(type=service_error param=capacity)"
        )
        assert result is not None
        assert "(type=" not in result.message
        assert "retry-after-ms" not in result.message


class TestFormatTransientFailure:
    def test_with_http_status(self) -> None:
        failure = TransientFailure(http_status=429, message="rate limit exceeded")
        result = format_transient_failure("grok", failure)
        assert "grok" in result
        assert "HTTP 429" in result
        assert "rate limit exceeded" in result
        assert "Try again" in result

    def test_without_http_status(self) -> None:
        failure = TransientFailure(http_status=None, message="server overloaded")
        result = format_transient_failure("agy", failure)
        assert "agy" in result
        assert "HTTP" not in result
        assert "server overloaded" in result

    def test_adds_trailing_period(self) -> None:
        failure = TransientFailure(http_status=503, message="temporarily unavailable")
        result = format_transient_failure("omp", failure)
        assert "temporarily unavailable." in result
