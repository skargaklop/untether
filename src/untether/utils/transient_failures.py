"""Shared transient upstream-failure classification and clean rendering.

Providers occasionally surface temporary capacity/rate-limit failures as opaque
JSON blobs (notably Grok's ``Internal error: {json}`` stream/stderr form). This
module detects those failures from arbitrary text and renders one readable
terminal message, so raw provider JSON never reaches the user.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

# Phrases that mark a failure as transient, independent of an HTTP status.
_TRANSIENT_PHRASES: tuple[str, ...] = (
    "admission capacity",
    "temporarily unavailable",
    "overloaded",
    "rate limit",
    "rate-limit",
    "retry shortly",
    "try again later",
)

# Substrings that must NOT be treated as transient even if they contain a phrase.
_NON_TRANSIENT_MARKERS: tuple[str, ...] = (
    "auth",
    "unauthorized",
    "forbidden",
    "invalid",
    "bad request",
    "cancel",
    "timeout",
    "timed out",
)

_HTTP_STATUS_RE = re.compile(r"(?i)\b(?:http|status)\s*(\d{3})\b")
# OMP/OmniRoute stream errors start with a bare ``503``/``429`` prefix:
# ``503 Chat admission capacity is temporarily unavailable.`` with no
# ``http``/``status`` keyword. Match only the two transient codes to avoid
# false positives from unrelated numbers elsewhere in the message.
_BARE_STATUS_PREFIX_RE = re.compile(r"^\s*(429|503)\b")
_RETRY_AFTER_MS_RE = re.compile(r"(?i)\s*retry-after-ms\s*=\s*\d+\s*")
# OMP terminal errors duplicate the capacity reason with different suffixes:
# ``...Retry shortly. retry-after-ms=2000\n...Retry shortly. (type=... param=...)``
# Strip the ``(type=... param=...)`` provider suffix and any text after it,
# then collapse any remaining exact-phrase duplication.
_PROVIDER_SUFFIX_RE = re.compile(r"(?i)\s*\(type=[^)]*param=[^)]*\)\s*.*$")
_RETRY_DIRECTIVE_RE = re.compile(
    r"(?i)\s*(?:retry\s+shortly|try\s+again\s+later)\.?\s*"
)
_INTERNAL_ERROR_PREFIX_RE = re.compile(r"(?is)^\s*internal\s+error\s*:\s*")
_API_ERROR_RE = re.compile(r"(?is)\bAPI\s+error\s*\(\s*status\s+(\d{3})[^)]*\)\s*:\s*")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class TransientFailure:
    """A classified transient upstream failure.

    ``http_status`` is the provider-reported HTTP status when known (429/503),
    otherwise ``None`` for a phrase-only transient signal.
    """

    http_status: int | None
    message: str


def _extract_http_status(text: str) -> int | None:
    match = _HTTP_STATUS_RE.search(text)
    if match:
        status = int(match.group(1))
        if status in (429, 503):
            return status
    bare = _BARE_STATUS_PREFIX_RE.match(text)
    if bare:
        return int(bare.group(1))
    return None


def _looks_non_transient(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in _NON_TRANSIENT_MARKERS)


def _has_transient_phrase(text: str) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in _TRANSIENT_PHRASES)


def _clean_reason(message: str, http_status: int | None) -> str:
    """Strip provider wrappers and duplicate retry advice from a reason string."""
    # Remove a leading "API error (status N ...):" provider wrapper.
    message = _API_ERROR_RE.sub("", message, count=1)
    # Strip a leading bare ``503``/``429`` status prefix (OMP errorMessage
    # format: ``503 Chat admission capacity...``); the status is already
    # captured in ``http_status`` for the formatted wrapper.
    message = _BARE_STATUS_PREFIX_RE.sub("", message, count=1)
    message = _RETRY_AFTER_MS_RE.sub(" ", message)
    # Remove ALL retry directives (OMP duplicates them mid-message).
    message = _RETRY_DIRECTIVE_RE.sub("", message)
    message = _WHITESPACE_RE.sub(" ", message).strip()
    # Strip OMP provider suffix ``(type=... param=...)`` and everything after.
    message = _PROVIDER_SUFFIX_RE.sub("", message).strip()
    # Collapse duplicate sentences/phrases (OMP repeats the capacity message
    # on separate lines with different suffixes).
    parts = [p.strip() for p in message.split(".") if p.strip()]
    seen: set[str] = set()
    unique: list[str] = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            unique.append(p)
    message = ". ".join(unique)
    # Capitalize the first letter without touching the rest (acronyms, URLs).
    if message and not message[0].isupper():
        message = message[0].upper() + message[1:]
    _ = http_status  # status only affects the formatted wrapper, not the reason
    return message


def classify_transient_failure(text: str) -> TransientFailure | None:
    """Classify ``text`` as a transient upstream failure, or ``None``.

    Accepts the observed Grok form ``Internal error: {json}``, a decoded stream
    error message, or raw subprocess stderr. Malformed JSON falls back to
    classifying the original text rather than raising.
    """
    if not text:
        return None

    collapsed = _WHITESPACE_RE.sub(" ", text).strip()
    if not collapsed:
        return None

    message_text = collapsed
    json_status: int | None = None

    stripped = _INTERNAL_ERROR_PREFIX_RE.sub("", collapsed, count=1).strip()
    # Attempt to parse a trailing JSON object after "Internal error:".
    candidate = stripped
    # Find the first '{' to tolerate leading prose before the blob.
    brace = candidate.find("{")
    if brace != -1:
        candidate = candidate[brace:]
    if candidate.startswith("{"):
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, dict):
            msg = obj.get("message")
            if isinstance(msg, str) and msg.strip():
                message_text = _WHITESPACE_RE.sub(" ", msg).strip()
            status = obj.get("http_status")
            if isinstance(status, int) and status in (429, 503):
                json_status = status

    # Resolve the effective HTTP status: JSON wins, then text scan.
    text_status = _extract_http_status(message_text) or _extract_http_status(collapsed)
    http_status = json_status or text_status

    is_status_transient = http_status in (429, 503)
    is_phrase_transient = _has_transient_phrase(message_text) or _has_transient_phrase(
        collapsed
    )
    if not (is_status_transient or is_phrase_transient):
        return None

    # Guard against false positives: a status code or phrase inside a clearly
    # non-transient failure (auth, invalid request, cancellation, timeout).
    if _looks_non_transient(message_text) and not is_status_transient:
        return None

    reason = _clean_reason(message_text, http_status)
    return TransientFailure(http_status=http_status, message=reason)


def format_transient_failure(engine: str, failure: TransientFailure) -> str:
    """Render one clean user-facing error message for a transient failure."""
    status = (
        f" (HTTP {failure.http_status})" if failure.http_status in (429, 503) else ""
    )
    reason = failure.message
    if not reason.endswith("."):
        reason += "."
    return f"{engine} upstream is temporarily unavailable{status}: {reason} Try again in a few minutes."
