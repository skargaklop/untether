"""Universal resume-session parsing helpers.

`resume` is a cross-engine alias (alongside --resume / --session / -r / -s and
engine-native forms such as agy --conversation).
"""

from __future__ import annotations

import re
from collections.abc import Callable

# Leading bare form after directives have been stripped (start of prompt).
_BARE_RESUME_RE = re.compile(
    r"(?is)^\s*(?:resume|--resume|--session|-r|-s)\s+(?P<token>\S+)\s*(?P<rest>.*)$"
)

# `{engine} resume <token>` — universal alias for every agent including agy.
_ENGINE_RESUME_ALIAS_RE = re.compile(
    r"(?im)^\s*`?(?P<engine>[a-z][a-z0-9_]*)\s+(?:resume|--resume|-r)\s+"
    r"(?P<token>[^`\s]+)`?(?:\s|$)"
)


def parse_bare_resume(text: str | None) -> tuple[str, str] | None:
    """Parse leading bare resume form. Returns (session_id, rest_of_prompt) or None."""
    if not text:
        return None
    match = _BARE_RESUME_RE.match(text)
    if match is None:
        return None
    token = match.group("token")
    rest = (match.group("rest") or "").strip()
    if not token:
        return None
    return token, rest


def parse_engine_resume_alias(text: str | None) -> tuple[str, str] | None:
    """Parse `{engine} resume <token>` anywhere as a line prefix.

    Returns (engine_id, session_id) for the last match, or None.
    """
    if not text:
        return None
    found: tuple[str, str] | None = None
    for match in _ENGINE_RESUME_ALIAS_RE.finditer(text):
        engine = (match.group("engine") or "").lower()
        token = match.group("token")
        if engine and token:
            found = (engine, token)
    return found


def strip_engine_resume_prefix(text: str, *, engine: str | None = None) -> str:
    """Remove a leading bare resume or `{engine} resume` line from the prompt."""
    if not text:
        return text
    bare = parse_bare_resume(text)
    if bare is not None:
        return bare[1]
    lines = text.splitlines()
    if not lines:
        return text
    first = lines[0].strip()
    # Match optional engine prefix + resume keyword
    if engine:
        pat = re.compile(
            rf"(?i)^\s*`?(?:{re.escape(engine)}\s+)?"
            rf"(?:resume|--resume|--session|-r|-s|--conversation)\s+\S+`?\s*(.*)$"
        )
    else:
        pat = re.compile(
            r"(?i)^\s*`?(?:[a-z][a-z0-9_]*\s+)?"
            r"(?:resume|--resume|--session|-r|-s|--conversation)\s+\S+`?\s*(.*)$"
        )
    m = pat.match(first)
    if m is None:
        return text
    rest_first = (m.group(1) or "").strip()
    tail = lines[1:]
    parts: list[str] = []
    if rest_first:
        parts.append(rest_first)
    parts.extend(tail)
    return "\n".join(parts).strip()


def strip_resume_lines(
    text: str,
    *,
    is_resume_line: Callable[[str], bool],
) -> str:
    lines = [line for line in text.splitlines() if not is_resume_line(line)]
    return "\n".join(lines).strip()
