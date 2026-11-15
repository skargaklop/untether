"""Native session-file scanners. Concepts adapted from Codbash (MIT)."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from .session_catalog import SessionObservation

_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_WALK_DEPTH = 3


def _parse_ts(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _text_from_content(content: object) -> str:
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    if any(
        isinstance(part, dict) and part.get("type") == "tool_result" for part in content
    ) and not any(
        isinstance(part, dict)
        and part.get("type") not in {"tool_result", "tool_use"}
        and isinstance(part.get("text"), str)
        and part.get("text").strip()
        for part in content
    ):
        return ""
    lines: list[str] = []
    for part in content:
        if isinstance(part, str) and part.strip():
            lines.append(part.strip())
        elif isinstance(part, dict):
            if part.get("type") in {"tool_result", "tool_use"}:
                continue
            text = part.get("text") or part.get("content")
            if isinstance(text, str) and text.strip():
                lines.append(text.strip())
    return "\n".join(lines).strip()


def _iter_jsonl(path: Path) -> Iterator[dict]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            yield payload


def _walk_jsonl(root: Path) -> Iterator[Path]:
    if not root.exists() or root.is_symlink():
        return

    def walk(directory: Path, depth: int) -> Iterator[Path]:
        try:
            entries = list(directory.iterdir())
        except OSError:
            return
        for entry in entries:
            try:
                if entry.is_symlink():
                    continue
                if entry.is_file() and entry.suffix == ".jsonl":
                    yield entry
                elif entry.is_dir() and depth < _WALK_DEPTH:
                    yield from walk(entry, depth + 1)
            except OSError:
                continue

    yield from walk(root, 0)


_CACHES: dict[tuple[str, str], _FingerprintCache] = {}


class _FingerprintCache:
    def __init__(self) -> None:
        self._fingerprint: tuple[tuple[str, int, int], ...] | None = None
        self._cached: tuple[SessionObservation, ...] = ()

    def scan(self, root: Path, parser) -> tuple[SessionObservation, ...]:
        fingerprint = _fingerprint_of(root)
        if fingerprint == self._fingerprint:
            return self._cached
        found = tuple(parser())
        self._fingerprint = fingerprint
        self._cached = found
        return found


def _fingerprint_of(root: Path) -> tuple[tuple[str, int, int], ...] | None:
    if not root.exists():
        return None
    rows: list[tuple[str, int, int]] = []
    try:
        root_stat = root.stat()
    except OSError:
        return None
    rows.append((str(root), root_stat.st_mtime_ns, 0))
    for path in _walk_jsonl(root):
        try:
            stat = path.stat()
        except OSError:
            continue
        rows.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(sorted(rows))


class PiFamilyScanner:
    def __init__(self, *, engine: str, agent_dir: Path) -> None:
        self.engine = engine
        self.agent_dir = agent_dir
        self._cache = _CACHES.setdefault((engine, str(agent_dir)), _FingerprintCache())

    def scan(self) -> tuple[SessionObservation, ...]:
        return self._cache.scan(self.agent_dir / "sessions", self._parse)

    def _parse(self) -> Iterator[SessionObservation]:
        for path in _walk_jsonl(self.agent_dir / "sessions"):
            record = self._parse_file(path)
            if record is not None:
                yield record

    def _parse_file(self, path: Path) -> SessionObservation | None:
        rows = list(_iter_jsonl(path))
        if not rows:
            return None
        header = rows[0]
        if header.get("type") != "session":
            return None
        session_id = str(header.get("id") or "")
        if not _SAFE_ID.fullmatch(session_id):
            return None
        title = header.get("title") if isinstance(header.get("title"), str) else None
        project = header.get("cwd") if isinstance(header.get("cwd"), str) else None
        model = header.get("model") if isinstance(header.get("model"), str) else None
        first_ts = _parse_ts(header.get("timestamp"))
        last_ts = first_ts
        first_user: str | None = None
        last_user: str | None = None
        for row in rows[1:]:
            ts = _parse_ts(row.get("timestamp") or row.get("ts"))
            if ts is not None:
                first_ts = ts if first_ts is None else min(first_ts, ts)
                last_ts = ts if last_ts is None else max(last_ts, ts)
            message = row.get("message")
            if not isinstance(message, dict):
                message = {}
            if row.get("type") not in {None, "message"} and "message" not in row:
                continue
            role = message.get("role") or row.get("role")
            if role != "user":
                continue
            content = message.get("content", row.get("content"))
            text = _text_from_content(content)
            if not text:
                continue
            if first_user is None:
                first_user = text
            last_user = text
        return SessionObservation(
            engine=self.engine,
            session_id=session_id,
            resume_value=session_id,
            project_path=project,
            title=title,
            first_user_prompt=first_user,
            last_user_prompt=last_user,
            created_at=first_ts,
            updated_at=last_ts or 0.0,
            model=model,
            source="provider",
        )


class ClaudeScanner:
    def __init__(self, *, root: Path) -> None:
        self.root = root
        self.engine = "claude"
        self._cache = _CACHES.setdefault(("claude", str(root)), _FingerprintCache())

    def scan(self) -> tuple[SessionObservation, ...]:
        return self._cache.scan(self.root / "projects", self._parse)

    def _parse(self) -> Iterator[SessionObservation]:
        for path in _walk_jsonl(self.root / "projects"):
            record = self._parse_file(path)
            if record is not None:
                yield record

    def _parse_file(self, path: Path) -> SessionObservation | None:
        session_id = ""
        project: str | None = None
        title: str | None = None
        first_ts: float | None = None
        last_ts: float | None = None
        first_user: str | None = None
        last_user: str | None = None
        for row in _iter_jsonl(path):
            if not session_id and isinstance(row.get("sessionId"), str):
                session_id = row["sessionId"]
            if project is None and isinstance(row.get("cwd"), str):
                project = row["cwd"]
            if row.get("type") == "custom-title" and isinstance(
                row.get("customTitle"), str
            ):
                title = row["customTitle"]
            ts = _parse_ts(row.get("timestamp"))
            if ts is not None:
                first_ts = ts if first_ts is None else min(first_ts, ts)
                last_ts = ts if last_ts is None else max(last_ts, ts)
            message = row.get("message")
            if not isinstance(message, dict):
                message = {}
            if message.get("role") != "user" and row.get("type") != "user":
                continue
            text = _text_from_content(message.get("content", row.get("content")))
            if not text:
                continue
            if first_user is None:
                first_user = text
            last_user = text
            if not session_id and _SAFE_ID.fullmatch(path.stem):
                session_id = path.stem
        if not session_id or not _SAFE_ID.fullmatch(session_id):
            return None
        return SessionObservation(
            engine="claude",
            session_id=session_id,
            resume_value=session_id,
            project_path=project,
            title=title,
            first_user_prompt=first_user,
            last_user_prompt=last_user,
            created_at=first_ts,
            updated_at=last_ts or 0.0,
            source="provider",
        )


class CodexScanner:
    def __init__(self, *, root: Path) -> None:
        self.root = root
        self.engine = "codex"
        self._cache = _CACHES.setdefault(("codex", str(root)), _FingerprintCache())

    def scan(self) -> tuple[SessionObservation, ...]:
        return self._cache.scan(self.root / "sessions", self._parse)

    def _parse(self) -> Iterator[SessionObservation]:
        for path in _walk_jsonl(self.root / "sessions"):
            record = self._parse_file(path)
            if record is not None:
                yield record

    def _parse_file(self, path: Path) -> SessionObservation | None:
        session_id = ""
        project: str | None = None
        first_ts: float | None = None
        last_ts: float | None = None
        last_user: str | None = None
        first_user: str | None = None
        for row in _iter_jsonl(path):
            payload = row.get("payload")
            if not isinstance(payload, dict):
                payload = row
            ts = _parse_ts(row.get("timestamp") or payload.get("timestamp"))
            if ts is not None:
                first_ts = ts if first_ts is None else min(first_ts, ts)
                last_ts = ts if last_ts is None else max(last_ts, ts)
            if not session_id and isinstance(payload.get("id"), str):
                session_id = payload["id"]
            if project is None and isinstance(payload.get("cwd"), str):
                project = payload["cwd"]
            text = payload.get("text") or payload.get("prompt")
            if not isinstance(text, str):
                message = payload.get("message")
                if isinstance(message, str):
                    text = message
                elif isinstance(message, dict):
                    text = _text_from_content(message.get("content"))
                else:
                    text = _text_from_content(payload.get("content"))
            if (
                isinstance(text, str)
                and text.strip()
                and payload.get("type") != "session_meta"
            ):
                cleaned = text.strip()
                if first_user is None:
                    first_user = cleaned
                last_user = cleaned
        if not session_id:
            match = re.search(r"([A-Za-z0-9._-]{1,128})$", path.stem)
            session_id = match.group(1) if match else ""
        if not _SAFE_ID.fullmatch(session_id):
            return None
        return SessionObservation(
            engine="codex",
            session_id=session_id,
            resume_value=session_id,
            project_path=project,
            title=None,
            first_user_prompt=first_user,
            last_user_prompt=last_user,
            created_at=first_ts,
            updated_at=last_ts or 0.0,
            source="provider",
        )
