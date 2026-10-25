"""Scoped crash-tolerant observed-message journal; never scans home directories.

A journal records only explicitly supplied user/assistant messages. It is NOT
evidence of full native session persistence and cannot migrate hidden state.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from uuid import UUID


class JournalError(ValueError):
    pass


class ObservedJournal:
    def __init__(self, root: Path, *, max_bytes: int = 16777216) -> None:
        self.root = root
        self.max_bytes = max_bytes

    def _path(self, session_id: str) -> Path:
        try:
            if str(UUID(session_id)) != session_id:
                raise ValueError
        except ValueError as exc:
            raise JournalError("SESSION_ID_INVALID") from exc
        return self.root / (session_id + ".jsonl")

    def append(self, session_id: str, *, role: str, text: str) -> None:
        if role not in ("user", "assistant") or not isinstance(text, str):
            raise JournalError("OBSERVATION_INVALID")
        if len(text.encode("utf-8")) > 262144:
            raise JournalError("MESSAGE_TOO_LARGE")
        if self.root.is_symlink():
            raise JournalError("JOURNAL_ROOT_SYMLINK")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = self._path(session_id)
        if path.is_symlink():
            raise JournalError("JOURNAL_SYMLINK")
        payload = json.dumps({"role": role, "text": text}, ensure_ascii=False).encode("utf-8")
        line = payload + b"\n"
        current = path.stat().st_size if path.exists() else 0
        if current + len(line) > self.max_bytes:
            raise JournalError("JOURNAL_FULL")
        flags = os.O_APPEND | os.O_WRONLY | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            with os.fdopen(fd, "ab", closefd=False) as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
        finally:
            os.close(fd)

    def snapshot(self, session_id: str) -> tuple[bytes, str]:
        path = self._path(session_id)
        if path.is_symlink() or not path.is_file():
            raise JournalError("JOURNAL_UNAVAILABLE")
        if path.stat().st_size > self.max_bytes:
            raise JournalError("JOURNAL_FULL")
        items = []
        with path.open("rb") as f:
            for raw in f:
                if len(raw) > 1048576 or not raw.endswith(b"\n"):
                    raise JournalError("JOURNAL_INTERRUPTED")
                try:
                    item = json.loads(raw)
                except (UnicodeError, ValueError) as exc:
                    raise JournalError("JOURNAL_CORRUPT") from exc
                if not isinstance(item, dict) or set(item) != {"role", "text"}:
                    raise JournalError("JOURNAL_SCHEMA_INVALID")
                if item["role"] not in ("user", "assistant") or not isinstance(item["text"], str):
                    raise JournalError("JOURNAL_SCHEMA_INVALID")
                items.append(item)
                if len(items) > 500:
                    raise JournalError("JOURNAL_MESSAGE_LIMIT")
        if not items:
            raise JournalError("JOURNAL_EMPTY")
        payload = json.dumps({"messages": items}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return payload, hashlib.sha256(payload).hexdigest()
