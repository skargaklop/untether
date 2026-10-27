"""Provider-neutral session catalog overlay."""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import msgspec

from .logging import get_logger
from .telegram.state_store import JsonStateStore

logger = get_logger(__name__)

STATE_VERSION = 1
STATE_FILENAME = "session_catalog_state.json"
_PREVIEW_LIMIT = 500
_TITLE_LIMIT = 160


def page_size() -> int:
    raw = os.environ.get("SESSION_CATALOG_PAGE_SIZE", "10")
    try:
        size = int(raw)
    except ValueError:
        return 10
    return size if size > 0 else 10


def _clip(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return text[:limit]


def _norm_project(path: str | Path | None) -> str | None:
    if path is None:
        return None
    text = str(path).strip()
    if not text:
        return None
    try:
        return str(Path(text).expanduser().resolve())
    except OSError:
        return str(Path(text))


@dataclass(frozen=True, slots=True)
class SessionObservation:
    engine: str
    session_id: str
    resume_value: str
    project_path: str | None = None
    title: str | None = None
    first_user_prompt: str | None = None
    last_user_prompt: str | None = None
    created_at: float | None = None
    updated_at: float = 0.0
    model: str | None = None
    source: str = "untether"


@dataclass(frozen=True, slots=True)
class SessionRecord:
    engine: str
    session_id: str
    resume_value: str
    project_path: str | None
    title: str | None
    first_user_prompt: str | None
    last_user_prompt: str | None
    created_at: float | None
    updated_at: float
    model: str | None
    source: str


@dataclass(frozen=True, slots=True)
class SessionPage:
    items: tuple[SessionRecord, ...]
    total: int
    offset: int
    limit: int


class _Entry(msgspec.Struct, forbid_unknown_fields=False):
    engine: str
    session_id: str
    resume_value: str
    project_path: str | None = None
    title: str | None = None
    first_user_prompt: str | None = None
    last_user_prompt: str | None = None
    created_at: float | None = None
    updated_at: float = 0.0
    model: str | None = None
    source: str = "untether"


class _CatalogState(msgspec.Struct, forbid_unknown_fields=False):
    version: int
    entries: dict[str, _Entry] = msgspec.field(default_factory=dict)


def _new_state() -> _CatalogState:
    return _CatalogState(version=STATE_VERSION)


def _key(engine: str, session_id: str) -> str:
    return f"{engine}\n{session_id}"


def _prefer(current: str | None, incoming: str | None) -> str | None:
    return incoming if incoming else current


def _to_record(entry: _Entry) -> SessionRecord:
    return SessionRecord(
        engine=entry.engine,
        session_id=entry.session_id,
        resume_value=entry.resume_value,
        project_path=entry.project_path,
        title=entry.title,
        first_user_prompt=entry.first_user_prompt,
        last_user_prompt=entry.last_user_prompt,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        model=entry.model,
        source=entry.source,
    )


class SessionCatalog(JsonStateStore[_CatalogState]):
    def __init__(self, path: Path) -> None:
        super().__init__(
            path,
            version=STATE_VERSION,
            state_type=_CatalogState,
            state_factory=_new_state,
            log_prefix="session_catalog",
            logger=logger,
        )

    async def observe(self, observation: SessionObservation) -> None:
        await self.merge_provider([observation])

    async def merge_provider(self, observations: Iterable[SessionObservation]) -> None:
        async with self._lock:
            self._reload_locked_if_needed()
            changed = False
            for observation in observations:
                if self._merge_locked(observation):
                    changed = True
            if changed:
                self._save_locked()

    def _merge_locked(self, observation: SessionObservation) -> bool:
        engine = observation.engine.strip()
        session_id = observation.session_id.strip()
        resume_value = observation.resume_value.strip()
        if not engine or not session_id or not resume_value:
            return False
        key = _key(engine, session_id)
        current = self._state.entries.get(key)
        title = _clip(observation.title, _TITLE_LIMIT)
        first = _clip(observation.first_user_prompt, _PREVIEW_LIMIT)
        last = _clip(observation.last_user_prompt, _PREVIEW_LIMIT)
        project = _norm_project(observation.project_path)
        model = _clip(observation.model, 80)
        if current is None:
            self._state.entries[key] = _Entry(
                engine=engine,
                session_id=session_id,
                resume_value=resume_value,
                project_path=project,
                title=title,
                first_user_prompt=first,
                last_user_prompt=last,
                created_at=observation.created_at,
                updated_at=observation.updated_at,
                model=model,
                source=observation.source,
            )
            return True
        current.resume_value = resume_value or current.resume_value
        current.project_path = _prefer(current.project_path, project)
        current.title = _prefer(current.title, title)
        current.first_user_prompt = _prefer(current.first_user_prompt, first)
        current.last_user_prompt = _prefer(current.last_user_prompt, last)
        current.model = _prefer(current.model, model)
        if observation.created_at is not None and (
            current.created_at is None or observation.created_at < current.created_at
        ):
            current.created_at = observation.created_at
        if observation.updated_at > current.updated_at:
            current.updated_at = observation.updated_at
        if observation.source == "untether":
            current.source = "untether"
        return True

    async def get(self, engine: str, session_id: str) -> SessionRecord | None:
        async with self._lock:
            self._reload_locked_if_needed()
            entry = self._state.entries.get(_key(engine, session_id))
            return None if entry is None else _to_record(entry)

    async def recent(
        self,
        *,
        engine: str | None,
        project_path: Path | None,
        offset: int,
        limit: int,
    ) -> SessionPage:
        async with self._lock:
            self._reload_locked_if_needed()
            wanted_engine = engine.strip() if engine else None
            wanted_project = _norm_project(project_path)
            matched = [
                _to_record(entry)
                for entry in self._state.entries.values()
                if (wanted_engine is None or entry.engine == wanted_engine)
                and (
                    wanted_project is None
                    or _norm_project(entry.project_path) == wanted_project
                )
            ]
        matched.sort(key=lambda item: item.updated_at, reverse=True)
        start = max(offset, 0)
        size = limit if limit > 0 else page_size()
        return SessionPage(
            items=tuple(matched[start : start + size]),
            total=len(matched),
            offset=start,
            limit=size,
        )
