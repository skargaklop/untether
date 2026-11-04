"""Read-only OMP 18.1.6 persistence collection; not an import/restore adapter.

Rules checked against installed session-loader, session-title-slot,
session-manager and blob-store sources. Call on quiescent, trusted roots:
filesystem checks reject links/reparse points, but are not a snapshot or a
portable defence against concurrent hostile directory replacement.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from .native_archive import FidelityError

_BLOB = re.compile(r"blob:sha256:([0-9a-f]{64})")


def _checked(path: Path, *, directory: bool = False) -> Path:
    if ".." in path.parts:
        raise FidelityError("OMP_PATH_INVALID")
    path = Path(os.path.abspath(path))
    for component in (*reversed(path.parents), path):
        info = component.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise FidelityError("OMP_LINK_REFUSED")
    info = path.stat()
    if not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
        raise FidelityError("OMP_FILE_TYPE_INVALID")
    return path


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise FidelityError("OMP_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _constant(value: str) -> object:
    raise FidelityError("OMP_NONFINITE_JSON")


def _journal(data: bytes) -> tuple[str, set[str]]:
    try:
        lines = data.decode("utf-8").splitlines(keepends=True)
        entries = [
            json.loads(line, object_pairs_hook=_object, parse_constant=_constant)
            for line in lines
            if line.strip()
        ]
        if (
            entries
            and isinstance(entries[0], dict)
            and entries[0].get("type") == "title"
        ):
            slot = entries.pop(0)
            if (
                len(lines[0].encode("utf-8")) != 256
                or not lines[0].endswith("\n")
                or type(slot.get("v")) is not int
                or slot["v"] != 1
                or any(
                    not isinstance(slot.get(k), str)
                    for k in ("title", "updatedAt", "pad")
                )
                or ("source" in slot and slot["source"] not in ("auto", "user"))
            ):
                raise FidelityError("OMP_TITLE_INVALID")
        if (
            not entries
            or any(
                not isinstance(e, dict) or not isinstance(e.get("type"), str)
                for e in entries
            )
            or entries[0].get("type") != "session"
            or type(entries[0].get("version")) is not int
            or entries[0]["version"] != 3
            or not isinstance(entries[0].get("id"), str)
            or not entries[0]["id"]
            or any(e.get("type") in ("session", "title") for e in entries[1:])
        ):
            raise FidelityError("OMP_JOURNAL_INVALID")
        refs: set[str] = set()
        pending = list(entries)
        while pending:
            value = pending.pop()
            if isinstance(value, str):
                match = _BLOB.fullmatch(value)
                if match:
                    refs.add(match[1])
            elif isinstance(value, dict):
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
        return entries[0]["id"], refs
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise FidelityError("OMP_JOURNAL_INVALID") from exc


@dataclass(frozen=True)
class OmpCollectedBundle:
    journal_name: str
    records: dict[str, bytes]


def collect_omp_bundle(
    *,
    sessions_dir: Path,
    blobs_dir: Path,
    native_id: str,
    max_expanded: int,
    max_file_bytes: int,
    max_files: int = 10_000,
) -> OmpCollectedBundle:
    """Collect the source journal name, exact journal, recursive artifacts and verified referenced blobs.

    Limits also bound discovery (files, directories and bytes read), not just
    the returned mapping. Unrelated malformed journals fail closed. Repeated
    references share one blob record; duplicate session IDs are ambiguous.
    Pass the archive consumer's expanded-byte limit explicitly.
    """
    if (
        not native_id
        or any(c in native_id for c in "/\\:")
        or native_id in (".", "..")
        or any(ord(c) < 32 for c in native_id)
        or any(
            type(n) is not int or n <= 0
            for n in (max_expanded, max_file_bytes, max_files)
        )
    ):
        raise FidelityError("OMP_ARGUMENT_INVALID")
    try:
        sessions = _checked(Path(sessions_dir), directory=True)
        blobs = _checked(Path(blobs_dir), directory=True)
        total = 0
        visited = 0

        def read(path: Path) -> bytes:
            nonlocal total
            path = _checked(path)
            remaining = min(max_file_bytes, max_expanded - total)
            with path.open("rb") as stream:
                data = stream.read(remaining + 1)
            if len(data) > remaining:
                raise FidelityError("OMP_SIZE_LIMIT")
            total += len(data)
            return data

        journals: dict[Path, tuple[bytes, str, set[str]]] = {}
        files: list[Path] = []
        pending = [sessions]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as children:
                for child in children:
                    visited += 1
                    if visited > max_files:
                        raise FidelityError("OMP_COUNT_LIMIT")
                    path = Path(child.path)
                    if child.is_dir(follow_symlinks=False):
                        pending.append(_checked(path, directory=True))
                    else:
                        path = _checked(path)
                        files.append(path)
                        if path.suffix == ".jsonl":
                            data = read(path)
                            identity, refs = _journal(data)
                            journals[path] = (data, identity, refs)
        ids = [item[1] for item in journals.values()]
        if len(ids) != len(set(ids)):
            raise FidelityError("OMP_DUPLICATE_SESSION_ID")
        matches = [path for path, item in journals.items() if item[1] == native_id]
        if len(matches) != 1:
            raise FidelityError("OMP_SESSION_NOT_UNIQUE")
        root = matches[0]
        artifacts = root.with_suffix("")
        records = {"session.jsonl": journals[root][0]}
        refs = set(journals[root][2])
        for path in files:
            if not path.is_relative_to(artifacts):
                continue
            relative = path.relative_to(artifacts).as_posix()
            if any(
                ":" in part or "\\" in part or any(ord(c) < 32 for c in part)
                for part in relative.split("/")
            ):
                raise FidelityError("OMP_PATH_INVALID")
            if path in journals:
                data, _, found = journals[path]
                refs.update(found)
            else:
                data = read(path)
            records["artifacts/" + relative] = data
        for digest in sorted(refs):
            if visited + len(records) >= max_files:
                raise FidelityError("OMP_COUNT_LIMIT")
            data = read(blobs / digest)
            if hashlib.sha256(data).hexdigest() != digest:
                raise FidelityError("OMP_BLOB_CORRUPT")
            records["blobs/" + digest] = data
        return OmpCollectedBundle(journal_name=root.name, records=records)
    except OSError as exc:
        raise FidelityError("OMP_FILESYSTEM_INVALID") from exc


def collect_omp_records(
    *,
    sessions_dir: Path,
    blobs_dir: Path,
    native_id: str,
    max_expanded: int,
    max_file_bytes: int,
    max_files: int = 10_000,
) -> dict[str, bytes]:
    """Collect exact records with the same discovery and byte limits."""
    return collect_omp_bundle(
        sessions_dir=sessions_dir,
        blobs_dir=blobs_dir,
        native_id=native_id,
        max_expanded=max_expanded,
        max_file_bytes=max_file_bytes,
        max_files=max_files,
    ).records
