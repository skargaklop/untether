"""Concrete file-bundle persistence for engines whose sessions are plain files.

Every engine session record is a set of opaque files inside one operator-declared
directory; the engine identifies a session by exact native id. Nothing here knows
any engine transcript schema: the operator declares, per engine, the layout
(``sessions_dir`` + ``session_record``) plus the exact restart-probe argv, all
validated fail-closed at config load. The adapter refuses any session whose file
set falls outside the declared layout, and refuses collision or foreign format.

Supported declared formats today:
- ``pi-jsonl-v3`` — verified against pi's real storage
  (``SessionManager``, ``CURRENT_SESSION_VERSION = 3``): one JSONL file per
  session named ``<native-id>.jsonl`` directly inside the declared
  ``sessions_dir``. The first line is the session header
  ``{"type":"session","version":3,"id":"<native-id>",...}``; every following
  line is one message entry. Resume is the engine's exact-id session selection
  (``--session-dir`` + ``--session <native-id>`` followed by ``--print --mode
  json --no-session``; pi's ``SessionManager.findById`` scans the declared dir
  by header id).

When the discovered root journal basename is not the degenerate
``<native-id>.jsonl`` (pi's native storage names journals with a timestamp
prefix), the export carries it in a small validated, manifest-hashed,
versioned ``layout.json`` archive record, and import/reclaim restore the
record under that exact original basename — native ``parentSession`` entries
reference journal paths, so renaming breaks parent linkage. Archives without
the layout record (legacy) keep the historical canonical-name behaviour.

Any other declared ``session_record`` or ``format`` value is refused
(``PERSISTENCE_FORMAT_UNSUPPORTED``): this adapter never claims to faithfully
restore engine states it does not structurally verify.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .native_adapter import (
    CapabilityUnavailable,
    RestoreProof,
    register_persistence_adapter,
    registered_adapter_engines,
)
from .native_archive import NativeArchive, _safe_name
from .omp_adapter import OmpPersistenceAdapter
from .settings import EnginePersistenceProfile, SessionMigrationSettings
from .subprocess_adapter import ExecutablePersistenceAdapter

_MAX_RECORDS = 64
_MAX_RECORD_BYTES = 64 * 1024 * 1024
_MAX_METADATA_BYTES = 4096
_MAX_LAYOUT_BYTES = 512  # shared small-record cap for layout.json/parents.json
_LAYOUT_RECORD = "layout.json"
_PARENTS_RECORD = "parents.json"
_MAX_PARENTS = 8
# Windows reserved device names cannot coexist with pi on Win32; pi itself
# refuses them, so no engine-legitimate journal ever uses one.
_RESERVED_BASENAMES = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{i}" for i in range(1, 10)}
    | {f"lpt{i}" for i in range(1, 10)}
)


class _MigrationMetadata(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    logical_id: str = Field(min_length=1, max_length=128)
    native_id: str = Field(min_length=1, max_length=128)
    generation: int = Field(ge=0)
    lineage: str = Field(min_length=1, max_length=2048)


class _LayoutRecord(BaseModel):
    """Versioned layout archive record: the original root journal basename."""

    model_config = ConfigDict(strict=True, extra="forbid")

    version: int = Field(ge=1, le=1)
    root_journal: str = Field(min_length=1, max_length=200)


def _safe_journal_basename(name: str) -> bool:
    """True only for one plain, non-hidden, ``.jsonl``-suffixed basename.

    The shared safety gate for every archive record that names a journal in
    the store: no directory component, no traversal or drive/alternate-stream
    colon, no control character, no Windows device stem. A basename is never
    authority for path resolution (the store path is operator-declared); it
    must still be unable to smuggle traversal, alternate data streams, device
    names, or symlink components into a store write.
    """
    stem, sep, suffix = name.rpartition(".")
    return bool(
        sep
        and suffix == "jsonl"
        and stem
        and not name.startswith(".")
        and ":" not in name
        and Path(name).name == name
        and "/" not in name
        and "\\" not in name
        and stem.casefold() not in _RESERVED_BASENAMES
        and _safe_name(name)
    )


def _layout_root_journal(data: bytes) -> str:
    """Validate a layout archive record; refuse anything unsafe fail-closed.

    See :func:`_safe_journal_basename` for the basename gate applied here.
    """
    try:
        if len(data) > _MAX_LAYOUT_BYTES:
            raise ValueError("layout too large")
        layout = _LayoutRecord.model_validate(
            json.loads(data, object_pairs_hook=_unique_pairs)
        )
        name = layout.root_journal
        if not _safe_journal_basename(name):
            raise ValueError("unsafe layout basename")
        return name
    except (ValueError, ValidationError) as exc:
        raise CapabilityUnavailable("LAYOUT_RECORD_REFUSED") from exc


class _ParentsRecord(BaseModel):
    """Versioned parents archive record: the parent journal basenames.

    pi's session header ``parentSession`` names another journal in the same
    store; the tree dangles if that journal is absent at restore.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    version: int = Field(ge=1, le=1)
    journals: list[Annotated[str, Field(min_length=1, max_length=200)]] = Field(
        min_length=1, max_length=_MAX_PARENTS
    )


def _parents_record_parse(data: bytes) -> list[str]:
    """Validate a parents archive record; refuse anything unsafe fail-closed.

    Same 512-byte small-record cap and duplicated-key rejection as the layout
    record, plus the shared basename gate on every entry: parent journals land
    in the same store as the child, and a basename repeated under different
    casing would describe one file in several parent slots.
    """
    try:
        if len(data) > _MAX_LAYOUT_BYTES:
            raise ValueError("parents record too large")
        record = _ParentsRecord.model_validate(
            json.loads(data, object_pairs_hook=_unique_pairs)
        )
        journals = record.journals
        if len({name.casefold() for name in journals}) != len(journals):
            raise ValueError("duplicate parent journal")
        if not all(_safe_journal_basename(name) for name in journals):
            raise ValueError("unsafe parent journal basename")
        return journals
    except (ValueError, ValidationError) as exc:
        raise CapabilityUnavailable("PARENTS_RECORD_REFUSED") from exc


def _read_bounded_header(record_path: Path) -> dict:
    """Read one journal header under the same 64 KiB first-line bound as lookup."""
    try:
        with record_path.open("rb") as stream:
            first = stream.readline(65537)
    except OSError as exc:
        raise CapabilityUnavailable("SESSION_RECORD_UNREADABLE") from exc
    if len(first) > 65536:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    try:
        header = json.loads(first)
    except (ValueError, UnicodeError) as exc:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED") from exc
    if not isinstance(header, dict):
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    return header


def _collect_parent_chain(
    profile: EnginePersistenceProfile,
    sessions_dir: Path,
    root_record_path: Path,
) -> list[Path]:
    """Walk pi's ``parentSession`` lineage: pure reader, root-nearest first.

    pi's header names its parent journal as an absolute path inside the same
    declared store, so a restore that drops the parent dangles the tree. The
    walk is path-based only (no directory scan, hence no lookup bound) and
    fail-closed: same-store directory, one plain non-hidden ``.jsonl``
    basename, no symlink, no missing file, no cycle (including self), and
    never more than ``_MAX_PARENTS`` hops. Full record bytes stay unread here;
    export performs the strict reads.
    """
    if profile.session_record != "pi":
        raise CapabilityUnavailable("PERSISTENCE_FORMAT_UNSUPPORTED")
    header = _read_bounded_header(root_record_path)
    root_id = header.get("id")
    if not isinstance(root_id, str) or not root_id:
        root_id = root_record_path.name
    seen = {root_id}
    chain: list[Path] = []
    parent = header.get("parentSession")
    while isinstance(parent, str) and parent:
        if len(chain) >= _MAX_PARENTS:
            raise CapabilityUnavailable("PARENT_REF_DEPTH")
        reference = Path(parent)
        if reference.parent != sessions_dir or not _safe_journal_basename(
            reference.name
        ):
            raise CapabilityUnavailable("PARENTS_RECORD_REFUSED")
        if reference.is_symlink():
            raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
        if not reference.is_file():
            raise CapabilityUnavailable("PARENT_REF_MISSING")
        header = _read_bounded_header(reference)
        parent_id = header.get("id")
        if not isinstance(parent_id, str) or not parent_id:
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        if parent_id in seen:
            raise CapabilityUnavailable("PARENT_REF_CYCLE")
        seen.add(parent_id)
        chain.append(reference)
        parent = header.get("parentSession")
    return chain


def _metadata_path(sessions_dir: Path, native_id: str) -> Path:
    directory = sessions_dir / ".untether-migration"
    if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
        raise CapabilityUnavailable("SESSION_METADATA_UNSAFE")
    if directory.exists() and os.name == "posix" and directory.stat().st_mode & 0o077:
        raise CapabilityUnavailable("SESSION_METADATA_UNSAFE")
    path = directory / f"{native_id}.json"
    if path.is_symlink():
        raise CapabilityUnavailable("SESSION_METADATA_UNSAFE")
    return path


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate layout/metadata key")
        result[key] = value
    return result


def _metadata_decode(data: bytes, native_id: str) -> _MigrationMetadata:
    try:
        if len(data) > _MAX_METADATA_BYTES:
            raise ValueError("metadata too large")
        metadata = _MigrationMetadata.model_validate(
            json.loads(data, object_pairs_hook=_unique_pairs)
        )
        if metadata.logical_id != native_id or metadata.native_id != native_id:
            raise ValueError("metadata identity mismatch")
        if not metadata.lineage.strip():
            raise ValueError("empty lineage")
        return metadata
    except (ValueError, ValidationError) as exc:
        raise CapabilityUnavailable("SESSION_METADATA_MALFORMED") from exc


def _validate_profile(
    engine: str,
    profile: EnginePersistenceProfile,
    sessions_dir: Path,
) -> None:
    """Fail closed unless the real store matches the declared layout."""
    if not sessions_dir.is_dir() or sessions_dir.is_symlink():
        raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY")
    try:
        resolved = sessions_dir.resolve(strict=True)
    except OSError as exc:
        raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY") from exc
    if resolved != sessions_dir:
        # Any symlink component escapes the declared store path.
        raise CapabilityUnavailable("PERSISTENCE_STORE_SYMLINK") from None
    if os.name == "posix" and (sessions_dir.stat().st_mode & 0o077):
        # Same check the runner applies to its own session dirs (pi.py: mkdir 0o700).
        raise CapabilityUnavailable("PERSISTENCE_STORE_PERMISSIONS") from None


def _header_matches_session(header: object, native_id: str) -> bool:
    """True only for the exact declared header: type session, version int 3,
    id exact match. Foreign entry types (message) or other versions sharing
    the id never match."""
    return (
        isinstance(header, dict)
        and header.get("type") == "session"
        and type(header.get("version")) is int
        and header["version"] == 3
        and header.get("id") == native_id
    )


def _project_path_key(value: object) -> tuple[str, ...] | None:
    """Compare absolute paths lexically, without consulting this host's filesystem."""
    if (
        not isinstance(value, str)
        or not value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        return None
    windows = PureWindowsPath(value)
    if windows.drive:
        # Only drive-rooted paths: reject UNC/device namespaces and drive-relative paths.
        if (
            len(windows.drive) != 2
            or windows.drive[1] != ":"
            or not windows.is_absolute()
        ):
            return None
        parts = windows.parts
        if (
            ".." in parts
            or not windows.drive[0].isascii()
            or not windows.drive[0].isalpha()
        ):
            return None
        return ("windows", windows.drive.lower(), *parts[1:])
    if not value.startswith("/") or value.startswith("//") or "\\" in value:
        return None
    path = PurePosixPath(value)
    if ".." in path.parts:
        return None
    return ("posix", *path.parts)


def _record_name_for(profile: EnginePersistenceProfile, native_id: str) -> str:
    if profile.session_record == "pi":
        return native_id + ".jsonl"
    raise CapabilityUnavailable("PERSISTENCE_FORMAT_UNSUPPORTED")


def _iter_layout(profile: EnginePersistenceProfile, sessions_dir: Path):
    if profile.session_record == "pi":
        return sessions_dir.glob("*.jsonl")
    raise CapabilityUnavailable("PERSISTENCE_FORMAT_UNSUPPORTED")


def _read_record_strict(
    profile: EnginePersistenceProfile, record_path: Path, native_id: str
) -> dict[str, bytes]:
    """Read and structurally verify the declared session record format."""
    if profile.session_record != "pi":
        raise CapabilityUnavailable("PERSISTENCE_FORMAT_UNSUPPORTED")
    if record_path.is_symlink() or not record_path.is_file():
        raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
    try:
        data = record_path.read_bytes()
    except OSError as exc:
        raise CapabilityUnavailable("SESSION_RECORD_UNREADABLE") from exc
    if not data or len(data) > _MAX_RECORD_BYTES or data.count(b"\n") == 0:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    first = data.split(b"\n", 1)[0].strip()
    try:
        header = json.loads(first)
    except ValueError as exc:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED") from exc
    if not _header_matches_session(header, native_id):
        raise CapabilityUnavailable("SESSION_RECORD_FORMAT_REFUSED")
    return {"session.jsonl": data}


def _write_record(
    profile: EnginePersistenceProfile,
    sessions_dir: Path,
    native_id: str,
    data: bytes,
    record_name: str | None = None,
) -> Path:
    """Collision-safe create: O_EXCL, never overwrite an existing native file."""
    if profile.session_record != "pi":
        raise CapabilityUnavailable("PERSISTENCE_FORMAT_UNSUPPORTED")
    name = record_name or _record_name_for(profile, native_id)
    record_path = sessions_dir / name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = None
    try:
        fd = os.open(record_path, flags, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as out:
            fd = None  # ownership moved to the file object
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
    except FileExistsError as exc:
        raise CapabilityUnavailable("SESSION_ID_COLLISION") from exc
    except OSError as exc:
        if fd is not None:
            os.close(fd)
        raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
    return record_path


def _probe_record(
    profile: EnginePersistenceProfile, record_path: Path, native_id: str
) -> None:
    """Independently re-read the record and verify identity matches the id."""
    stored = _read_record_strict(profile, record_path, native_id)
    if not stored or not stored.get("session.jsonl"):
        raise CapabilityUnavailable("RESTORE_PROBE_FAILED")


def _record_bytes_eq(left: bytes, right: bytes) -> bool:
    return hashlib.sha256(left).digest() == hashlib.sha256(right).digest()


def _capped_bytes_eq(path: Path, expected: bytes) -> bool:
    """True only for exact equality, reading at most ``len(expected) + 1`` bytes.

    An oversized or unreadable file is never slurped or treated as a match: the
    preflight must bound what it reads from a store it does not own.
    """
    try:
        with path.open("rb") as stream:
            actual = stream.read(len(expected) + 1)
    except OSError as exc:
        raise CapabilityUnavailable("SESSION_RECORD_UNREADABLE") from exc
    return actual == expected


@dataclass(frozen=True, slots=True)
class FileBundlePersistenceAdapter:
    """Native adapter over one validated per-engine file layout.

    Export reads the declared record verbatim; import writes it byte-exactly
    with O_EXCL, then proves the restore with the engine's own restart probe
    (exact-id session selection over ``--session-dir``), re-reading the bytes
    it wrote.
    """

    engine: str
    profile: EnginePersistenceProfile
    sessions_dir: Path
    restart_runner: Callable[[tuple[str, ...]], None] | None = None
    run_argv: Callable[[list[str], float], None] | None = None

    def __post_init__(self) -> None:
        _validate_profile(self.engine, self.profile, self.sessions_dir)

    # -- helpers ---------------------------------------------------------

    def _resolve_native_id(self, native_id: str) -> Path:
        if (
            not native_id
            or len(native_id) > 128
            or not _safe_name(native_id)
            or Path(native_id).name != native_id
            or "/" in native_id
            or "\\" in native_id
        ):
            raise CapabilityUnavailable("NATIVE_ID_INVALID")
        return self.sessions_dir / _record_name_for(self.profile, native_id)

    def _run_restart_probe(self, native_id: str, record_path: Path) -> None:
        # Scope-verify argv FIRST: every execution path receives the same
        # substituted argv, and an unscoped probe is refused before any runner.
        template = list(self.profile.restart_argv)
        if not any(
            "{session_id}" in arg or "{session_path}" in arg for arg in template
        ):
            raise CapabilityUnavailable("SESSION_PROBE_UNSCOPED")
        replacements = {
            "{session_id}": native_id,
            "{session_path}": str(record_path),
            "{sessions_dir}": str(self.sessions_dir),
        }
        argv = []
        for arg in template:
            for key, value in replacements.items():
                arg = arg.replace(key, value)
            argv.append(arg)
        if self.restart_runner is not None:
            self.restart_runner(tuple(argv))
            return
        if self.run_argv is not None:
            self.run_argv(argv, self.profile.restart_timeout_s)
            return
        try:
            result = subprocess.run(
                argv,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=self.profile.restart_timeout_s,
                check=False,
                # Validated operator-configured probe argv, not chat input.
                shell=False,  # nosec B603
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        if result.returncode != 0:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED")

    # -- PersistenceAdapter protocol --------------------------------------

    @contextmanager
    def export_snapshot(self, session_id: str) -> Iterator[NativeArchive]:
        gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not gate_root:
            raise CapabilityUnavailable("SOURCE_GATE_UNCONFIGURED")
        from .session_gate import session_gate

        with session_gate(
            root=Path(gate_root),
            engine_id=self.engine,
            native_id=session_id,
        ):
            yield self._export_snapshot(session_id)

    def export_exact(self, session_id: str) -> NativeArchive:
        with self.export_snapshot(session_id) as archive:
            return archive

    def export_locked(self, session_id: str) -> NativeArchive:
        """Read only under a live migration lock owned by the caller context."""
        from .session_gate import assert_migration_lock_owned

        root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not root:
            raise CapabilityUnavailable("SOURCE_GATE_UNCONFIGURED")
        assert_migration_lock_owned(
            root=Path(root), engine_id=self.engine, native_id=session_id
        )
        return self._export_snapshot(session_id)

    def lookup_exact(self, session_id: str) -> str:
        """Return store-local identity only, never proof of project membership.

        The caller must authorize the project independently. This bounded header
        lookup does not export content, acquire gates, or run restart probes.
        """
        if (
            not self.profile.sessions_dir
            or Path(self.profile.sessions_dir) != self.sessions_dir
        ):
            raise CapabilityUnavailable("PERSISTENCE_STORE_SCOPE_MISMATCH")
        _validate_profile(self.engine, self.profile, self.sessions_dir)
        self._lookup_record(session_id)
        return session_id

    def lookup_for_project(self, session_id: str, project_root: Path) -> str:
        """Prove exact header project identity only; never export or acquire gates."""
        try:
            if (
                not self.profile.sessions_dir
                or Path(self.profile.sessions_dir) != self.sessions_dir
            ):
                raise CapabilityUnavailable("PERSISTENCE_STORE_SCOPE_MISMATCH")
            _validate_profile(self.engine, self.profile, self.sessions_dir)
            key = _project_path_key(str(project_root))
            if key is None:
                raise CapabilityUnavailable("SESSION_PROJECT_REFUSED")
            self._lookup_record(session_id, project_key=key)
        except (CapabilityUnavailable, OSError, ValueError):
            raise CapabilityUnavailable("SESSION_PROJECT_REFUSED") from None
        return session_id

    def _lookup_record(
        self, session_id: str, *, project_key: tuple[str, ...] | None = None
    ) -> Path:
        self._resolve_native_id(session_id)  # validate before directory lookup
        matches = []
        for count, candidate in enumerate(self.sessions_dir.glob("*.jsonl"), 1):
            if count > 10000:
                raise CapabilityUnavailable("SESSION_LOOKUP_LIMIT")
            if candidate.is_symlink():
                raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
            with candidate.open("rb") as stream:
                first = stream.readline(65537)
            if len(first) > 65536:
                raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
            try:
                header = json.loads(first)
            except (ValueError, UnicodeError):
                continue
            if _header_matches_session(header, session_id):
                # Count identity matches before authorization: duplicates stay ambiguous.
                matches.append((candidate, header))
        if len(matches) != 1:
            raise CapabilityUnavailable(
                "SESSION_LOOKUP_AMBIGUOUS" if matches else "SESSION_NOT_FOUND"
            )
        candidate, header = matches[0]
        if (
            project_key is not None
            and _project_path_key(header.get("cwd")) != project_key
        ):
            raise CapabilityUnavailable("SESSION_PROJECT_REFUSED")
        return candidate

    def _export_snapshot(self, session_id: str) -> NativeArchive:
        record_path = self._lookup_record(session_id)
        records = _read_record_strict(self.profile, record_path, session_id)
        # pi's parentSession lineage must survive the move: each parent journal
        # is carried as its own record so the restored tree does not dangle.
        parents = _collect_parent_chain(self.profile, self.sessions_dir, record_path)
        if len(parents) + (1 if parents else 0) + len(records) > _MAX_RECORDS:
            raise CapabilityUnavailable("PARENT_REF_DEPTH")
        for index, parent_path in enumerate(parents, 1):
            header = _read_bounded_header(parent_path)
            parent_id = header.get("id")
            if not isinstance(parent_id, str) or not parent_id:
                raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
            records[f"parent-{index}.jsonl"] = _read_record_strict(
                self.profile, parent_path, parent_id
            )["session.jsonl"]
        if parents:
            records[_PARENTS_RECORD] = json.dumps(
                {"version": 1, "journals": [p.name for p in parents]},
                sort_keys=True,
            ).encode()
        # Keep the original non-canonical root basename: native parentSession
        # pointers reference journal paths and would dangle after a rename.
        canonical = _record_name_for(self.profile, session_id)
        if record_path.name != canonical:
            records[_LAYOUT_RECORD] = json.dumps(
                {"version": 1, "root_journal": record_path.name},
                sort_keys=True,
            ).encode()
        metadata_path = _metadata_path(self.sessions_dir, session_id)
        metadata = None
        if metadata_path.exists():
            try:
                with metadata_path.open("rb") as stream:
                    metadata = _metadata_decode(
                        stream.read(_MAX_METADATA_BYTES + 1), session_id
                    )
            except OSError as exc:
                raise CapabilityUnavailable("SESSION_METADATA_UNREADABLE") from exc
        # Metadata records imported identity, not a version of mutable engine history.
        return NativeArchive(
            engine=self.engine,
            logical_id=session_id,
            native_id=session_id,
            generation=metadata.generation if metadata else 0,
            lineage=(
                metadata.lineage
                if metadata
                else f"file-bundle:{self.profile.session_record}:{record_path.name}"
            ),
            records=records,
            digest=hashlib.sha256(b"".join(sorted(records.values()))).hexdigest(),
        )

    def import_exact(self, archive: NativeArchive) -> RestoreProof:
        if archive.engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        if archive.logical_id != archive.native_id or not archive.records:
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        allowed = {"session.jsonl", _LAYOUT_RECORD, _PARENTS_RECORD} | {
            f"parent-{index}.jsonl" for index in range(1, _MAX_PARENTS + 1)
        }
        if not {"session.jsonl"} <= set(archive.records) <= allowed:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        layout_name = (
            _layout_root_journal(archive.records[_LAYOUT_RECORD])
            if _LAYOUT_RECORD in archive.records
            else None
        )
        data = archive.records["session.jsonl"]
        if len(data) > _MAX_RECORD_BYTES or not data.count(b"\n"):
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        self._resolve_native_id(archive.native_id)  # traversal guard for metadata path
        record_path = (
            self.sessions_dir / layout_name
            if layout_name
            else self._resolve_native_id(archive.native_id)
        )
        # Pure, byte-only parent-pairing preflight before ANY store write.
        from .pi_parent_archive import validate_parent_records

        parents = validate_parent_records(
            archive.records, record_path.name, archive.native_id
        )
        metadata_path = _metadata_path(self.sessions_dir, archive.native_id)
        metadata = _metadata_decode(
            json.dumps(
                {
                    "logical_id": archive.logical_id,
                    "native_id": archive.native_id,
                    "generation": archive.generation,
                    "lineage": archive.lineage,
                }
            ).encode(),
            archive.native_id,
        )
        # Preflight loop: every destination is validated before ANY write —
        # reject symlinks/non-files, never adopt foreign bytes, and reuse a
        # parent only when byte-identical (the root never, collision instead).
        for basename, payload in parents:
            destination = self.sessions_dir / basename
            if destination.is_symlink() or (
                destination.exists() and not destination.is_file()
            ):
                raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
            if destination.exists() and not _capped_bytes_eq(destination, payload):
                raise CapabilityUnavailable("SESSION_ID_COLLISION")
        if record_path.is_symlink() or (
            record_path.exists() and not record_path.is_file()
        ):
            raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
        if record_path.exists():
            raise CapabilityUnavailable("SESSION_ID_COLLISION")
        owned: list[Path] = []  # only files this import created may be removed
        metadata_owned = False
        try:
            for basename, payload in parents:
                destination = self.sessions_dir / basename
                if destination.exists():
                    continue  # byte-identical preflight reuse: never owned
                _write_record(
                    self.profile,
                    self.sessions_dir,
                    json.loads(payload.split(b"\n", 1)[0])["id"],
                    payload,
                    record_name=basename,
                )
                owned.append(destination)
            # Parents first, then the root: a truncated restore never leaves a
            # root whose lineage is missing.
            _write_record(
                self.profile,
                self.sessions_dir,
                archive.native_id,
                data,
                record_name=layout_name,
            )
            owned.append(record_path)
            metadata_path.parent.mkdir(mode=0o700, exist_ok=True)
            _metadata_path(self.sessions_dir, archive.native_id)
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(metadata_path, flags, 0o600)
            metadata_owned = True
            with os.fdopen(fd, "wb") as stream:
                stream.write(metadata.model_dump_json().encode())
                stream.flush()
                os.fsync(stream.fileno())
            # 1) exact header check on the bytes we just wrote
            _probe_record(self.profile, record_path, archive.native_id)
            # 2) the engine's own restart probe against the declared store
            self._run_restart_probe(archive.native_id, record_path)
            # 3) record still intact after the restart probe
            stored = _read_record_strict(self.profile, record_path, archive.native_id)
            # 4) every parent re-read from disk for exact bytes: hashes are
            # claimed only for parents that verifiably survived the restart.
            for basename, payload in parents:
                parent_path = self.sessions_dir / basename
                if parent_path.is_symlink() or not parent_path.is_file():
                    raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
                if not _capped_bytes_eq(parent_path, payload):
                    raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        except (CapabilityUnavailable, OSError) as exc:
            for path in owned:  # remove only this import's files
                path.unlink(missing_ok=True)
            if metadata_owned:
                metadata_path.unlink(missing_ok=True)
            if isinstance(exc, OSError):
                raise CapabilityUnavailable("SESSION_METADATA_WRITE_FAILED") from exc
            raise
        if not _record_bytes_eq(stored["session.jsonl"], data):
            for path in owned:
                path.unlink(missing_ok=True)
            metadata_path.unlink(missing_ok=True)
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        return RestoreProof(
            logical_id=archive.logical_id,
            native_id=archive.native_id,
            generation=archive.generation,
            lineage=archive.lineage,
            record_hashes={
                name: hashlib.sha256(blob).hexdigest()
                for name, blob in archive.records.items()
            },
            restarted=True,
            resumed=True,
        )

    def import_overwrite(
        self,
        *,
        engine: str,
        native_id: str,
        records: dict[str, bytes],
        expected_prior_records: dict[str, bytes],
        generation: int = 0,
        lineage: str = "",
    ) -> dict[str, str]:
        """Atomically replace one existing record as part of a fenced reclaim.

        Refuses unless the engine matches, the layout is exactly the declared
        single session record, and the CURRENT stored bytes equal
        ``expected_prior_records["session.jsonl"]`` — a divergent store is
        never touched. The replacement is an atomic ``os.replace`` of a fully
        fsynced temp file in the same directory, so a crash can never leave a
        partial record. A pending coordinator fence must remain until both
        metadata and restart proof verify; retry accepts already replaced bytes.
        """
        if engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        prior_layout = expected_prior_records.get(_LAYOUT_RECORD)
        layout_name = (
            _layout_root_journal(records[_LAYOUT_RECORD])
            if _LAYOUT_RECORD in records
            else None
        )
        # A reclaim may never rename the store record: the layout record must
        # be present on both sides with identical bytes, or absent on both.
        if (_LAYOUT_RECORD in records) != (_LAYOUT_RECORD in expected_prior_records):
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if layout_name is not None and records[_LAYOUT_RECORD] != prior_layout:
            raise CapabilityUnavailable("OVERWRITE_PRIOR_MISMATCH")
        record_names = {"session.jsonl"} | (
            {_LAYOUT_RECORD} if layout_name is not None else set()
        )
        if set(records) != record_names or set(expected_prior_records) != record_names:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        data = records["session.jsonl"]
        prior = expected_prior_records["session.jsonl"]
        if len(data) > _MAX_RECORD_BYTES or not data.count(b"\n"):
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        # Validate incoming identity BEFORE replacing any durable bytes.
        try:
            header = json.loads(data.split(b"\n", 1)[0])
        except ValueError as exc:
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED") from exc
        if not _header_matches_session(header, native_id):
            raise CapabilityUnavailable("SESSION_RECORD_FORMAT_REFUSED")
        metadata = _metadata_decode(
            json.dumps(
                {
                    "logical_id": native_id,
                    "native_id": native_id,
                    "generation": generation,
                    "lineage": lineage,
                }
            ).encode(),
            native_id,
        )
        metadata_path = _metadata_path(self.sessions_dir, native_id)
        self._resolve_native_id(native_id)  # traversal guard for metadata path
        record_path = (
            self.sessions_dir / layout_name
            if layout_name
            else self._resolve_native_id(native_id)
        )
        if record_path.is_symlink() or not record_path.is_file():
            raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
        current = record_path.read_bytes()
        if current not in (prior, data):
            raise CapabilityUnavailable("OVERWRITE_PRIOR_MISMATCH")
        # ponytail: prior-bytes pin reuses the digest above; records-level
        # pinning is the file adapter's single-record specialty.
        tmp_name: str | None = None
        try:
            fd, tmp_name = tempfile.mkstemp(
                dir=self.sessions_dir, prefix=".reclaim-", suffix=".tmp"
            )
            tmp = Path(tmp_name)
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, record_path)
            tmp_name = None
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
        finally:
            if tmp_name is not None:
                Path(tmp_name).unlink(missing_ok=True)
        # The two atomic replacements are deliberately not claimed as one
        # transaction. Pending ownership fences admission through any crash gap.
        metadata_path.parent.mkdir(mode=0o700, exist_ok=True)
        _metadata_path(self.sessions_dir, native_id)
        tmp_name = None
        try:
            fd, tmp_name = tempfile.mkstemp(
                dir=metadata_path.parent, prefix=".reclaim-"
            )
            with os.fdopen(fd, "wb") as out:
                out.write(metadata.model_dump_json().encode())
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_name, metadata_path)
            tmp_name = None
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_METADATA_WRITE_FAILED") from exc
        finally:
            if tmp_name is not None:
                Path(tmp_name).unlink(missing_ok=True)
        self._run_restart_probe(native_id, record_path)
        stored = _read_record_strict(self.profile, record_path, native_id)
        if _metadata_decode(metadata_path.read_bytes(), native_id) != metadata:
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        if not _record_bytes_eq(stored["session.jsonl"], data):
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        return {
            name: hashlib.sha256(blob).hexdigest() for name, blob in records.items()
        }


def register_from_settings(settings: SessionMigrationSettings, *, engine: str) -> None:
    """Register the in-tree file-bundle adapter for one declared engine profile.

    No-op when the engine has no persistence profile (plugin entry points stay
    authoritative) or when another registration already claims the engine.
    """
    from .session_gate import configure_session_gate

    configure_session_gate(settings)
    profile = settings.persistence.engines.get(engine)
    if profile is None:
        return
    if profile.format == "omp-v1":
        # Route before any file-layout path math: the OMP adapter owns its own
        # realdir validation at construction and never creates stores implicitly.
        if engine in registered_adapter_engines():
            return
        register_persistence_adapter(
            engine,
            lambda: OmpPersistenceAdapter(engine=engine, profile=profile),
        )
        return
    if profile.format == "executable-v1":
        # Route before any file-layout path math: the executable adapter owns
        # its own realdir/executable validation at call time and never creates
        # stores implicitly.
        if engine in registered_adapter_engines():
            return
        register_persistence_adapter(
            engine,
            lambda: ExecutablePersistenceAdapter(
                engine=engine,
                executable=Path(profile.executable),
                store=Path(profile.store),
                timeout=profile.timeout,
                interpreter=profile.interpreter,
            ),
        )
        return
    if profile.format == "codex-rollout-v1":
        # Codex rollout journals under a disposable CODEX_HOME/sessions tree:
        # the dedicated adapter owns store validation and the app-server
        # resume proof.
        if engine in registered_adapter_engines():
            return
        from .codex_rollout_adapter import CodexRolloutPersistenceAdapter

        register_persistence_adapter(
            engine,
            lambda: CodexRolloutPersistenceAdapter(
                engine=engine,
                sessions_root=Path(profile.sessions_root),
                app_server_executable=Path(profile.app_server_executable),
                resume_timeout_s=profile.resume_timeout_s,
            ),
        )
        return
    if profile.format == "opencode-sync-v1":
        # OpenCode durable sync events via the local serve API: the dedicated
        # adapter owns isolation-root validation and the serve/replay proof.
        if engine in registered_adapter_engines():
            return
        from .opencode_sync_adapter import OpenCodeSyncAdapter

        register_persistence_adapter(
            engine,
            lambda: OpenCodeSyncAdapter(engine=engine, profile=profile),
        )
        return
    sessions_dir = Path(profile.sessions_dir)
    if engine in registered_adapter_engines():
        return
    register_persistence_adapter(
        engine,
        lambda: FileBundlePersistenceAdapter(
            engine=engine, profile=profile, sessions_dir=sessions_dir
        ),
    )
