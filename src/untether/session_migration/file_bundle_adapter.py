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


class _MigrationMetadata(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    logical_id: str = Field(min_length=1, max_length=128)
    native_id: str = Field(min_length=1, max_length=128)
    generation: int = Field(ge=0)
    lineage: str = Field(min_length=1, max_length=2048)


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


def _metadata_decode(data: bytes, native_id: str) -> _MigrationMetadata:
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate metadata key")
            result[key] = value
        return result

    try:
        if len(data) > _MAX_METADATA_BYTES:
            raise ValueError("metadata too large")
        metadata = _MigrationMetadata.model_validate(
            json.loads(data, object_pairs_hook=unique_pairs)
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
        # Any symlink component in the declared store escapes the declared
        # path; the operator must declare the real directory itself.
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
) -> Path:
    """Collision-safe create: O_EXCL, never overwrite an existing native file."""
    if profile.session_record != "pi":
        raise CapabilityUnavailable("PERSISTENCE_FORMAT_UNSUPPORTED")
    record_path = sessions_dir / _record_name_for(profile, native_id)
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
        # Build and validate the scoped argv FIRST: every execution path —
        # injected runner, injected argv runner, subprocess — receives the
        # same substituted, scope-verified argv. An unscoped probe is refused
        # before any runner is invoked.
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
                # Count identity matches before project authorization: duplicate IDs
                # remain ambiguous even when only one declares the requested cwd.
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
        if set(archive.records) != {"session.jsonl"}:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        data = archive.records["session.jsonl"]
        if len(data) > _MAX_RECORD_BYTES or not data.count(b"\n"):
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        record_path = self._resolve_native_id(archive.native_id)
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
        _write_record(self.profile, self.sessions_dir, archive.native_id, data)
        metadata_owned = False
        try:
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
        except (CapabilityUnavailable, OSError) as exc:
            record_path.unlink(missing_ok=True)  # remove only this import's files
            if metadata_owned:
                metadata_path.unlink(missing_ok=True)
            if isinstance(exc, OSError):
                raise CapabilityUnavailable("SESSION_METADATA_WRITE_FAILED") from exc
            raise
        if not _record_bytes_eq(stored["session.jsonl"], data):
            record_path.unlink(missing_ok=True)
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
        if set(records) != {"session.jsonl"}:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if set(expected_prior_records) != {"session.jsonl"}:
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
        record_path = self._resolve_native_id(native_id)
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
        return {"session.jsonl": hashlib.sha256(data).hexdigest()}


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
