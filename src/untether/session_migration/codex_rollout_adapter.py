"""Concrete persistence adapter for Codex rollout stores (``codex-rollout-v1``).

Every Codex session is one opaque JSONL rollout file under
``sessions/YYYY/MM/DD/rollout-*.jsonl`` inside the engine's CODEX_HOME; line 1
is the strict identity header ``{"type":"session_meta","payload":{"session_id",
"cwd",...}}``. Export reads the exact rollout bytes; import restores them
byte-for-byte with ``O_EXCL`` at the lineage-pinned path (parents created
``0o700``) and proves the restore by resuming the session through the real
``codex app-server`` JSON-RPC flow (initialize -> initialized -> thread/resume
-> thread/read). Proof requires the echoed thread id/cwd/model/provider to
match the rollout header exactly (cwd compared via ``os.path.normcase``) and
persisted history to remain visible through thread/read (including stable native
completed-item IDs where present), and the post-probe rollout on disk to keep
the pre-probe bytes as an exact prefix
(the engine appends a ``thread_settings_applied`` event on resume).

Generation/lineage identity is durably preserved in a separate validated
metadata record (``untether.json``) carried inside the archive — never inside
the mutable rollout — so evolved engine history cannot forge imported
identity. Lineage pins the exact relative rollout path under ``sessions/``
(``codex-rollout-v1:2026/10/27/rollout-....jsonl``), validated as a safe
relative path on both export and import.

The resume probe never touches ``auth.json``, ``config.toml`` or any path
outside ``sessions/``: the child runs with an isolated CODEX_HOME and a
scrubbed environment. Unit tests inject ``rpc_resume`` so no process is ever
spawned; the real subprocess probe is the default only.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import shutil
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .native_adapter import CapabilityUnavailable, RestoreProof
from .native_archive import NativeArchive, _safe_name

_METADATA_RECORD = "untether.json"
_LINEAGE_PREFIX = "codex-rollout-v1:"
_MAX_METADATA_BYTES = 4096
_MAX_JOURNAL_BYTES = 64 * 1024 * 1024
_MAX_HEADER_BYTES = 65536
_MAX_SCAN_ENTRIES = 10000
_MAX_FILES_PER_DAY = 4096
_YEAR_RE = re.compile(r"\d{4}")
_DAY_RE = re.compile(r"\d{2}")
# Only what Windows needs to spawn a process; everything engine- or
# identity-specific (credentials, config, telemetry) is scrubbed.
_PROBE_ENV_KEYS = frozenset(
    {"SYSTEMROOT", "WINDIR", "PATH", "PATHEXT", "COMSPEC", "SYSTEMDRIVE"}
)

RpcResume = Callable[[str, Path, float], None]


class _CodexRolloutMetadata(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    logical_id: str = Field(min_length=1, max_length=128)
    native_id: str = Field(min_length=1, max_length=128)
    generation: int = Field(ge=0)
    lineage: str = Field(min_length=1, max_length=2048)


def _metadata_for(
    native_id: str, generation: int, lineage: str
) -> _CodexRolloutMetadata:
    """Validate identity into the metadata record shape, fail closed."""
    try:
        metadata = _CodexRolloutMetadata.model_validate(
            {
                "logical_id": native_id,
                "native_id": native_id,
                "generation": generation,
                "lineage": lineage,
            }
        )
    except ValidationError as exc:
        raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID") from exc
    if not metadata.lineage.strip():
        raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
    return metadata


def _decode_metadata(data: bytes, native_id: str) -> _CodexRolloutMetadata:
    try:
        if len(data) > _MAX_METADATA_BYTES:
            raise ValueError("metadata too large")
        metadata = _CodexRolloutMetadata.model_validate_json(data)
        if metadata.logical_id != native_id or metadata.native_id != native_id:
            raise ValueError("metadata identity mismatch")
        if not metadata.lineage.strip():
            raise ValueError("empty lineage")
        return metadata
    except (ValueError, ValidationError) as exc:
        raise CapabilityUnavailable("SESSION_METADATA_MALFORMED") from exc


def _journal_name_from_lineage(lineage: str) -> str:
    """Extract and validate the rollout's sessions/-relative path."""
    if not lineage.startswith(_LINEAGE_PREFIX):
        raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
    name = lineage[len(_LINEAGE_PREFIX) :]
    # _safe_name refuses empty names, POSIX absolute paths, backslashes,
    # ""/"."/".." parts and control characters; the PureWindowsPath check
    # additionally refuses drive-absolute names ("C:/x.jsonl") which would
    # otherwise REPLACE the sessions root when joined on Windows.
    if not _safe_name(name) or PureWindowsPath(name).drive:
        raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
    return name


def _header_matches_session(header: object, native_id: str) -> bool:
    """True only for the exact declared header: type session_meta, a dict
    payload, and payload.session_id exact match. Foreign entry types sharing
    the id never match."""
    return (
        isinstance(header, dict)
        and header.get("type") == "session_meta"
        and isinstance(header.get("payload"), dict)
        and header["payload"].get("session_id") == native_id
    )


def _header_from_journal(data: bytes, *, strict_id: str | None = None) -> dict | None:
    """Parse line 1 as the session_meta header.

    ``strict_id=None`` (store lookup): a foreign first line yields ``None`` so
    unrelated files are skipped; a meta-shaped line without a dict payload is
    refused as malformed. With ``strict_id`` (import/export): every deviation
    raises — the identity carrier must be exactly right.
    """
    first = data.split(b"\n", 1)[0].strip()
    if len(first) > _MAX_HEADER_BYTES:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    try:
        header = json.loads(first)
    except (ValueError, UnicodeError) as exc:
        if strict_id is not None:
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED") from exc
        return None
    if not isinstance(header, dict) or header.get("type") != "session_meta":
        if strict_id is not None:
            raise CapabilityUnavailable("SESSION_RECORD_FORMAT_REFUSED")
        return None
    payload = header.get("payload")
    if not isinstance(payload, dict):
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    if strict_id is not None and payload.get("session_id") != strict_id:
        raise CapabilityUnavailable("SESSION_RECORD_FORMAT_REFUSED")
    return header


def _digit_dirs(parent: Path, pattern: re.Pattern[str], limit: int) -> Iterator[Path]:
    """Bounded, link-free scan yielding exactly digit-named child dirs."""
    with os.scandir(parent) as scan:
        for count, entry in enumerate(scan, 1):
            if count > limit:
                raise CapabilityUnavailable("SESSION_LOOKUP_LIMIT")
            if entry.is_symlink():
                raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
            if not entry.is_dir(follow_symlinks=False) or not pattern.fullmatch(
                entry.name
            ):
                continue
            yield Path(entry.path)


def _norm(value: object) -> str:
    return os.path.normcase(value) if isinstance(value, str) else ""


def _assert_resume_echo(
    result: object, native_id: str, cwd: str, model: str, provider: str
) -> None:
    """thread/resume must echo id (nested) and cwd/model/provider (top level)."""
    ok = (
        isinstance(result, dict)
        and isinstance(result.get("thread"), dict)
        and result["thread"].get("id") == native_id
        and _norm(result.get("cwd")) == os.path.normcase(cwd)
        and result.get("model") == model
        and result.get("modelProvider") == provider
    )
    if not ok:
        raise CapabilityUnavailable("RESTART_PROBE_FAILED")


def _assert_read_echo(result: object, native_id: str, cwd: str) -> None:
    """thread/read must echo the same thread id and cwd back."""
    thread = result.get("thread") if isinstance(result, dict) else None
    ok = (
        isinstance(thread, dict)
        and thread.get("id") == native_id
        and _norm(thread.get("cwd")) == os.path.normcase(cwd)
    )
    if not ok:
        raise CapabilityUnavailable("RESTART_PROBE_FAILED")


def _assert_read_history(data: bytes, result: object) -> None:
    """Refuse empty replay of persisted history; pin native completed item IDs.

    This is a history visibility check, not proof of model continuation or tool
    execution. Legacy response items do not always carry stable item IDs.
    """
    has_history = False
    expected_ids: set[str] = set()
    try:
        for line in data.splitlines()[1:]:
            record = json.loads(line)
            payload = record.get("payload", {})
            if (
                record.get("type") == "response_item"
                and payload.get("type")
                in {"message", "function_call", "function_call_output"}
            ) or (
                record.get("type") == "event_msg"
                and payload.get("type")
                in {"item_completed", "user_message", "agent_message"}
            ):
                has_history = True
                item = payload.get("item", {})
                if isinstance(item, dict) and isinstance(item.get("id"), str):
                    expected_ids.add(item["id"])
        if not has_history:
            return
        thread = result.get("thread") if isinstance(result, dict) else None
        turns = thread.get("turns") if isinstance(thread, dict) else None
        if not isinstance(turns, list):
            raise ValueError("missing turns")
        items = [item for turn in turns for item in turn["items"]]
        if not items or not expected_ids.issubset({item["id"] for item in items}):
            raise ValueError("missing history items")
    except (ValueError, UnicodeError, AttributeError, KeyError, TypeError) as exc:
        raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc


def _validate_rollout_store(sessions_root: Path) -> None:
    """Fail closed unless the declared root is a real, link-free directory."""
    if not sessions_root.is_dir() or sessions_root.is_symlink():
        raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY")
    try:
        resolved = sessions_root.resolve(strict=True)
    except OSError as exc:
        raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY") from exc
    if resolved != sessions_root:
        raise CapabilityUnavailable("PERSISTENCE_STORE_SYMLINK") from None
    if os.name == "posix" and (sessions_root.stat().st_mode & 0o077):
        raise CapabilityUnavailable("PERSISTENCE_STORE_PERMISSIONS") from None


@dataclass(frozen=True, slots=True)
class CodexRolloutPersistenceAdapter:
    """Codex rollout persistence: lookup -> archive -> restore -> RPC proof.

    ``rpc_resume(native_id, journal_path, timeout_s)`` is the injection seam
    for the restart probe: unit tests inject fakes so no process is ever
    spawned; production defaults to the real bounded app-server JSON-RPC flow.
    """

    engine: str
    sessions_root: Path
    app_server_executable: Path
    resume_timeout_s: float = 120.0
    rpc_resume: RpcResume | None = None

    def __post_init__(self) -> None:
        if self.resume_timeout_s <= 0:
            raise CapabilityUnavailable("PERSISTENCE_LIMIT_INVALID")
        _validate_rollout_store(self.sessions_root)

    # -- restart probe ----------------------------------------------------

    def _run_resume_probe(
        self, native_id: str, journal_path: Path, expected_bytes: bytes
    ) -> None:
        """Resume via the injected (or real) probe, then verify the rollout on
        disk kept the pre-probe bytes as an exact prefix: a successful resume
        only ever appends events."""
        if self.rpc_resume is not None:
            self.rpc_resume(native_id, journal_path, self.resume_timeout_s)
        else:
            self._default_rpc_resume(native_id, journal_path, self.resume_timeout_s)
        try:
            after = journal_path.read_bytes()
        except OSError as exc:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        if not after.startswith(expected_bytes):
            raise CapabilityUnavailable("RESTART_PROBE_FAILED")

    def _default_rpc_resume(
        self, native_id: str, journal_path: Path, timeout_s: float
    ) -> object:
        """Real probe: drive codex app-server's JSON-RPC resume flow.

        The child runs with an isolated CODEX_HOME (the declared store's
        parent) and a scrubbed environment, so the probe never reads
        ``auth.json``, ``config.toml`` or any path outside ``sessions/``.
        Echoed id/cwd/model/provider must match the rollout header exactly.
        """
        try:
            data = journal_path.read_bytes()
        except OSError as exc:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        header = _header_from_journal(data, strict_id=native_id)
        if header is None:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED")
        payload = header["payload"]
        cwd = payload.get("cwd")
        instructions = payload.get("base_instructions")
        provenance = (
            instructions.get("provenance") if isinstance(instructions, dict) else None
        )
        model = provenance.get("model") if isinstance(provenance, dict) else None
        provider = payload.get("model_provider")
        if not (
            isinstance(cwd, str)
            and isinstance(model, str)
            and isinstance(provider, str)
        ):
            raise CapabilityUnavailable("RESTART_PROBE_FAILED")
        if (
            self.app_server_executable.is_symlink()
            or not self.app_server_executable.is_file()
        ):
            raise CapabilityUnavailable("PERSISTENCE_EXECUTABLE_NOT_READY")
        env = {
            key: value
            for key, value in os.environ.items()
            if key.upper() in _PROBE_ENV_KEYS
        }
        home = self.sessions_root.parent
        for key in ("CODEX_HOME", "HOME", "USERPROFILE"):
            env[key] = str(home)
        scratch = tempfile.mkdtemp(prefix="untether-codex-probe-")
        for key in (
            "LOCALAPPDATA",
            "APPDATA",
            "TEMP",
            "TMP",
            "XDG_CONFIG_HOME",
            "XDG_CACHE_HOME",
            "XDG_DATA_HOME",
        ):
            location = Path(scratch) / key.lower()
            location.mkdir()
            env[key] = str(location)
        env["PYTHONUTF8"] = "1"
        env["OTEL_SDK_DISABLED"] = "true"
        messages: queue.Queue[str | None] = queue.Queue()
        deadline = time.monotonic() + timeout_s
        try:
            proc = subprocess.Popen(
                [
                    str(self.app_server_executable),
                    "-c",
                    "check_for_update_on_startup=false",
                    "-c",
                    "analytics.enabled=false",
                    "-c",
                    "feedback.enabled=false",
                    "app-server",
                ],
                cwd=str(home),
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
            )
        except (OSError, ValueError) as exc:
            shutil.rmtree(scratch, ignore_errors=True)
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        try:
            stdin, stdout = proc.stdin, proc.stdout
            if stdin is None or stdout is None:
                raise CapabilityUnavailable("RESTART_PROBE_FAILED")

            def _reader() -> None:
                for line in stdout:
                    messages.put(line)
                messages.put(None)

            threading.Thread(target=_reader, daemon=True).start()

            def _send(
                method: str, params: dict, request_id: int | None = None
            ) -> object:
                request = {
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                }
                if request_id is not None:
                    request["id"] = request_id
                try:
                    stdin.write(json.dumps(request) + "\n")
                    stdin.flush()
                except (OSError, ValueError, TypeError, AttributeError) as exc:
                    raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
                if request_id is None:
                    return None
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise CapabilityUnavailable("RESTART_PROBE_FAILED")
                    try:
                        line = messages.get(timeout=remaining)
                    except queue.Empty as exc:
                        raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
                    if line is None:  # child exited before replying
                        raise CapabilityUnavailable("RESTART_PROBE_FAILED")
                    try:
                        response = json.loads(line)
                    except (ValueError, UnicodeError) as exc:
                        raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
                    if (
                        not isinstance(response, dict)
                        or response.get("id") != request_id
                    ):
                        continue
                    if "error" in response:
                        error = response["error"]
                        code = error.get("code") if isinstance(error, dict) else None
                        raise CapabilityUnavailable(
                            f"RESTART_PROBE_FAILED: {method} RPC code={code}"
                        )
                    return response.get("result")

            _send(
                "initialize",
                {"clientInfo": {"name": "untether-migration", "version": "1.0"}},
                1,
            )
            _send("initialized", {})
            resumed = _send(
                "thread/resume",
                {
                    "threadId": native_id,
                    "cwd": cwd,
                    "model": model,
                    "modelProvider": provider,
                },
                2,
            )
            _assert_resume_echo(resumed, native_id, cwd, model, provider)
            read = _send(
                "thread/read", {"threadId": native_id, "includeTurns": True}, 3
            )
            _assert_read_echo(read, native_id, cwd)
            _assert_read_history(data, read)
            return read  # Narrow observation seam; child is closed before return.
        finally:
            with suppress(OSError, ValueError, AttributeError):
                if proc.stdin is not None:
                    proc.stdin.close()
            with suppress(OSError, subprocess.TimeoutExpired):
                proc.wait(timeout=2)
            if proc.poll() is None:
                with suppress(OSError):
                    proc.terminate()
                with suppress(OSError, subprocess.TimeoutExpired):
                    proc.wait(timeout=2)
            shutil.rmtree(scratch, ignore_errors=True)

    @staticmethod
    def _reread_journal(journal_path: Path) -> bytes:
        """Independently re-read the restored rollout for proof verification."""
        try:
            return journal_path.read_bytes()
        except OSError as exc:
            raise CapabilityUnavailable("RESTORE_PROBE_FAILED") from exc

    # -- metadata sidecar -------------------------------------------------

    def _metadata_path(self, native_id: str) -> Path:
        directory = self.sessions_root / ".untether-migration"
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            raise CapabilityUnavailable("SESSION_METADATA_UNSAFE")
        if (
            directory.exists()
            and os.name == "posix"
            and directory.stat().st_mode & 0o077
        ):
            raise CapabilityUnavailable("SESSION_METADATA_UNSAFE")
        path = directory / f"{native_id}.json"
        if path.is_symlink():
            raise CapabilityUnavailable("SESSION_METADATA_UNSAFE")
        return path

    def _write_metadata(
        self, metadata: _CodexRolloutMetadata, *, replace_existing: bool = False
    ) -> None:
        """Create the sidecar O_EXCL, or atomically replace it for reclaim
        retry (the pending coordinator fence owns admission during the gap)."""
        path = self._metadata_path(metadata.native_id)
        path.parent.mkdir(mode=0o700, exist_ok=True)
        path = self._metadata_path(metadata.native_id)  # re-check after mkdir
        payload = metadata.model_dump_json().encode()
        if replace_existing and path.exists():
            try:
                fd, tmp_name = tempfile.mkstemp(
                    dir=path.parent, prefix=".reclaim-", suffix=".tmp"
                )
                with os.fdopen(fd, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(tmp_name, path)
                return
            except OSError as exc:
                Path(tmp_name).unlink(missing_ok=True)
                raise CapabilityUnavailable("SESSION_METADATA_WRITE_FAILED") from exc
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = None
        try:
            fd = os.open(path, flags, 0o600)
            with os.fdopen(fd, "wb", closefd=True) as stream:
                fd = None
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError as exc:
            if fd is not None:
                os.close(fd)
            raise CapabilityUnavailable("SESSION_METADATA_EXISTS") from exc
        except OSError as exc:
            if fd is not None:
                os.close(fd)
            raise CapabilityUnavailable("SESSION_METADATA_WRITE_FAILED") from exc

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
        """Return store-local identity only, never proof of project membership."""
        self._lookup_rollout(session_id)
        return session_id

    def lookup_for_project(self, session_id: str, project_root: Path) -> str:
        """Prove exact header cwd identity only; never export or gate."""
        from .file_bundle_adapter import _project_path_key

        try:
            key = _project_path_key(str(project_root))
            if key is None:
                raise CapabilityUnavailable("SESSION_PROJECT_REFUSED")
            _path, header = self._lookup_rollout(session_id)
            if _project_path_key(header["payload"].get("cwd")) != key:
                raise CapabilityUnavailable("SESSION_PROJECT_REFUSED")
        except (CapabilityUnavailable, OSError, ValueError):
            raise CapabilityUnavailable("SESSION_PROJECT_REFUSED") from None
        return session_id

    def _lookup_rollout(self, session_id: str) -> tuple[Path, dict]:
        """Bounded date-tree scan proving exactly one rollout header matches.

        Only ``sessions/YYYY/MM/DD/*.jsonl`` is descended; identity matches
        are counted across the whole scan before cwd authorization, mirroring
        the file bundle adapter's duplicate policy.
        """
        if (
            not session_id
            or len(session_id) > 128
            or Path(session_id).name != session_id
            or "/" in session_id
            or "\\" in session_id
        ):
            raise CapabilityUnavailable("NATIVE_ID_INVALID")
        matches: list[tuple[Path, dict]] = []
        try:
            for year in _digit_dirs(self.sessions_root, _YEAR_RE, _MAX_SCAN_ENTRIES):
                for month in _digit_dirs(year, _DAY_RE, _MAX_SCAN_ENTRIES):
                    for day in _digit_dirs(month, _DAY_RE, _MAX_SCAN_ENTRIES):
                        with os.scandir(day) as entries:
                            for count, entry in enumerate(entries, 1):
                                if count > _MAX_FILES_PER_DAY:
                                    raise CapabilityUnavailable("SESSION_LOOKUP_LIMIT")
                                path = Path(entry.path)
                                if entry.is_symlink() or not entry.is_file():
                                    raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
                                if path.suffix != ".jsonl":
                                    continue
                                header = self._read_rollout_header(path)
                                if header is not None and _header_matches_session(
                                    header, session_id
                                ):
                                    matches.append((path, header))
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_LOOKUP_FAILED") from exc
        if len(matches) != 1:
            raise CapabilityUnavailable(
                "SESSION_LOOKUP_AMBIGUOUS" if matches else "SESSION_NOT_FOUND"
            )
        return matches[0]

    def _read_rollout_header(self, path: Path) -> dict | None:
        """Strict line-1 header of one rollout; ``None`` for foreign files."""
        with path.open("rb") as stream:
            first = stream.readline(_MAX_HEADER_BYTES + 1)
        if len(first) > _MAX_HEADER_BYTES:
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        return _header_from_journal(first, strict_id=None)

    def _read_rollout_bytes(self, path: Path) -> bytes:
        if path.is_symlink() or not path.is_file():
            raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_RECORD_UNREADABLE") from exc
        if not data or len(data) > _MAX_JOURNAL_BYTES:
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        return data

    def _stored_generation(self, session_id: str, lineage: str) -> int:
        """Durable generation from the sidecar metadata; lineage must match."""
        path = self._metadata_path(session_id)
        if not path.is_file():
            return 0
        try:
            with path.open("rb") as stream:
                metadata = _decode_metadata(
                    stream.read(_MAX_METADATA_BYTES + 1), session_id
                )
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_METADATA_UNREADABLE") from exc
        if metadata.lineage != lineage:
            raise CapabilityUnavailable("SESSION_METADATA_MALFORMED")
        return metadata.generation

    def _export_snapshot(self, session_id: str) -> NativeArchive:
        rollout_path, _header = self._lookup_rollout(session_id)
        data = self._read_rollout_bytes(rollout_path)
        # Re-prove line-1 identity against the exact bytes being exported: a
        # store mutated between the scan and this read must refuse, not ship
        # mismatched bytes under a trusted lineage.
        _header_from_journal(data, strict_id=session_id)
        relative = rollout_path.relative_to(self.sessions_root)
        lineage = _LINEAGE_PREFIX + relative.as_posix()
        generation = self._stored_generation(session_id, lineage)
        metadata = _metadata_for(session_id, generation, lineage)
        records = {
            "session.jsonl": data,
            _METADATA_RECORD: metadata.model_dump_json().encode(),
        }
        return NativeArchive(
            engine=self.engine,
            logical_id=session_id,
            native_id=session_id,
            generation=generation,
            lineage=lineage,
            records=records,
            digest=hashlib.sha256(
                b"".join(records[name] for name in sorted(records))
            ).hexdigest(),
        )

    # -- import -----------------------------------------------------------

    def _check_archive_identity(self, archive: NativeArchive) -> None:
        if archive.engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        if archive.logical_id != archive.native_id or not archive.records:
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        if not all(_safe_name(name) for name in archive.records):
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if "session.jsonl" not in archive.records or set(archive.records) - {
            "session.jsonl",
            _METADATA_RECORD,
        }:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if _METADATA_RECORD in archive.records:
            metadata = _decode_metadata(
                archive.records[_METADATA_RECORD], archive.native_id
            )
            if (
                metadata.generation != archive.generation
                or metadata.lineage != archive.lineage
            ):
                raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")

    def _restore_rollout(self, journal_path: Path, data: bytes) -> Path:
        """Collision-safe create at the lineage-pinned path: O_EXCL, parents
        ``0o700``, content fsynced before the handle closes."""
        if journal_path.is_symlink() or journal_path.exists():
            raise CapabilityUnavailable("SESSION_ID_COLLISION")
        journal_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = None
        try:
            fd = os.open(journal_path, flags, 0o600)
            with os.fdopen(fd, "wb", closefd=True) as out:
                fd = None
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
        except FileExistsError as exc:
            if fd is not None:
                os.close(fd)
            raise CapabilityUnavailable("SESSION_ID_COLLISION") from exc
        except OSError as exc:
            if fd is not None:
                os.close(fd)
            raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
        return journal_path

    def import_exact(self, archive: NativeArchive) -> RestoreProof:
        self._check_archive_identity(archive)
        metadata = _metadata_for(archive.native_id, archive.generation, archive.lineage)
        journal_name = _journal_name_from_lineage(archive.lineage)
        data = archive.records["session.jsonl"]
        if len(data) > _MAX_JOURNAL_BYTES:
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        # The archive must claim this identity in the rollout header itself,
        # not only in the metadata record, before any byte lands on disk.
        _header_from_journal(data, strict_id=archive.native_id)
        journal_path = self.sessions_root / journal_name
        journal_path = self._restore_rollout(journal_path, data)
        # Metadata only after the content restore succeeded; a crash before
        # this point leaves the rollout without imported identity, and the
        # next import_exact still collides instead of silently overwriting.
        metadata_owned = False
        try:
            self._write_metadata(metadata)
            metadata_owned = True
            # 1) re-read the rollout bytes we restored
            if self._reread_journal(journal_path) != data:
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
            # 2) the app-server resume probe against the declared store
            self._run_resume_probe(archive.native_id, journal_path, data)
        except (CapabilityUnavailable, OSError):
            # Failed proof removes only this import's rollout and metadata.
            if metadata_owned:
                with suppress(CapabilityUnavailable, OSError):
                    self._metadata_path(archive.native_id).unlink(missing_ok=True)
            with suppress(OSError):
                journal_path.unlink(missing_ok=True)
            raise
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

    # -- bounded reclaim support ------------------------------------------

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
        """Fenced reclaim overwrite, bounded to the records this archive owns.

        The rollout at the lineage-pinned path is replaced (atomic fsynced
        temp plus ``os.replace``) only when the CURRENT stored bytes equal
        ``expected_prior_records["session.jsonl"]`` — or already equal the
        target bytes (idempotent retry after a partial attempt). Metadata is
        upserted after content. Reread plus resume probe follow; late
        failures refuse without rollback: the pending coordinator fence keeps
        admission closed.
        """
        if engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        if not all(_safe_name(name) for name in records):
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if set(records) != {"session.jsonl", _METADATA_RECORD}:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if "session.jsonl" not in expected_prior_records:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        metadata = _decode_metadata(records[_METADATA_RECORD], native_id)
        journal_name = _journal_name_from_lineage(lineage)
        if (
            metadata.generation != generation
            or metadata.lineage != lineage
            or lineage != _LINEAGE_PREFIX + journal_name
        ):
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        data = records["session.jsonl"]
        if len(data) > _MAX_JOURNAL_BYTES:
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        _header_from_journal(data, strict_id=native_id)
        journal_path = self.sessions_root / journal_name
        try:
            current = journal_path.read_bytes()
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_RECORD_UNSAFE") from exc
        # Exact prior pin, or the target bytes already stored (idempotent
        # retry) — where "already stored" tolerates the engine's own appended
        # events after a probe: a resume only ever appends.
        prior = expected_prior_records["session.jsonl"]
        if current != prior and not current.startswith(data):
            raise CapabilityUnavailable("OVERWRITE_PRIOR_MISMATCH")
        try:
            fd, tmp_name = tempfile.mkstemp(
                dir=journal_path.parent, prefix=".reclaim-", suffix=".tmp"
            )
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_name, journal_path)
        except OSError as exc:
            Path(tmp_name).unlink(missing_ok=True)
            raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
        self._write_metadata(metadata, replace_existing=True)
        # 1) re-read; 2) resume probe with prefix check. Late failures refuse
        # without rollback: the pending coordinator fence keeps admission shut.
        if self._reread_journal(journal_path) != data:
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        self._run_resume_probe(native_id, journal_path, data)
        return {
            name: hashlib.sha256(blob).hexdigest() for name, blob in records.items()
        }
