"""Concrete persistence adapter for OMP 18.1.6 stores (declared ``omp-v1``).

Counterpart to :mod:`omp_bundle` and :mod:`omp_restore`. Export collects the
exact journal, its sibling artifacts and verified referenced blobs via
``collect_omp_bundle``; import restores via ``restore_omp_records`` and proves
the restore with the declared operator restart probe. Generation/lineage
identity is durably preserved in a separate validated metadata record
(``untether.json``) carried inside the archive — never inside the mutable
journal — so evolved engine history cannot forge imported identity; the
metadata record is hash-verified like every other archive record.

Shared content-addressed blobs are reused, never deleted or overwritten:
reclaim overwrite is bounded to the journal, artifact and metadata records
this archive owns. A blob whose canonical file already exists with identical
bytes is reused (skipped); a blob file present under a digest with divergent
bytes refuses the reclaim as ``RECLAIM_BLOB_GAP`` — a bounded safe overwrite
does not fit and blob reclaim stays with the coordinator.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path, PurePath

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .native_adapter import (
    CapabilityUnavailable,
    RestoreProof,
    register_persistence_adapter,
    registered_adapter_engines,
)
from .native_archive import FidelityError, NativeArchive, _safe_name
from .omp_bundle import _checked, _journal, collect_omp_bundle
from .omp_restore import restore_omp_records
from .probe_process import run_probe
from .settings import OmpPersistenceProfile, SessionMigrationSettings

_METADATA_RECORD = "untether.json"
_BLOBS_KEY = "blobs/"
_LINEAGE_PREFIX = "omp-v1:"
_MAX_METADATA_BYTES = 4096
_HEX = set("0123456789abcdef")
# OS process essentials only: the probe child gets a disposable environment
# assembled under a scratch root, never operator credentials or profiles.
_PROBE_ENV_KEYS = frozenset(
    {"SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "PATH", "PATHEXT", "COMSPEC"}
)
# Bounds what a compromised probe can make the adapter parse or buffer.
_MAX_PROBE_EVIDENCE_BYTES = 4096
# Shipped probe verifies native RPC identity/history in an isolated process.
# Deadline and output limits fail closed; tests/ is not installed package data.
_OMP_RPC_PROBE_SOURCE = """import { spawn } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import assert from "node:assert/strict";

const [, packageRoot, root, sessionFile, evidenceFile, envJson] =
  process.argv.slice(2);
const metadata = JSON.parse(
  readFileSync(path.join(packageRoot, "package.json"), "utf8"),
);
assert.equal(metadata.name, "@oh-my-pi/pi-coding-agent");
assert.equal(metadata.version, "18.1.6");
const lines = readFileSync(sessionFile, "utf8").split("\\n");
const header = JSON.parse(lines[1]);
assert.equal(header.type, "session");
const historical = lines
  .slice(2)
  .filter((line) => line.trim())
  .map((line) => JSON.parse(line))
  .filter((entry) => entry.type === "message")
  .map((entry) => entry.message);
const child = spawn(
  process.execPath,
  [
    path.join(packageRoot, "dist", "cli.js"),
    "--model", "openai/gpt-4o-mini",
    "--mode", "rpc",
    "--session-dir", path.dirname(sessionFile),
    "--session", sessionFile,
    "--no-extensions", "--no-skills", "--no-rules", "--no-title",
    "--no-lsp", "--no-pty", "--no-prewalk", "--no-tools",
  ],
  {
    cwd: root,
    env: JSON.parse(readFileSync(envJson, "utf8")),
    stdio: ["pipe", "pipe", "pipe"],
  },
);
let stdout = "";
let stderr = "";
let exited = false;
const pending = new Map();
child.stdout.on("data", (data) => {
  stdout += data;
  if (Buffer.byteLength(stdout, "utf8") > 67108864) {
    child.kill("SIGKILL");
    process.exit(1);
  }
  for (;;) {
    const newline = stdout.indexOf("\\n");
    if (newline === -1) break;
    const line = stdout.slice(0, newline);
    stdout = stdout.slice(newline + 1);
    if (!line.trim()) continue;
    let frame;
    try { frame = JSON.parse(line); } catch { continue; }
    const kind =
      frame.type === "ready" ? "ready" :
      frame.type === "response" ? frame.id : undefined;
    if (kind === undefined) continue;
    const waiter = pending.get(kind);
    if (waiter) {
      pending.delete(kind);
      waiter(frame);
    }
  }
});
child.stderr.on("data", (data) => { stderr = (stderr + data).slice(-4000); });
child.on("error", (error) => {
  console.error("probe refused: spawn " + error);
  process.exit(1);
});
child.on("exit", () => { exited = true; });
const kill = () => { try { child.kill("SIGKILL"); } catch { /* gone */ } };
const timer = setTimeout(() => {
  kill();
  console.error("probe refused: deadline exceeded; stderr=" + stderr);
  process.exit(1);
}, 20000);
// One pending waiter per correlated frame; the request is issued once and
// the response must arrive within the deadline or the child dies hard.
const awaitFrame = (kind) =>
  new Promise((resolve, reject) => {
    const deadline = Date.now() + 15000;
    if (kind !== "ready") {
      child.stdin.write(JSON.stringify({ id: kind, type: kind }) + "\\n");
    }
    const check = () => {
      if (!pending.has(kind)) return;
      if (Date.now() > deadline || exited) {
        pending.delete(kind);
        reject(new Error(kind + " missing; stderr=" + stderr));
        return;
      }
      setTimeout(check, 50);
    };
    pending.set(kind, (frame) => {
      try {
        if (kind === "ready") {
          assert.equal(frame.type, "ready");
          assert.equal(frame.protocolVersion, 1);
        } else {
          assert.equal(frame.type, "response");
          assert.equal(frame.id, kind);
          assert.ok(frame.success, kind + " refused");
        }
        resolve(frame.data ?? frame);
      } catch (error) {
        reject(error);
      }
    });
    setTimeout(check, 50);
  });
try {
  await awaitFrame("ready");
  const state = await awaitFrame("get_state");
  assert.equal(state.sessionId, header.id);
  assert.equal(path.resolve(state.sessionFile), path.resolve(sessionFile));
  const { messages } = await awaitFrame("get_messages");
  assert.equal(messages.length, historical.length);
  messages.forEach((message, index) => {
    assert.deepStrictEqual(message, historical[index]);
  });
  child.stdin.end();
  const code = await new Promise((resolve) => child.once("exit", resolve));
  if (code !== 0) {
    console.error("probe refused: rpc exit " + code + "; stderr=" + stderr);
    process.exit(1);
  }
  writeFileSync(evidenceFile, JSON.stringify({
    sessionId: state.sessionId,
    sessionFile: state.sessionFile,
    messages: messages.length,
  }));
  clearTimeout(timer);
  process.exit(0);
} catch (error) {
  clearTimeout(timer);
  kill();
  console.error("probe refused: " + error + "; stderr=" + stderr);
  process.exit(1);
}
"""


class _OmpMigrationMetadata(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    logical_id: str = Field(min_length=1, max_length=128)
    native_id: str = Field(min_length=1, max_length=128)
    generation: int = Field(ge=0)
    lineage: str = Field(min_length=1, max_length=2048)


def _metadata_for(
    native_id: str, generation: int, lineage: str
) -> _OmpMigrationMetadata:
    """Validate identity into the metadata record shape, fail closed."""
    try:
        metadata = _OmpMigrationMetadata.model_validate(
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


def _decode_metadata(data: bytes, native_id: str) -> _OmpMigrationMetadata:
    try:
        if len(data) > _MAX_METADATA_BYTES:
            raise ValueError("metadata too large")
        metadata = _OmpMigrationMetadata.model_validate_json(data)
        if metadata.logical_id != native_id or metadata.native_id != native_id:
            raise ValueError("metadata identity mismatch")
        if not metadata.lineage.strip():
            raise ValueError("empty lineage")
        return metadata
    except (ValueError, ValidationError) as exc:
        raise CapabilityUnavailable("SESSION_METADATA_MALFORMED") from exc


def _journal_name_from_lineage(lineage: str) -> str:
    if not lineage.startswith(_LINEAGE_PREFIX):
        raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
    return lineage[len(_LINEAGE_PREFIX) :]


@dataclass(frozen=True, slots=True)
class OmpPersistenceAdapter:
    """OMP persistence: collect -> archive -> restore -> restart proof.

    ``restart_runner(argv)`` / ``run_argv(argv, timeout)`` mirror the file
    bundle adapter's injection seams so tests prove argv scoping without
    spawning processes. Unaided (no injection), the production path launches
    the native RPC probe declared by ``restart_probe_argv`` — a fresh engine
    process that must answer ``get_state``/``get_messages`` for the restored
    store — and refuses fail-closed when the profile configures none.
    """

    engine: str
    profile: OmpPersistenceProfile
    sessions_dir: Path = field(init=False)
    blobs_dir: Path = field(init=False)
    restart_runner: Callable[[tuple[str, ...]], None] | None = None
    run_argv: Callable[[list[str], float], None] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sessions_dir", Path(self.profile.sessions_dir))
        object.__setattr__(self, "blobs_dir", Path(self.profile.blobs_dir))
        _validate_omp_store(self.sessions_dir, self.blobs_dir)

    # -- helpers ---------------------------------------------------------

    def _collect(self, native_id: str) -> tuple[str, dict[str, bytes]]:
        try:
            bundle = collect_omp_bundle(
                sessions_dir=self.sessions_dir,
                blobs_dir=self.blobs_dir,
                native_id=native_id,
                # One bounded expanded ceiling per session tree, derived from
                # the declared per-file limit; discovery counts stay explicit.
                max_expanded=self.profile.max_file_bytes * 32,
                max_file_bytes=self.profile.max_file_bytes,
                max_files=self.profile.max_files,
            )
        except FidelityError as exc:
            raise CapabilityUnavailable(str(exc)) from None
        except OSError as exc:
            raise CapabilityUnavailable("OMP_FILESYSTEM_INVALID") from exc
        return bundle.journal_name, bundle.records

    def _scoped_probe_argv(self, native_id: str, journal_path: Path) -> list[str]:
        # Scoped argv built and validated before any runner is invoked; an
        # unscoped probe is refused exactly like the file bundle adapter's.
        template = list(self.profile.restart_argv)
        if not any(
            "{session_id}" in arg or "{session_path}" in arg for arg in template
        ):
            raise CapabilityUnavailable("SESSION_PROBE_UNSCOPED")
        replacements = {
            "{session_id}": native_id,
            "{session_path}": str(journal_path),
            "{sessions_dir}": str(self.sessions_dir),
            "{blobs_dir}": str(self.blobs_dir),
        }
        argv = []
        for arg in template:
            for key, value in replacements.items():
                arg = arg.replace(key, value)
            argv.append(arg)
        return argv

    def _omp_rpc_probe_argv(self, journal_path: Path) -> tuple[list[str], Path]:
        """Assemble the production RPC probe argv plus its disposable scratch
        root (evidence file and allowlisted environment payload).

        Template contract, fixed order: launcher token(s), ``{probe}`` (the
        embedded protocol script path, written into ``scratch``), ``rpc``,
        the installed package root and the probe working directory; the
        adapter appends the restored journal, evidence and environment paths.
        The engine child never sees operator credentials: ``scratch``
        isolates HOME/USERPROFILE, XDG_*, APPDATA/LOCALAPPDATA, TEMP/TMP,
        agent and config dirs. A template without ``{probe}`` refuses
        fail-closed.
        """
        template = list(self.profile.restart_probe_argv)
        if not any("{probe}" in arg for arg in template):
            raise CapabilityUnavailable("RESTART_PROBE_NOT_CONFIGURED")
        scratch = Path(tempfile.mkdtemp(prefix="untether-omp-probe-"))
        probe_path = scratch / "omp_rpc_probe.ts"
        try:
            probe_path.write_text(_OMP_RPC_PROBE_SOURCE, encoding="utf-8")
            env = {
                key: value
                for key, value in os.environ.items()
                if key.upper() in _PROBE_ENV_KEYS
            }
            home = scratch / "home"
            home.mkdir(parents=True)
            env["HOME"] = str(home)
            env["USERPROFILE"] = str(home)
            agent = scratch / "agent"
            agent.mkdir()
            env["PI_CODING_AGENT_DIR"] = str(agent)
            # Relative by contract: OMP joins PI_CONFIG_DIR onto the isolated
            # HOME, so an absolute value would produce an invalid host path.
            env["PI_CONFIG_DIR"] = ".omp"
            for key in (
                "XDG_CONFIG_HOME",
                "XDG_DATA_HOME",
                "XDG_STATE_HOME",
                "XDG_CACHE_HOME",
                "APPDATA",
                "LOCALAPPDATA",
                "TEMP",
                "TMP",
            ):
                target = scratch / key.lower()
                target.mkdir()
                env[key] = str(target)
            env["OMP_PROFILE"] = ""
            env["PI_PROFILE"] = ""
            env["PI_OFFLINE"] = "1"
            env_path = scratch / "probe-env.json"
            env_path.write_bytes(json.dumps(env).encode())
            evidence_path = scratch / "probe-evidence.json"
            replacements = {
                "{probe}": str(probe_path),
                "{sessions_dir}": str(self.sessions_dir),
                "{blobs_dir}": str(self.blobs_dir),
                "{session_path}": str(journal_path),
            }
            argv = []
            for arg in template:
                for key, value in replacements.items():
                    arg = arg.replace(key, value)
                argv.append(arg)
            argv += [str(journal_path), str(evidence_path), str(env_path)]
        except OSError as exc:
            shutil.rmtree(scratch, ignore_errors=True)
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        return argv, scratch

    @staticmethod
    def _verify_probe_evidence(
        evidence_path: Path, native_id: str, journal_path: Path
    ) -> None:
        """Certify the probe's evidence record, fail-closed.

        Exit 0 alone is not proof: the record must be a bounded JSON object
        naming this session and this restored journal with a nonnegative
        message count. Any malformed, oversized, foreign or missing record
        refuses with ``RESTART_PROBE_FAILED``.
        """
        try:
            with evidence_path.open("rb") as handle:
                raw = handle.read(_MAX_PROBE_EVIDENCE_BYTES + 1)
        except OSError as exc:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        if len(raw) > _MAX_PROBE_EVIDENCE_BYTES:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED")
        try:
            evidence = json.loads(raw)
            certified = (
                isinstance(evidence, dict)
                and evidence.get("sessionId") == native_id
                and Path(evidence["sessionFile"]).resolve() == journal_path.resolve()
                and isinstance(evidence.get("messages"), int)
                and not isinstance(evidence["messages"], bool)
                and evidence["messages"] >= 0
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        if not certified:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED")

    def _run_omp_rpc_probe(self, native_id: str, journal_path: Path) -> None:
        """Launch the RPC probe unaided; it exits 0 only when a fresh engine
        process answered get_state/get_messages for the restored journal and
        left its evidence record. The probe bounds its own child;
        ``restart_timeout_s`` bounds the probe process as the outer deadline.
        """
        argv, scratch = self._omp_rpc_probe_argv(journal_path)
        evidence_path = scratch / "probe-evidence.json"
        try:
            # Bounded launch with guaranteed group/tree teardown: any start
            # failure, nonzero exit or missed deadline refuses fail-closed.
            run_probe(argv, self.profile.restart_timeout_s)
            self._verify_probe_evidence(evidence_path, native_id, journal_path)
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    def _run_restart_probe(self, native_id: str, journal_path: Path) -> None:
        # The legacy scoped template is only a contract for injected runners;
        # the production RPC probe resolves its own argv and never requires it.
        if self.restart_runner is not None:
            self.restart_runner(tuple(self._scoped_probe_argv(native_id, journal_path)))
            return
        if self.run_argv is not None:
            self.run_argv(
                self._scoped_probe_argv(native_id, journal_path),
                self.profile.restart_timeout_s,
            )
            return
        if not self.profile.restart_probe_argv:
            # Fail closed: an unaided import must be proven by the engine's
            # own native RPC probe, never by an unverified exit code.
            raise CapabilityUnavailable("RESTART_PROBE_NOT_CONFIGURED")
        self._run_omp_rpc_probe(native_id, journal_path)

    @staticmethod
    def _reread_journal(journal_path: Path) -> bytes:
        """Independently re-read the restored journal for probe verification."""
        try:
            return journal_path.read_bytes()
        except OSError as exc:
            raise CapabilityUnavailable("RESTORE_PROBE_FAILED") from exc

    def _verified_restore_hashes(
        self, native_id: str, journal_name: str, records: dict[str, bytes]
    ) -> dict[str, str]:
        """Certify destination bytes after the probe, never the input mapping.

        The exact-restore contract permits no journal title/settings mutation.
        The collector validates the tree and referenced blobs; also read imported
        unreferenced blobs, which legitimately need not appear in that bundle.
        """
        try:
            collected_name, actual = self._collect(native_id)
            if collected_name != journal_name:
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
            for name, expected in records.items():
                if name == _METADATA_RECORD:
                    path = self._metadata_path(native_id)
                elif name.startswith(_BLOBS_KEY) and name not in actual:
                    digest = name[len(_BLOBS_KEY) :]
                    if len(digest) != 64 or any(c not in _HEX for c in digest):
                        raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
                    path = self.blobs_dir / digest
                else:
                    continue
                with _checked(path).open("rb") as stream:
                    actual[name] = stream.read(len(expected) + 1)
            if any(actual.get(name) != data for name, data in records.items()):
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        except (CapabilityUnavailable, FidelityError, OSError) as exc:
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED") from exc
        return {name: hashlib.sha256(actual[name]).hexdigest() for name in records}

    def _metadata_path(self, native_id: str) -> Path:
        directory = self.sessions_dir / ".untether-migration"
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
        self,
        metadata: _OmpMigrationMetadata,
        *,
        replace_existing: bool = False,
        payload: bytes | None = None,
    ) -> None:
        """Create the sidecar O_EXCL, or atomically replace it for reclaim
        retry (the pending coordinator fence owns admission during the gap)."""
        path = self._metadata_path(metadata.native_id)
        path.parent.mkdir(mode=0o700, exist_ok=True)
        path = self._metadata_path(metadata.native_id)  # re-check after mkdir
        if payload is None:
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
        self._lookup_journal(session_id)
        return session_id

    def lookup_for_project(self, session_id: str, project_root: PurePath) -> str:
        """Prove exact header cwd identity only; never export or gate."""
        from .file_bundle_adapter import _project_path_key

        try:
            key = _project_path_key(str(project_root))
            if key is None:
                raise CapabilityUnavailable("SESSION_PROJECT_REFUSED")
            _path, header = self._lookup_journal(session_id)
            if _project_path_key(header.get("cwd")) != key:
                raise CapabilityUnavailable("SESSION_PROJECT_REFUSED")
        except (CapabilityUnavailable, OSError, ValueError):
            raise CapabilityUnavailable("SESSION_PROJECT_REFUSED") from None
        return session_id

    def _lookup_journal(self, session_id: str) -> tuple[Path, dict]:
        """Single bounded scan proving exactly one journal header matches.

        Identity matches are counted across the whole scan before cwd
        authorization, mirroring the file bundle adapter's duplicate policy.
        """
        from .file_bundle_adapter import _header_matches_session

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
            with os.scandir(self.sessions_dir) as children:
                for count, child in enumerate(children, 1):
                    if count > self.profile.max_files:
                        raise CapabilityUnavailable("SESSION_LOOKUP_LIMIT")
                    path = Path(child.path)
                    if child.is_dir(follow_symlinks=False):
                        continue  # sibling artifact trees, not root journals
                    if path.is_symlink() or not path.is_file():
                        raise CapabilityUnavailable("SESSION_RECORD_UNSAFE")
                    with path.open("rb") as stream:
                        first = stream.readline(65537)
                        if len(first) > 65536:
                            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
                        try:
                            header = json.loads(first)
                        except (ValueError, UnicodeError):
                            continue
                        if isinstance(header, dict) and header.get("type") == "title":
                            # Native OMP has one fixed 256-byte title slot before
                            # the header. Never search arbitrary history for cwd.
                            if len(first) != 256 or not first.endswith(b"\n"):
                                raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
                            second = stream.readline(65537)
                            if len(second) > 65536:
                                raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
                            try:
                                _journal(first + second)
                                header = json.loads(second)
                            except (FidelityError, ValueError, UnicodeError) as exc:
                                raise CapabilityUnavailable(
                                    "SESSION_RECORD_MALFORMED"
                                ) from exc
                    if _header_matches_session(header, session_id):
                        matches.append((path, header))
        except OSError as exc:
            raise CapabilityUnavailable("OMP_FILESYSTEM_INVALID") from exc
        if len(matches) != 1:
            raise CapabilityUnavailable(
                "SESSION_LOOKUP_AMBIGUOUS" if matches else "SESSION_NOT_FOUND"
            )
        return matches[0]

    def _stored_generation(self, session_id: str, lineage: str) -> int:
        """Durable generation from the sidecar metadata; lineage must match."""
        path = self._metadata_path(session_id)
        if not path.is_file():
            return 0
        try:
            metadata = _decode_metadata(
                path.open("rb").read(_MAX_METADATA_BYTES + 1), session_id
            )
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_METADATA_UNREADABLE") from exc
        if metadata.lineage != lineage:
            raise CapabilityUnavailable("SESSION_METADATA_MALFORMED")
        return metadata.generation

    def _export_snapshot(self, session_id: str) -> NativeArchive:
        journal_path, _header = self._lookup_journal(session_id)
        journal_name, content = self._collect(session_id)
        if journal_path.name != journal_name:
            # The bounded header scan and the collector must agree on the
            # matched journal; divergence means the store moved mid-read.
            raise CapabilityUnavailable("SESSION_LOOKUP_AMBIGUOUS")
        lineage = _LINEAGE_PREFIX + journal_name
        generation = self._stored_generation(session_id, lineage)
        # Identity travels as its own validated record so mutable journal
        # bytes can never forge generation/lineage; it is hash-verified in
        # the manifest like every other record.
        metadata = _metadata_for(session_id, generation, lineage)
        records = {**content, _METADATA_RECORD: metadata.model_dump_json().encode()}
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

    def _check_archive_identity(self, archive: NativeArchive) -> None:
        if archive.engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        if archive.logical_id != archive.native_id or not archive.records:
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        if not all(_safe_name(name) for name in archive.records):
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if "session.jsonl" not in archive.records:
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

    def _restore_overwrite(self, content: dict[str, bytes], journal_name: str) -> Path:
        """Bounded atomic replace of the journal, artifacts and metadata-shape
        records this archive owns.

        Unlike :func:`restore_omp_records` (which refuses every existing
        target), reclaim replaces each owned non-blob record with a fully
        fsynced temp file plus ``os.replace``. Blobs never reach this path:
        identical shared blobs are pre-verified and reused, divergent ones
        refuse earlier, and absent ones are created O_EXCL here.
        """
        journal_path = self.sessions_dir / journal_name
        sibling_root = journal_path.with_suffix("")
        # Absent blobs are created O_EXCL here; present identical ones are
        # reused untouched (verified above), divergent ones never reach this
        # point.
        for name, data in sorted(content.items()):
            if not name.startswith(_BLOBS_KEY):
                continue
            digest = name[len(_BLOBS_KEY) :]
            target = self.blobs_dir / digest
            if target.exists():
                continue
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = None
            try:
                fd = os.open(target, flags, 0o600)
                with os.fdopen(fd, "wb", closefd=True) as out:
                    fd = None
                    out.write(data)
                    out.flush()
                    os.fsync(out.fileno())
            except FileExistsError as exc:
                if fd is not None:
                    os.close(fd)
                # Lost a create race against an identical blob: verify and reuse.
                if target.read_bytes() != data:
                    raise CapabilityUnavailable("RECLAIM_BLOB_GAP") from exc
            except OSError as exc:
                if fd is not None:
                    os.close(fd)
                raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
        owned: list[tuple[Path, bytes]] = []
        for name, data in sorted(content.items()):
            if name.startswith(_BLOBS_KEY):
                continue
            if name == "session.jsonl":
                target = journal_path
            else:
                from .omp_restore import _safe_relative

                if not name.startswith("artifacts/"):
                    raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
                relative = _safe_relative(name[len("artifacts/") :])
                target = sibling_root / Path(*relative.parts)
            owned.append((target, data))
        for target, data in owned:
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                fd, tmp_name = tempfile.mkstemp(
                    dir=target.parent,
                    prefix=".reclaim-",
                    suffix=".tmp",
                )
            except OSError as exc:
                raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
            try:
                with os.fdopen(fd, "wb") as out:
                    out.write(data)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(tmp_name, target)
            except OSError as exc:
                Path(tmp_name).unlink(missing_ok=True)
                raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc
        return journal_path

    def _restore(self, content: dict[str, bytes], journal_name: str) -> Path:
        try:
            return restore_omp_records(
                records=content,
                sessions_dir=self.sessions_dir,
                blobs_dir=self.blobs_dir,
                journal_name=journal_name,
            )
        except FileExistsError as exc:
            raise CapabilityUnavailable("SESSION_ID_COLLISION") from exc
        except ValueError as exc:
            raise CapabilityUnavailable(str(exc)) from None
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_RECORD_WRITE_FAILED") from exc

    def import_exact(self, archive: NativeArchive) -> RestoreProof:
        self._check_archive_identity(archive)
        metadata = _metadata_for(archive.native_id, archive.generation, archive.lineage)
        content = {
            name: data
            for name, data in archive.records.items()
            if name != _METADATA_RECORD
        }
        journal_path = self._restore(
            content, _journal_name_from_lineage(archive.lineage)
        )
        # Metadata only after the content restore succeeded; a crash before
        # this point leaves the journal without imported identity, and the
        # next import_exact still collides instead of silently overwriting.
        metadata_owned = False
        try:
            self._write_metadata(
                metadata, payload=archive.records.get(_METADATA_RECORD)
            )
            metadata_owned = True
            # 1) re-read the journal bytes we restored
            if self._reread_journal(journal_path) != content["session.jsonl"]:
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
            # 2) the operator's scoped restart probe against the declared store
            self._run_restart_probe(archive.native_id, journal_path)
            hashes = self._verified_restore_hashes(
                archive.native_id, journal_path.name, archive.records
            )
        except (CapabilityUnavailable, OSError):
            # Failed proof removes only this import's journal and metadata.
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
            record_hashes=hashes,
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
        """Fenced reclaim overwrite, bounded to records this archive owns.

        The journal, sibling artifacts and metadata record are replaced only
        when the CURRENT stored journal bytes equal ``expected_prior_records``
        (or the target bytes — idempotent retry after a partial attempt).
        Content-addressed blobs are NEVER deleted or overwritten: a blob
        already present in the shared store with identical bytes is reused;
        one present under a matching digest with divergent bytes refuses as
        ``RECLAIM_BLOB_GAP`` — the bounded safe overwrite does not fit and
        blob reclaim remains with the coordinator.
        """
        if engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        if not all(_safe_name(name) for name in records):
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if "session.jsonl" not in records or _METADATA_RECORD not in records:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        metadata = _decode_metadata(records[_METADATA_RECORD], native_id)
        journal_name = _journal_name_from_lineage(lineage)
        if (
            metadata.generation != generation
            or metadata.lineage != lineage
            or lineage != _LINEAGE_PREFIX + journal_name
        ):
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        content = {
            name: data for name, data in records.items() if name != _METADATA_RECORD
        }
        journal_path = self.sessions_dir / journal_name
        try:
            current = journal_path.read_bytes()
        except OSError as exc:
            raise CapabilityUnavailable("SESSION_RECORD_UNSAFE") from exc
        if current not in (
            expected_prior_records["session.jsonl"],
            content["session.jsonl"],
        ):
            raise CapabilityUnavailable("OVERWRITE_PRIOR_MISMATCH")
        # Blob policy first: reuse identical shared blobs, refuse divergent
        # ones before any local byte moves.
        for name, data in content.items():
            if not name.startswith(_BLOBS_KEY):
                continue
            digest = name[len(_BLOBS_KEY) :]
            if len(digest) != 64 or any(c not in _HEX for c in digest):
                raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
            target = self.blobs_dir / digest
            try:
                existing = target.read_bytes()
            except FileNotFoundError:
                continue  # absent: restore_omp_records creates it O_EXCL
            except OSError as exc:
                raise CapabilityUnavailable("OMP_FILESYSTEM_INVALID") from exc
            if existing != data:
                raise CapabilityUnavailable("RECLAIM_BLOB_GAP")
        restored = self._restore_overwrite(content, journal_name)
        self._write_metadata(
            metadata, replace_existing=True, payload=records[_METADATA_RECORD]
        )
        # 1) re-read; 2) scoped restart probe. Late failures refuse without
        # rollback: the pending coordinator fence keeps admission closed.
        if self._reread_journal(restored) != content["session.jsonl"]:
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        self._run_restart_probe(native_id, restored)
        return self._verified_restore_hashes(native_id, journal_name, records)


def _validate_omp_store(sessions_dir: Path, blobs_dir: Path) -> None:
    """Fail closed unless both declared roots are real, link-free directories."""
    for root in (sessions_dir, blobs_dir):
        if not root.is_dir() or root.is_symlink():
            raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY")
        try:
            resolved = root.resolve(strict=True)
        except OSError as exc:
            raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY") from exc
        if resolved != root:
            raise CapabilityUnavailable("PERSISTENCE_STORE_SYMLINK") from None
    if os.name == "posix" and (sessions_dir.stat().st_mode & 0o077):
        raise CapabilityUnavailable("PERSISTENCE_STORE_PERMISSIONS") from None


def register_from_settings(settings: SessionMigrationSettings, *, engine: str) -> None:
    """Route the declared ``omp-v1`` profile here; every other format keeps
    the file bundle adapter's routing untouched."""
    from .session_gate import configure_session_gate

    configure_session_gate(settings)
    profile = settings.persistence.engines.get(engine)
    if profile is None:
        return
    if not isinstance(profile, OmpPersistenceProfile):
        from .file_bundle_adapter import register_from_settings as register_file_bundle

        register_file_bundle(settings, engine=engine)
        return
    if engine in registered_adapter_engines():
        return
    register_persistence_adapter(
        engine,
        lambda: OmpPersistenceAdapter(engine=engine, profile=profile),
    )
