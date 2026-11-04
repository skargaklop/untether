"""Typed, fail-closed SSH migration configuration shared by both execution modes."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SAFE_HOST = re.compile(r"^(?=.{1,253}$)[a-zA-Z0-9][a-zA-Z0-9._-]*$")
_SAFE_NAME = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")
_SHA256 = re.compile(r"^SHA256:[A-Za-z0-9+/]{43}$")
_RECORD_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ENGINE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/].*")


class EnginePersistenceProfile(BaseModel):
    """Operator-declared, one-time persistence layout for exactly one engine.

    Nothing is inferred: every path, the record name, and the restart probe
    command are explicit operator configuration validated fail-closed.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    format: Literal["pi-jsonl-v3"]
    sessions_dir: str
    session_record: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    restart_argv: tuple[str, ...] = Field(min_length=1, max_length=16)
    restart_timeout_s: float = Field(default=120, gt=0, le=1800)

    @field_validator("sessions_dir")
    @classmethod
    def sessions_dir_safe(cls, value: str) -> str:
        # Local engine store: Windows and POSIX absolute paths are both valid
        # (the profile may run on either OS); symlinks are refused at adapter
        # construction, not at parse time.
        return _absolute_local_path(value)

    @field_validator("restart_argv")
    @classmethod
    def restart_argv_safe(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not v or any(c in v for c in "\x00\n\r") for v in values):
            raise ValueError("restart_argv entries must be non-empty argv tokens")
        return values


class ExecutablePersistenceProfile(BaseModel):
    """Operator-declared external adapter bound to one real executable.

    Paths must be literal absolute local paths; the adapter refuses to run
    against a missing executable or store — stores are never created implicitly.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    format: Literal["executable-v1"]
    executable: str
    store: str
    interpreter: str | None = None
    timeout: float = Field(default=30.0, gt=0, le=1800)

    @field_validator("executable", "store")
    @classmethod
    def paths_absolute(cls, value: str) -> str:
        return _absolute_local_path(value)

    @field_validator("interpreter")
    @classmethod
    def interpreter_absolute(cls, value: str | None) -> str | None:
        return None if value is None else _absolute_local_path(value)


class OmpPersistenceProfile(BaseModel):
    """Operator-declared OMP 18.1.6 store: journal tree plus canonical blob store.

    ``sessions_dir`` holds ``<journal>.jsonl`` files with sibling artifact
    directories; ``blobs_dir`` is the content-addressed blob store root
    (``blob:sha256:...`` refs resolve to ``<blobs_dir>/<digest>``).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    format: Literal["omp-v1"]
    sessions_dir: str
    blobs_dir: str
    restart_argv: tuple[str, ...] = Field(min_length=1, max_length=16)
    restart_timeout_s: float = Field(default=120, gt=0, le=1800)
    # Unaided (no injected runner) the adapter launches the native OMP RPC
    # restart probe from this launcher argv; empty means fail-closed refusal
    # (`RESTART_PROBE_NOT_CONFIGURED`). Fixed template order: launcher
    # token(s), `{probe}` (substituted with the bundled probe script), `rpc`,
    # installed package root, probe working directory; the adapter appends the
    # session file, evidence file and environment payload paths itself.
    restart_probe_argv: tuple[str, ...] = Field(default=(), max_length=8)
    max_file_bytes: int = Field(default=33554432, gt=0, le=1073741824)
    max_files: int = Field(default=10000, ge=1, le=1000000)

    @field_validator("sessions_dir", "blobs_dir")
    @classmethod
    def dirs_absolute(cls, value: str) -> str:
        return _absolute_local_path(value)

    @field_validator("restart_argv", "restart_probe_argv")
    @classmethod
    def restart_argv_safe(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not v or any(c in v for c in "\x00\n\r") for v in values):
            raise ValueError("restart_argv entries must be non-empty argv tokens")
        return values


class CodexRolloutProfile(BaseModel):
    """Operator-declared Codex rollout store (``codex-rollout-v1``).

    ``sessions_root`` is the ``sessions`` subtree of a disposable
    ``CODEX_HOME``; ``app_server_executable`` is the installed
    ``codex`` binary used for the resume/read restart proof.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    format: Literal["codex-rollout-v1"]
    sessions_root: str
    app_server_executable: str
    resume_timeout_s: float = Field(default=120, gt=0, le=600)
    resume_model: str | None = Field(default=None, max_length=128)
    resume_model_provider: str | None = Field(default=None, max_length=128)
    resume_approval_policy: str | None = Field(default=None, max_length=128)

    @field_validator("sessions_root", "app_server_executable")
    @classmethod
    def paths_absolute(cls, value: str) -> str:
        return _absolute_local_path(value)


class OpenCodeSyncProfile(BaseModel):
    """Operator-declared OpenCode sync store (``opencode-sync-v1``).

    ``data_root`` holds per-endpoint isolated XDG/HOME trees; the adapter
    serves the engine locally on ``hostname:port`` for sync/history export
    and sync/replay import.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    format: Literal["opencode-sync-v1"]
    executable: str
    data_root: str
    hostname: str = "127.0.0.1"
    port: int = Field(default=18790, ge=1024, le=65535)
    request_timeout_s: float = Field(default=30, gt=0, le=300)
    max_response_bytes: int = Field(default=67108864, gt=0, le=1073741824)
    serve_start_timeout_s: float = Field(default=30, gt=0, le=300)

    @field_validator("executable", "data_root")
    @classmethod
    def paths_absolute(cls, value: str) -> str:
        return _absolute_local_path(value)

    @field_validator("hostname")
    @classmethod
    def hostname_safe(cls, value: str) -> str:
        if not _SAFE_HOST.fullmatch(value):
            raise ValueError("invalid hostname")
        return value


EngineProfile = Annotated[
    EnginePersistenceProfile
    | ExecutablePersistenceProfile
    | OmpPersistenceProfile
    | CodexRolloutProfile
    | OpenCodeSyncProfile,
    Field(discriminator="format"),
]


class EnginePersistenceSettings(BaseModel):
    """The single persistence contract root: engines -> exact layout."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    engines: dict[str, EngineProfile] = Field(default_factory=dict)

    @field_validator("engines")
    @classmethod
    def engines_safe(
        cls, values: dict[str, EnginePersistenceProfile]
    ) -> dict[str, EnginePersistenceProfile]:
        for name in values:
            if not _ENGINE_ID.fullmatch(name):
                raise ValueError("engine keys must be lowercase engine ids")
        return values


def _absolute_safe_path(value: str) -> str:
    """Accept a literal POSIX path (remote hosts), never a shell expression."""
    if not value.startswith("/") or any(c in value for c in "\\'\"\x00\n\r`;$|&<>*?{}"):
        raise ValueError("expected an absolute, literal POSIX path")
    if any(p in ("", ".", "..") for p in value.split("/")[1:]):
        raise ValueError("empty or traversal path component")
    if not PurePosixPath(value).is_absolute():
        raise ValueError("absolute path required")
    return value


def _absolute_local_path(value: str) -> str:
    """Accept a literal absolute local path — Windows or POSIX — never traversal.

    Deterministic across host OSes: a Windows ``C:\\...`` or UNC path and a
    POSIX ``/...`` path are both accepted; relative paths, drive-relative
    paths, and any ``.``/``..`` component are refused.
    """
    if not value or any(c in value for c in "'\"\x00\n\r`;$|&<>*?{}"):
        raise ValueError("expected a literal absolute path")
    windows_absolute = _WINDOWS_ABSOLUTE.fullmatch(value) or value.startswith("\\\\")
    if not (value.startswith("/") or windows_absolute):
        raise ValueError("absolute path required (Windows or POSIX)")
    parts = [p for p in re.split(r"[\\/]", value) if p]
    if not parts or any(p in (".", "..") for p in parts):
        raise ValueError("empty or traversal path component")
    return value


class MigrationPeer(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    host: str
    port: int = Field(default=22, ge=1, le=65535)
    user: str
    identity_dir: str
    identity_file: str
    known_hosts_file: str
    host_key_fingerprint: str
    receiver_path: str
    execution_mode: Literal["native", "container-wrapper"] = "native"
    expected_endpoint_id: str
    destination_binding: str
    projects: dict[str, str] = Field(min_length=1)

    @field_validator("host")
    @classmethod
    def host_safe(cls, value: str) -> str:
        if not _SAFE_HOST.fullmatch(value) or value.startswith("-"):
            raise ValueError("host must be a literal DNS name or IPv4 address")
        return value

    @field_validator("user", "expected_endpoint_id", "destination_binding")
    @classmethod
    def identifier_safe(cls, value: str) -> str:
        if not _SAFE_NAME.fullmatch(value):
            raise ValueError("invalid peer identifier")
        return value

    @field_validator("identity_file")
    @classmethod
    def identity_safe(cls, value: str) -> str:
        if not _SAFE_NAME.fullmatch(value) or value in (".", ".."):
            raise ValueError("identity_file must be an explicit file basename")
        return value

    @field_validator("host_key_fingerprint")
    @classmethod
    def fingerprint_safe(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("an out-of-band verified SHA256 host key pin is required")
        return value

    @field_validator("receiver_path")
    @classmethod
    def receiver_safe(cls, value: str) -> str:
        return _absolute_safe_path(value)

    @field_validator("identity_dir", "known_hosts_file")
    @classmethod
    def local_path_safe(cls, value: str) -> str:
        if not value or any(c in value for c in "\x00\n\r"):
            raise ValueError("invalid local path")
        return value

    @field_validator("projects")
    @classmethod
    def projects_safe(cls, values: dict[str, str]) -> dict[str, str]:
        for name, path in values.items():
            if not _SAFE_NAME.fullmatch(name):
                raise ValueError("invalid project alias")
            _absolute_safe_path(path)
        return values


class ReceiverSettings(BaseModel):
    """Fail-closed receiver deployment configuration."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    endpoint_id: str
    engine: str
    project: str
    state_dir: str
    destination_binding: str
    health_argv: tuple[str, ...] = Field(min_length=1)
    project_root: str | None = None
    authorized_peer: str | None = None

    @field_validator("authorized_peer")
    @classmethod
    def authorized_peer_safe(cls, value: str | None) -> str | None:
        if value is not None and not _SAFE_NAME.fullmatch(value):
            raise ValueError("invalid authorized peer identifier")
        return value

    execution_mode: Literal["native", "container-wrapper"] = "native"

    @field_validator("endpoint_id", "engine", "destination_binding")
    @classmethod
    def identifier_safe(cls, value: str) -> str:
        if not _SAFE_NAME.fullmatch(value):
            raise ValueError("invalid receiver identifier")
        return value

    @field_validator("project")
    @classmethod
    def project_safe(cls, value: str) -> str:
        if not _SAFE_NAME.fullmatch(value):
            raise ValueError("invalid project alias")
        return value

    @field_validator("state_dir")
    @classmethod
    def state_dir_safe(cls, value: str) -> str:
        return _absolute_local_path(value)

    @field_validator("project_root")
    @classmethod
    def project_root_safe(cls, value: str | None) -> str | None:
        return None if value is None else _absolute_local_path(value)

    @field_validator("health_argv")
    @classmethod
    def health_argv_safe(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not v or any(c in v for c in "\x00\n\r") for v in values):
            raise ValueError("health_argv entries must be non-empty argv tokens")
        return values


class SessionMigrationSettings(BaseModel):
    """Fail-closed settings; ``persistence`` gates adapter registration."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    enabled: bool = False
    gate_root: str | None = None

    @field_validator("gate_root")
    @classmethod
    def gate_root_safe(cls, value: str | None) -> str | None:
        return _absolute_local_path(value) if value is not None else None

    receiver: ReceiverSettings | None = None
    connect_timeout_s: float = Field(default=10, gt=0, le=120)
    probe_timeout_s: float = Field(default=30, gt=0, le=300)
    drain_timeout_s: float = Field(default=15, gt=0, le=300)
    transfer_timeout_s: float = Field(default=300, gt=0, le=3600)
    import_timeout_s: float = Field(default=180, gt=0, le=3600)
    reconcile_timeout_s: float = Field(default=30, gt=0, le=300)
    max_retries: int = Field(default=2, ge=0, le=5)
    retry_backoff_s: float = Field(default=2, ge=0, le=60)
    health_ttl_s: float = Field(default=15, gt=0, le=60)
    max_archive_bytes: int = Field(default=67108864, ge=1, le=1073741824)
    max_expanded_bytes: int = Field(default=134217728, ge=1, le=2147483648)
    min_free_bytes: int = Field(default=536870912, ge=1)
    staging_ttl_s: int = Field(default=86400, ge=60)
    peers: dict[str, MigrationPeer] = Field(default_factory=dict)
    persistence: EnginePersistenceSettings = Field(
        default_factory=EnginePersistenceSettings
    )

    @model_validator(mode="after")
    def check_peers(self) -> SessionMigrationSettings:
        for name in self.peers:
            if not _SAFE_NAME.fullmatch(name):
                raise ValueError("invalid peer name")
        if self.enabled and not self.peers:
            raise ValueError("enabled session migration requires a named peer")
        if self.max_expanded_bytes < self.max_archive_bytes:
            raise ValueError("expanded limit must be at least archive limit")
        return self
