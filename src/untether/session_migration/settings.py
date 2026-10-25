"""Typed, fail-closed SSH migration configuration shared by both execution modes."""
from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SAFE_HOST = re.compile(r"^(?=.{1,253}$)[a-zA-Z0-9][a-zA-Z0-9._-]*$")
_SAFE_NAME = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$")
_SHA256 = re.compile(r"^SHA256:[A-Za-z0-9+/]{43}$")


def _absolute_safe_path(value: str) -> str:
    """Accept a literal POSIX path, never an interpreter expression or traversal."""
    if not value.startswith("/") or any(c in value for c in "\\'\"\x00\n\r`;$|&<>*?{}"):
        raise ValueError("expected an absolute, literal POSIX path")
    if any(p in ("", ".", "..") for p in value.split("/")[1:]):
        raise ValueError("empty or traversal path component")
    if not PurePosixPath(value).is_absolute():
        raise ValueError("absolute path required")
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


class SessionMigrationSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    enabled: bool = False
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
