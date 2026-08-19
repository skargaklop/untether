"""Safe, startup-only discovery helpers for generic ACP agents."""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .acp_installations import InstalledLauncher, match_distribution
from .backends import EngineBackend
from .ids import is_valid_id
from .settings import NonEmptyStr
from .utils.json_state import atomic_write_json

OFFICIAL_REGISTRY_URL = (
    "https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json"
)

# Version of the top-level registry document we understand (the value of the
# top-level "version" key). Verified against the served registry document
# (https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json),
# which carries "1.0.0". Applied only when a document actually carries a
# top-level "version" key.
REGISTRY_DOC_VERSION = "1.0.0"

# Agent ids per get-started/registry.md: "Create a folder with your agent's ID
# (lowercase, hyphens allowed)". Lowercase alphanumerics with hyphens between
# segments — no leading/trailing/double hyphens.
_REGISTRY_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

_UNSCOPED_PACKAGE_RE = re.compile(
    r"^[a-z0-9][a-z0-9._-]*(?:(?:@|==)[0-9][a-z0-9._+-]*)?$"
)

_SCOPED_NPM_PACKAGE_RE = re.compile(
    r"^@[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*(?:@[0-9][a-z0-9._+-]*)?$"
)


class _RegistryDistributionModel(BaseModel):
    """Strict-but-open view of a single registry distribution entry."""

    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)

    target: NonEmptyStr
    type: NonEmptyStr
    cmd: NonEmptyStr
    args: list[NonEmptyStr] = Field(default_factory=list)
    env: dict[NonEmptyStr, NonEmptyStr] | None = None


class _RegistryAgentModel(BaseModel):
    """Strict-but-open view of a single registry agent entry."""

    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)

    id: NonEmptyStr
    version: str = ""
    distributions: list[_RegistryDistributionModel] = Field(default_factory=list)
    distribution: dict[str, Any] | None = None

    @field_validator("id")
    @classmethod
    def _validate_agent_id(cls, value: str) -> str:
        if not _REGISTRY_ID_RE.fullmatch(value):
            raise ValueError(f"invalid ACP registry agent id: {value!r}")
        return value


@dataclass(frozen=True, slots=True)
class RegistryDistribution:
    target: str
    type: str
    cmd: str
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = None
    package: str | None = None


@dataclass(frozen=True, slots=True)
class RegistryAgent:
    id: str
    version: str
    distributions: tuple[RegistryDistribution, ...]


@dataclass(frozen=True, slots=True)
class InstallationRecord:
    agent_id: str
    version: str
    target: str
    cmd: str
    checked_at: float
    installed: bool
    executable: str | None = None


def normalise_registry_id(value: str) -> str:
    result = value.replace("-", "_")
    if not is_valid_id(result):
        raise ValueError(f"invalid ACP registry id: {value!r}")
    return result


def choose_binary_distribution(
    agent: RegistryAgent, *, target: str
) -> RegistryDistribution | None:
    for distribution in agent.distributions:
        if distribution.target == target and distribution.type == "binary":
            return distribution
    return next(
        (
            distribution
            for distribution in agent.distributions
            if distribution.type in {"npx", "uvx"}
        ),
        None,
    )


def _now() -> float:
    return time.time()


def fresh_cache(path: Path, *, ttl_days: int) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        fetched_at = float(payload["fetched_at"])
        if _now() - fetched_at <= ttl_days * 86400:
            return payload
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    return None


def read_stale_cache(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return (
            payload if isinstance(payload, dict) and "fetched_at" in payload else None
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def write_cache(path: Path, value: Any) -> None:
    atomic_write_json(path, {"fetched_at": _now(), "value": value})


def load_installation_cache(path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    records = payload.get("records", payload) if isinstance(payload, dict) else {}
    return records if isinstance(records, dict) else {}


def write_installation_cache(path: Path, records: dict[str, dict[str, Any]]) -> None:
    atomic_write_json(path, {"records": records})


def current_platform_target() -> str:
    system = sys.platform
    os_name = (
        "windows" if system == "win32" else "darwin" if system == "darwin" else "linux"
    )
    machine = (
        os.uname().machine
        if hasattr(os, "uname")
        else os.environ.get("PROCESSOR_ARCHITECTURE", "")
    )
    arch = {
        "AMD64": "x86_64",
        "x86_64": "x86_64",
        "aarch64": "aarch64",
        "ARM64": "aarch64",
    }.get(machine, machine)
    return f"{os_name}-{arch}"


def discover_installation(
    agent: RegistryAgent,
    *,
    target: str | None = None,
    cache: dict[str, Any] | None,
    installed: tuple[InstalledLauncher, ...] = (),
) -> InstallationRecord:
    target = target or current_platform_target()
    distribution = choose_binary_distribution(agent, target=target)
    launcher = (
        match_distribution(distribution, installed)
        if distribution and distribution.type in {"npx", "uvx"}
        else None
    )
    cmd = (
        Path(launcher.command).stem if launcher else _distribution_command(distribution)
    )
    if cache and cache.get("installed") and cache.get("executable"):
        executable = str(Path(cache["executable"]).resolve())
        if Path(executable).is_file():
            return InstallationRecord(
                agent.id,
                agent.version,
                target,
                cmd,
                float(cache.get("checked_at", _now())),
                True,
                executable,
            )
    if launcher:
        executable = launcher.command
    else:
        found = shutil.which(Path(cmd).name) if cmd else None
        executable = str(Path(found).resolve()) if found else None
    return InstallationRecord(
        agent.id, agent.version, target, cmd, _now(), executable is not None, executable
    )


def build_install_state(record: InstallationRecord) -> dict[str, Any]:
    return {
        "agent_id": record.agent_id,
        "version": record.version,
        "target": record.target,
        "cmd": record.cmd,
        "checked_at": record.checked_at,
        "installed": record.installed,
        "executable": record.executable,
    }


def resolve_explicit_command(command: str, *, base_dir: Path) -> str:
    path = Path(command).expanduser()
    if not path.is_absolute():
        raise ValueError("ACP command must be an absolute executable path")
    resolved = path.resolve()
    if not resolved.is_file():
        raise ValueError("ACP command must point to an existing executable file")
    return str(resolved)


def _valid_args(value: Any) -> tuple[str, ...] | None:
    if not isinstance(value, list) or not all(
        isinstance(arg, str) and arg for arg in value
    ):
        return None
    return tuple(value)


def _valid_env(value: Any) -> dict[str, str] | None:
    if value is None:
        return {}
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and key and isinstance(item, str) and item
        for key, item in value.items()
    ):
        return None
    return value


def _distribution_command(distribution: RegistryDistribution | None) -> str:
    return distribution.cmd if distribution else ""


def _official_distributions(
    model: _RegistryAgentModel,
) -> tuple[RegistryDistribution, ...]:
    distributions = [
        RegistryDistribution(
            target=dist.target,
            type=dist.type,
            cmd=dist.cmd,
            args=tuple(dist.args),
            env=dist.env,
        )
        for dist in model.distributions
    ]
    distribution = model.distribution or {}
    binary = distribution.get("binary")
    if isinstance(binary, dict):
        for target, value in binary.items():
            if not isinstance(target, str) or not isinstance(value, dict):
                continue
            cmd = value.get("cmd")
            args = _valid_args(value.get("args", []))
            env = _valid_env(value.get("env"))
            if isinstance(cmd, str) and cmd and args is not None and env is not None:
                distributions.append(
                    RegistryDistribution(target, "binary", cmd, args, env)
                )
    for distribution_type in ("npx", "uvx"):
        package_distribution = distribution.get(distribution_type)
        if not isinstance(package_distribution, dict):
            continue
        package = package_distribution.get("package")
        args = _valid_args(package_distribution.get("args", []))
        env = _valid_env(package_distribution.get("env"))
        if not isinstance(package, str) or args is None or env is None:
            continue
        if distribution_type == "npx" and not (
            _UNSCOPED_PACKAGE_RE.fullmatch(package)
            or _SCOPED_NPM_PACKAGE_RE.fullmatch(package)
        ):
            continue
        if distribution_type == "uvx" and not _UNSCOPED_PACKAGE_RE.fullmatch(package):
            continue
        cmd = ""
        distributions.append(
            RegistryDistribution("", distribution_type, cmd, args, env, package)
        )
    return tuple(distributions)


def parse_registry_agents(value: Any) -> list[RegistryAgent]:
    """Strictly parse the consumed portion of an official registry document.

    Raises ``ValueError`` naming the offending agent index on any shape or
    conversion failure — never coerces non-string scalars.
    """
    if not isinstance(value, dict):
        raise ValueError("ACP registry document must be an object")
    raw_version = value.get("version")
    if raw_version is not None and str(raw_version) != REGISTRY_DOC_VERSION:
        raise ValueError(
            f"unsupported ACP registry document version {raw_version!r}; "
            f"expected {REGISTRY_DOC_VERSION!r}"
        )
    raw = value.get("agents")
    if not isinstance(raw, list):
        raise ValueError("ACP registry agents must be a list")
    agents: list[RegistryAgent] = []
    for index, item in enumerate(raw):
        try:
            model = _RegistryAgentModel.model_validate(item)
        except ValidationError as exc:
            raise ValueError(
                f"invalid ACP registry agent at index {index}: {exc}"
            ) from exc
        distributions = _official_distributions(model)
        agents.append(
            RegistryAgent(
                id=model.id, version=model.version, distributions=distributions
            )
        )
    return agents


def fetch_registry(url: str = OFFICIAL_REGISTRY_URL) -> Any:
    """Fetch the bounded official registry document synchronously."""
    with httpx.Client(timeout=httpx.Timeout(10.0), follow_redirects=False) as client:
        response = client.get(url)
        response.raise_for_status()
        if len(response.content) > 5 * 1024 * 1024:
            raise ValueError("ACP registry response exceeds 5 MiB")
        return response.json()


def load_registry_agents(cache_path: Path, *, ttl_days: int) -> list[RegistryAgent]:
    """Load a fresh cache, refreshing it, or use stale data on fetch failure."""
    payload = fresh_cache(cache_path, ttl_days=ttl_days)
    if payload is None:
        try:
            document = fetch_registry()
            write_cache(cache_path, document)
            payload = {"value": document}
        except (OSError, ValueError, TypeError, KeyError, httpx.HTTPError):
            payload = read_stale_cache(cache_path)
    if payload is None:
        return []
    return parse_registry_agents(payload.get("value"))


def explicit_backend(engine_id: str, settings: Any) -> EngineBackend:
    from .runners.acp.backend import acp_backend

    command = resolve_explicit_command(settings.command, base_dir=Path.cwd())
    config = settings.model_dump()
    config["command"] = command
    config["args"] = list(settings.args)
    return acp_backend(engine_id, config)


def registry_backend(
    record: InstallationRecord, distribution: RegistryDistribution
) -> EngineBackend:
    from .runners.acp.backend import acp_backend

    config = {
        "command": record.executable,
        "args": list(distribution.args),
        "env": distribution.env or {},
    }
    return acp_backend(normalise_registry_id(record.agent_id), config)
