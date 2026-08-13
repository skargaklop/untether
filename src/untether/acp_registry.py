"""Safe, startup-only discovery helpers for generic ACP agents."""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .backends import EngineBackend
from .ids import is_valid_id
from .utils.json_state import atomic_write_json

OFFICIAL_REGISTRY_URL = (
    "https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json"
)


@dataclass(frozen=True, slots=True)
class RegistryDistribution:
    target: str
    type: str
    cmd: str
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = None


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
    return None


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
) -> InstallationRecord:
    target = target or current_platform_target()
    distribution = choose_binary_distribution(agent, target=target)
    cmd = distribution.cmd if distribution else ""
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


def parse_registry_agents(value: Any) -> list[RegistryAgent]:
    """Parse the consumed portion of an official registry document."""
    raw = value.get("agents", value) if isinstance(value, dict) else value
    if not isinstance(raw, list):
        raise ValueError("ACP registry agents must be a list")
    agents: list[RegistryAgent] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("ACP registry agent must be an object")
        distributions: list[RegistryDistribution] = []
        for raw_dist in item.get("distributions", []):
            if not isinstance(raw_dist, dict):
                raise ValueError("ACP registry distribution must be an object")
            distributions.append(
                RegistryDistribution(
                    target=str(raw_dist["target"]),
                    type=str(raw_dist["type"]),
                    cmd=str(raw_dist["cmd"]),
                    args=tuple(str(x) for x in raw_dist.get("args", [])),
                    env={str(k): str(v) for k, v in raw_dist.get("env", {}).items()}
                    or None,
                )
            )
        agents.append(
            RegistryAgent(
                str(item["id"]), str(item.get("version", "")), tuple(distributions)
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


def select_backends(
    agents: list[RegistryAgent],
    *,
    target: str,
    executables: dict[str, str],
    explicit_ids: set[str],
    reserved_ids: set[str] | None = None,
) -> list[InstallationRecord]:
    reserved = reserved_ids or set()
    result: list[InstallationRecord] = []
    seen = set(explicit_ids) | reserved
    for agent in agents:
        try:
            engine_id = normalise_registry_id(agent.id)
        except ValueError:
            continue
        if engine_id in seen:
            continue
        distribution = choose_binary_distribution(agent, target=target)
        if distribution is None or distribution.cmd not in executables:
            continue
        result.append(
            InstallationRecord(
                agent.id,
                agent.version,
                target,
                distribution.cmd,
                _now(),
                True,
                str(Path(executables[distribution.cmd]).resolve()),
            )
        )
        seen.add(engine_id)
    return result
