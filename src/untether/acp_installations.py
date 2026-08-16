"""Passive local installation inventory for official ACP registry agents."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping, Protocol, Sequence

_NPM_PACKAGE_RE = re.compile(
    r"^(?P<name>@[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*|[a-z0-9][a-z0-9._-]*)(?:@(?P<version>[0-9][a-z0-9._+-]*))?$"
)
_PYTHON_PACKAGE_RE = re.compile(
    r"^(?P<name>[a-z0-9][a-z0-9._-]*)(?:@(?P<version>[0-9][a-z0-9._+-]*))?$",
    re.IGNORECASE,
)


class _Distribution(Protocol):
    type: str
    package: str | None


@dataclass(frozen=True, slots=True)
class InstalledLauncher:
    """A launcher verified from one local package-manager metadata source."""

    ecosystem: Literal["npm", "bun", "uv", "pipx", "cargo", "binary"]
    package: str
    version: str | None
    command: str
    metadata_path: str


def parse_registry_package(
    value: str, *, ecosystem: Literal["npm", "python"]
) -> tuple[str, str | None] | None:
    """Parse a pinned or unpinned registry package specification strictly."""
    pattern = _NPM_PACKAGE_RE if ecosystem == "npm" else _PYTHON_PACKAGE_RE
    match = pattern.fullmatch(value)
    if match is None:
        return None
    name = match["name"]
    if ecosystem == "python":
        name = re.sub(r"[-_.]+", "-", name).lower()
    return name, match["version"]


def discover_installed_launchers(
    *, env: Mapping[str, str], home: Path
) -> tuple[InstalledLauncher, ...]:
    """Return launchers from available local metadata roots.

    Readers are intentionally added per supported package-manager format. This
    initial interface performs no PATH probing and has no evidence source yet.
    """
    del env, home
    return ()


def match_distribution(
    distribution: _Distribution, installed: Sequence[InstalledLauncher]
) -> InstalledLauncher | None:
    """Return one exact package-backed launcher for a registry distribution."""
    if distribution.package is None:
        return None
    if distribution.type == "npx":
        parsed = parse_registry_package(distribution.package, ecosystem="npm")
        ecosystems = {"npm", "bun"}
    elif distribution.type == "uvx":
        parsed = parse_registry_package(distribution.package, ecosystem="python")
        ecosystems = {"uv", "pipx"}
    else:
        return None
    if parsed is None:
        return None
    package, version = parsed
    candidates = [
        launcher
        for launcher in installed
        if launcher.ecosystem in ecosystems
        and launcher.package == package
        and (version is None or launcher.version == version)
    ]
    return candidates[0] if len(candidates) == 1 else None
