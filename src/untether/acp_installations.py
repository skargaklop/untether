"""Passive local installation inventory for official ACP registry agents."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

_NPM_PACKAGE_RE = re.compile(
    r"^(?P<name>@[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*|[a-z0-9][a-z0-9._-]*)(?:@(?P<version>[0-9][a-z0-9._+-]*))?$"
)
_PYTHON_PACKAGE_RE = re.compile(
    r"^(?P<name>[a-z0-9][a-z0-9._-]*)(?:@(?P<version>[0-9][a-z0-9._+-]*))?$",
    re.IGNORECASE,
)


class _Distribution(Protocol):
    @property
    def type(self) -> str: ...

    @property
    def package(self) -> str | None: ...


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
    """Return launchers proven by local package-manager metadata only."""
    roots: list[tuple[Literal["npm", "bun"], Path]] = []
    app_data = env.get("APPDATA")
    if app_data:
        roots.append(("npm", Path(app_data) / "npm"))
    bun_install = Path(env["BUN_INSTALL"]) if env.get("BUN_INSTALL") else home / ".bun"
    roots.append(("bun", bun_install / "install" / "global"))
    return tuple(
        launcher
        for ecosystem, root in roots
        for launcher in _package_launchers(root, ecosystem=ecosystem)
    )


def _package_launchers(
    root: Path, *, ecosystem: Literal["npm", "bun"]
) -> tuple[InstalledLauncher, ...]:
    node_modules = root / "node_modules"
    if not node_modules.is_dir():
        return ()
    metadata_paths = tuple(node_modules.glob("*/package.json")) + tuple(
        node_modules.glob("@*/*/package.json")
    )
    return tuple(
        launcher
        for metadata_path in metadata_paths
        if (launcher := _package_launcher(root, metadata_path, ecosystem=ecosystem))
        is not None
    )


def _package_launcher(
    root: Path, metadata_path: Path, *, ecosystem: Literal["npm", "bun"]
) -> InstalledLauncher | None:
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    package = metadata.get("name") if isinstance(metadata, dict) else None
    version = metadata.get("version") if isinstance(metadata, dict) else None
    bin_value = metadata.get("bin") if isinstance(metadata, dict) else None
    if (
        not isinstance(package, str)
        or parse_registry_package(package, ecosystem="npm") is None
    ):
        return None
    if not isinstance(version, str) or not version:
        return None
    if isinstance(bin_value, str):
        launcher_name = package.rsplit("/", 1)[-1]
        launcher_target = bin_value
    elif isinstance(bin_value, dict) and len(bin_value) == 1:
        launcher_name, launcher_target = next(iter(bin_value.items()))
    else:
        return None
    if not isinstance(launcher_name, str) or not launcher_name:
        return None
    if not isinstance(launcher_target, str) or not launcher_target:
        return None
    target_path = (metadata_path.parent / launcher_target).resolve()
    if not target_path.is_relative_to(metadata_path.parent.resolve()):
        return None
    executable = _package_executable(root, launcher_name)
    if executable is None:
        return None
    return InstalledLauncher(
        ecosystem=ecosystem,
        package=package,
        version=version,
        command=str(executable),
        metadata_path=str(metadata_path.resolve()),
    )


def _package_executable(root: Path, name: str) -> Path | None:
    for suffix in (".cmd", ".exe", ""):
        path = root / f"{name}{suffix}"
        if path.is_file():
            return path.resolve()
    return None


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
