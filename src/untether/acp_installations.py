"""Passive local installation inventory for official ACP registry agents."""

from __future__ import annotations

import configparser
import json
import re
import tomllib
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
    npm_bun_roots: list[tuple[Literal["npm", "bun"], Path]] = []
    app_data = env.get("APPDATA")
    if app_data:
        npm_bun_roots.append(("npm", Path(app_data) / "npm"))
    bun_install = Path(env["BUN_INSTALL"]) if env.get("BUN_INSTALL") else home / ".bun"
    npm_bun_roots.append(("bun", bun_install / "install" / "global"))
    uv_root = (
        Path(env["UV_TOOL_DIR"])
        if env.get("UV_TOOL_DIR")
        else home / ".local" / "share" / "uv" / "tools"
    )
    pipx_home = (
        Path(env["PIPX_HOME"]) if env.get("PIPX_HOME") else home / ".local" / "pipx"
    )
    pipx_bin = (
        Path(env["PIPX_BIN_DIR"])
        if env.get("PIPX_BIN_DIR")
        else home / ".local" / "bin"
    )
    cargo_home = Path(env["CARGO_HOME"]) if env.get("CARGO_HOME") else home / ".cargo"
    return (
        tuple(
            launcher
            for ecosystem, root in npm_bun_roots
            for launcher in _package_launchers(root, ecosystem=ecosystem)
        )
        + _uv_launchers(uv_root)
        + _pipx_launchers(pipx_home, pipx_bin)
        + _cargo_launchers(cargo_home)
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


def _cargo_launchers(home: Path) -> tuple[InstalledLauncher, ...]:
    receipt = home / ".crates2.json"
    try:
        installs = json.loads(receipt.read_text(encoding="utf-8")).get("installs")
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(installs, dict):
        return ()
    launchers: list[InstalledLauncher] = []
    for specification, value in installs.items():
        parsed = _cargo_specification(specification)
        bins = value.get("bins") if isinstance(value, dict) else None
        if parsed is None or not isinstance(bins, list) or len(bins) != 1:
            continue
        package, version = parsed
        bin_name = bins[0]
        if not isinstance(bin_name, str) or not bin_name:
            continue
        executable = _package_executable(home / "bin", bin_name)
        if executable is not None:
            launchers.append(
                InstalledLauncher(
                    "cargo", package, version, str(executable), str(receipt.resolve())
                )
            )
    return tuple(launchers)


def _cargo_specification(value: object) -> tuple[str, str] | None:
    if not isinstance(value, str):
        return None
    package, separator, remainder = value.partition(" ")
    version, _, _source = remainder.partition(" ")
    parsed = parse_registry_package(package, ecosystem="python")
    if not separator or not version or parsed is None or parsed[1] is not None:
        return None
    return parsed[0], version


def _uv_launchers(root: Path) -> tuple[InstalledLauncher, ...]:
    return tuple(
        launcher
        for receipt in root.glob("*/uv-receipt.toml")
        if (
            launcher := _python_launcher(
                receipt,
                environment=receipt.parent / ".venv",
                bin_dir=receipt.parent / ".venv" / "Scripts",
                ecosystem="uv",
                package=_receipt_package(receipt),
            )
        )
        is not None
    )


def _pipx_launchers(root: Path, bin_dir: Path) -> tuple[InstalledLauncher, ...]:
    return tuple(
        launcher
        for receipt in (root / "venvs").glob("*/pipx_metadata.json")
        if (
            launcher := _python_launcher(
                receipt,
                environment=receipt.parent,
                bin_dir=bin_dir,
                ecosystem="pipx",
                package=_pipx_package(receipt),
            )
        )
        is not None
    )


def _receipt_package(receipt: Path) -> str | None:
    try:
        value = tomllib.loads(receipt.read_text(encoding="utf-8")).get("name")
    except (OSError, ValueError, tomllib.TOMLDecodeError):
        return None
    return _normalised_python_package(value)


def _pipx_package(receipt: Path) -> str | None:
    try:
        value = json.loads(receipt.read_text(encoding="utf-8"))["main_package"][
            "package_or_url"
        ]
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        return None
    return _normalised_python_package(value)


def _normalised_python_package(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = parse_registry_package(value, ecosystem="python")
    return parsed[0] if parsed and parsed[1] is None else None


def _python_launcher(
    receipt: Path,
    *,
    environment: Path,
    bin_dir: Path,
    ecosystem: Literal["uv", "pipx"],
    package: str | None,
) -> InstalledLauncher | None:
    if package is None:
        return None
    candidates = tuple(
        _distribution_launcher(
            dist_info,
            bin_dir=bin_dir,
            ecosystem=ecosystem,
            package=package,
            receipt=receipt,
        )
        for dist_info in (environment / "Lib" / "site-packages").glob("*.dist-info")
    )
    found = tuple(candidate for candidate in candidates if candidate is not None)
    return found[0] if len(found) == 1 else None


def _distribution_launcher(
    dist_info: Path,
    *,
    bin_dir: Path,
    ecosystem: Literal["uv", "pipx"],
    package: str,
    receipt: Path,
) -> InstalledLauncher | None:
    try:
        fields = _metadata_fields(dist_info / "METADATA")
        scripts = _console_scripts(dist_info / "entry_points.txt")
    except OSError:
        return None
    metadata_package = _normalised_python_package(fields.get("Name"))
    version = fields.get("Version")
    if metadata_package != package or not isinstance(version, str) or len(scripts) != 1:
        return None
    executable = _package_executable(bin_dir, scripts[0])
    if executable is None:
        return None
    return InstalledLauncher(
        ecosystem, package, version, str(executable), str(receipt.resolve())
    )


def _metadata_fields(path: Path) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            break
        key, separator, value = line.partition(":")
        if separator and key in {"Name", "Version"}:
            fields[key] = value.strip()
    return fields


def _console_scripts(path: Path) -> tuple[str, ...]:
    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")
    return (
        tuple(parser["console_scripts"])
        if parser.has_section("console_scripts")
        else ()
    )


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
    package, _version = parsed
    candidates = [
        launcher
        for launcher in installed
        if launcher.ecosystem in ecosystems and launcher.package == package
    ]
    return candidates[0] if len(candidates) == 1 else None
