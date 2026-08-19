from __future__ import annotations

import json
from pathlib import Path

from untether.acp_installations import (
    InstalledLauncher,
    discover_installed_launchers,
    match_distribution,
    parse_registry_package,
)
from untether.acp_registry import RegistryDistribution


def test_parse_registry_package_preserves_npm_scope_and_normalises_python() -> None:
    assert parse_registry_package("cline@3.0.55", ecosystem="npm") == (
        "cline",
        "3.0.55",
    )
    assert parse_registry_package("@scope/agent@1.2.0", ecosystem="npm") == (
        "@scope/agent",
        "1.2.0",
    )
    assert parse_registry_package("Minion.Code@0.1.44", ecosystem="python") == (
        "minion-code",
        "0.1.44",
    )


def test_match_distribution_matches_package_across_registry_versions() -> None:
    launcher = InstalledLauncher(
        ecosystem="npm",
        package="cline",
        version="3.0.55",
        command="C:/Tools/cline.cmd",
        metadata_path="C:/npm/node_modules/cline/package.json",
    )
    distribution = RegistryDistribution(
        target="", type="npx", cmd="", package="cline@3.0.55"
    )

    assert match_distribution(distribution, (launcher,)) == launcher
    assert (
        match_distribution(
            RegistryDistribution(target="", type="npx", cmd="", package="cline@3.0.56"),
            (launcher,),
        )
        == launcher
    )
    assert (
        match_distribution(
            RegistryDistribution(target="", type="uvx", cmd="", package="cline@3.0.55"),
            (launcher,),
        )
        is None
    )


def _write_package(
    root: Path, package: str, *, version: str, bin_value: object
) -> None:
    package_dir = root / "node_modules" / Path(package)
    package_dir.mkdir(parents=True)
    (package_dir / "package.json").write_text(
        json.dumps({"name": package, "version": version, "bin": bin_value}),
        encoding="utf-8",
    )


def test_discovery_reads_npm_metadata_and_requires_declared_launcher(
    tmp_path: Path,
) -> None:
    app_data = tmp_path / "appdata"
    npm_root = app_data / "npm"
    _write_package(
        npm_root, "agent-wrapper", version="1.0.0", bin_value={"agent": "bin/agent.js"}
    )
    (npm_root / "agent.cmd").write_text("", encoding="utf-8")

    launchers = discover_installed_launchers(
        env={"APPDATA": str(app_data)}, home=tmp_path
    )

    assert launchers == (
        InstalledLauncher(
            ecosystem="npm",
            package="agent-wrapper",
            version="1.0.0",
            command=str((npm_root / "agent.cmd").resolve()),
            metadata_path=str(
                (npm_root / "node_modules" / "agent-wrapper" / "package.json").resolve()
            ),
        ),
    )


def test_discovery_reads_scoped_bun_metadata_and_omits_ambiguous_bins(
    tmp_path: Path,
) -> None:
    bun_root = tmp_path / "bun"
    global_root = bun_root / "install" / "global"
    _write_package(
        global_root,
        "@scope/agent",
        version="1.0.0",
        bin_value={"agent": "bin/agent.js"},
    )
    (global_root / "agent.cmd").write_text("", encoding="utf-8")
    _write_package(
        global_root,
        "ambiguous-agent",
        version="1.0.0",
        bin_value={"first": "bin/first.js", "second": "bin/second.js"},
    )

    launchers = discover_installed_launchers(
        env={"BUN_INSTALL": str(bun_root)}, home=tmp_path
    )

    assert launchers == (
        InstalledLauncher(
            ecosystem="bun",
            package="@scope/agent",
            version="1.0.0",
            command=str((global_root / "agent.cmd").resolve()),
            metadata_path=str(
                (
                    global_root / "node_modules" / "@scope" / "agent" / "package.json"
                ).resolve()
            ),
        ),
    )


def test_discovery_omits_invalid_package_metadata_and_unbacked_launcher(
    tmp_path: Path,
) -> None:
    app_data = tmp_path / "appdata"
    npm_root = app_data / "npm"
    _write_package(npm_root, "empty-bin", version="1.0.0", bin_value={})
    _write_package(
        npm_root, "escaping-bin", version="1.0.0", bin_value={"escape": "../escape.js"}
    )
    _write_package(
        npm_root, "missing-launcher", version="1.0.0", bin_value="bin/missing.js"
    )

    assert (
        discover_installed_launchers(env={"APPDATA": str(app_data)}, home=tmp_path)
        == ()
    )


def test_discovery_returns_no_launchers_without_metadata_roots(tmp_path: Path) -> None:
    assert discover_installed_launchers(env={}, home=tmp_path) == ()


def _write_python_distribution(
    environment: Path,
    package: str,
    *,
    version: str,
    script: str,
) -> None:
    dist_info = environment / "Lib" / "site-packages" / f"{package}-{version}.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        f"Name: {package}\nVersion: {version}\n", encoding="utf-8"
    )
    (dist_info / "entry_points.txt").write_text(
        f"[console_scripts]\n{script} = {package}.main:run\n", encoding="utf-8"
    )


def test_discovery_reads_uv_receipt_distribution_and_launcher(tmp_path: Path) -> None:
    tool_root = tmp_path / "uv-tools"
    tool = tool_root / "minion-code"
    environment = tool / ".venv"
    tool.mkdir(parents=True)
    (tool / "uv-receipt.toml").write_text('name = "minion-code"\n', encoding="utf-8")
    _write_python_distribution(
        environment, "Minion.Code", version="0.1.44", script="minion-code"
    )
    (environment / "Scripts").mkdir()
    (environment / "Scripts" / "minion-code.cmd").write_text("", encoding="utf-8")

    assert discover_installed_launchers(
        env={"UV_TOOL_DIR": str(tool_root)}, home=tmp_path
    ) == (
        InstalledLauncher(
            ecosystem="uv",
            package="minion-code",
            version="0.1.44",
            command=str((environment / "Scripts" / "minion-code.cmd").resolve()),
            metadata_path=str((tool / "uv-receipt.toml").resolve()),
        ),
    )


def test_discovery_reads_pipx_receipt_distribution_and_launcher(tmp_path: Path) -> None:
    pipx_home = tmp_path / "pipx"
    pipx_bin = tmp_path / "pipx-bin"
    venv = pipx_home / "venvs" / "agent-tools"
    venv.mkdir(parents=True)
    (venv / "pipx_metadata.json").write_text(
        json.dumps({"main_package": {"package_or_url": "agent-tools"}}),
        encoding="utf-8",
    )
    _write_python_distribution(
        venv, "agent_tools", version="2.0.0", script="agent-tools"
    )
    pipx_bin.mkdir()
    (pipx_bin / "agent-tools.cmd").write_text("", encoding="utf-8")

    assert discover_installed_launchers(
        env={"PIPX_HOME": str(pipx_home), "PIPX_BIN_DIR": str(pipx_bin)},
        home=tmp_path,
    ) == (
        InstalledLauncher(
            ecosystem="pipx",
            package="agent-tools",
            version="2.0.0",
            command=str((pipx_bin / "agent-tools.cmd").resolve()),
            metadata_path=str((venv / "pipx_metadata.json").resolve()),
        ),
    )


def test_discovery_omits_python_metadata_without_single_executable(
    tmp_path: Path,
) -> None:
    tool_root = tmp_path / "uv-tools"
    tool = tool_root / "ambiguous"
    environment = tool / ".venv"
    tool.mkdir(parents=True)
    (tool / "uv-receipt.toml").write_text('name = "ambiguous"\n', encoding="utf-8")
    _write_python_distribution(
        environment, "ambiguous", version="1.0.0", script="first"
    )
    entry_points = next(
        (environment / "Lib" / "site-packages").glob("*.dist-info/entry_points.txt")
    )
    entry_points.write_text(
        "[console_scripts]\nfirst = ambiguous.main:run\nsecond = ambiguous.main:run\n",
        encoding="utf-8",
    )
    (environment / "Scripts").mkdir()
    (environment / "Scripts" / "first.cmd").write_text("", encoding="utf-8")

    assert (
        discover_installed_launchers(env={"UV_TOOL_DIR": str(tool_root)}, home=tmp_path)
        == ()
    )


def test_discovery_reads_cargo_receipt_and_launcher(tmp_path: Path) -> None:
    cargo_home = tmp_path / "cargo"
    cargo_home.mkdir()
    (cargo_home / ".crates2.json").write_text(
        json.dumps(
            {
                "installs": {
                    "agent-cli 1.2.3 (registry+https://example.invalid)": {
                        "bins": ["agent-cli"]
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (cargo_home / "bin").mkdir()
    (cargo_home / "bin" / "agent-cli.cmd").write_text("", encoding="utf-8")

    assert discover_installed_launchers(
        env={"CARGO_HOME": str(cargo_home)}, home=tmp_path
    ) == (
        InstalledLauncher(
            ecosystem="cargo",
            package="agent-cli",
            version="1.2.3",
            command=str((cargo_home / "bin" / "agent-cli.cmd").resolve()),
            metadata_path=str((cargo_home / ".crates2.json").resolve()),
        ),
    )
