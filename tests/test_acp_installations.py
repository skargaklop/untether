from __future__ import annotations

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


def test_match_distribution_requires_exact_package_version_and_ecosystem() -> None:
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
        is None
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
        __import__("json").dumps(
            {"name": package, "version": version, "bin": bin_value}
        ),
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
