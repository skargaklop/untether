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
            RegistryDistribution(
                target="", type="npx", cmd="", package="cline@3.0.56"
            ),
            (launcher,),
        )
        is None
    )
    assert (
        match_distribution(
            RegistryDistribution(
                target="", type="uvx", cmd="", package="cline@3.0.55"
            ),
            (launcher,),
        )
        is None
    )


def test_discovery_returns_no_launchers_without_metadata_roots(tmp_path: Path) -> None:
    assert discover_installed_launchers(env={}, home=tmp_path) == ()
