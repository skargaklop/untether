from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from untether.acp_installations import InstalledLauncher
from untether.acp_registry import (
    REGISTRY_DOC_VERSION,
    RegistryAgent,
    RegistryDistribution,
    build_install_state,
    choose_binary_distribution,
    discover_installation,
    fresh_cache,
    normalise_registry_id,
    parse_registry_agents,
    read_stale_cache,
    resolve_explicit_command,
)
from untether.settings import AcpEngineSettings, AcpRegistrySettings, UntetherSettings


def test_acp_settings_are_strict_and_share_positive_default_ttl() -> None:
    settings = UntetherSettings.model_validate(
        {
            "transport": "telegram",
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                }
            },
        }
    )
    assert settings.acp.registry.enabled is True
    assert settings.acp.registry.cache_ttl_days == 3
    with pytest.raises(ValueError, match="greater than 0"):
        AcpRegistrySettings(cache_ttl_days=0)
    with pytest.raises(ValueError, match="unexpected"):
        AcpRegistrySettings.model_validate({"unexpected": True})


def test_explicit_acp_engine_id_and_command_are_validated() -> None:
    engine = AcpEngineSettings(
        command="C:/Tools/agent.exe",
        config_option_map={"permission_mode": "approval_policy", "plan": "mode"},
    )
    assert engine.command == "C:/Tools/agent.exe"
    assert engine.config_option_map == {
        "permission_mode": "approval_policy",
        "plan": "mode",
    }
    assert engine.request_timeout_s == 60.0
    assert engine.close_timeout_s == 5.0
    with pytest.raises(ValueError, match="engine id"):
        UntetherSettings.model_validate(
            {
                "transport": "telegram",
                "transports": {
                    "telegram": {
                        "bot_token": "token",
                        "chat_id": 1,
                        "allow_any_user": True,
                    }
                },
                "acp": {"engines": {"bad-id": {"command": "C:/agent.exe"}}},
            }
        )


def test_registry_id_normalisation() -> None:
    assert normalise_registry_id("amp-acp") == "amp_acp"
    with pytest.raises(ValueError, match="invalid ACP registry id"):
        normalise_registry_id("Too-Many-CAPS")


def test_choose_binary_distribution_for_current_platform_only() -> None:
    agent = RegistryAgent(
        id="demo-agent",
        version="1",
        distributions=(
            RegistryDistribution(target="linux-x86_64", type="binary", cmd="demo"),
            RegistryDistribution(
                target="windows-x86_64", type="binary", cmd="demo.exe"
            ),
            RegistryDistribution(target="windows-x86_64", type="source", cmd="python"),
        ),
    )
    distribution = choose_binary_distribution(agent, target="windows-x86_64")
    assert distribution is not None
    assert distribution.cmd == "demo.exe"


def test_cache_freshness_and_stale_fallback(tmp_path: Path) -> None:
    fresh = tmp_path / "cache.json"
    fresh.write_text(
        json.dumps({"fetched_at": time.time(), "value": 1}), encoding="utf-8"
    )
    assert fresh_cache(fresh, ttl_days=3) == {
        "fetched_at": pytest.approx(time.time(), abs=2),
        "value": 1,
    }
    stale = tmp_path / "stale.json"
    stale.write_text(json.dumps({"fetched_at": 0, "value": 2}), encoding="utf-8")
    assert fresh_cache(stale, ttl_days=3) is None
    assert read_stale_cache(stale) == {"fetched_at": 0, "value": 2}


def test_discovery_uses_absolute_executable_and_cache(
    tmp_path: Path, monkeypatch
) -> None:
    executable = tmp_path / "demo.exe"
    executable.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        "untether.acp_registry.shutil.which", lambda name: str(executable)
    )
    agent = RegistryAgent(
        id="demo-agent",
        version="1",
        distributions=(RegistryDistribution("windows-x86_64", "binary", "demo.exe"),),
    )
    record = discover_installation(agent, target="windows-x86_64", cache=None)
    assert record.installed is True
    assert record.executable == str(executable.resolve())
    assert os.path.isabs(record.executable)
    assert build_install_state(record)["installed"] is True


def test_official_npx_distribution_uses_matching_inventory_launcher() -> None:
    agent = parse_registry_agents(
        {
            "version": REGISTRY_DOC_VERSION,
            "agents": [
                {
                    "id": "cline",
                    "version": "3.0.55",
                    "distribution": {
                        "npx": {"package": "cline@3.0.55", "args": ["--acp"]}
                    },
                }
            ],
        }
    )[0]
    launcher = InstalledLauncher(
        ecosystem="npm",
        package="cline",
        version="3.0.55",
        command="C:/Tools/cline.cmd",
        metadata_path="C:/npm/node_modules/cline/package.json",
    )

    record = discover_installation(
        agent, target="windows-x86_64", cache=None, installed=(launcher,)
    )

    assert record.cmd == "cline"
    assert record.installed is True
    assert record.executable == "C:/Tools/cline.cmd"


def test_official_binary_distribution_prefers_current_platform() -> None:
    agent = parse_registry_agents(
        {
            "version": REGISTRY_DOC_VERSION,
            "agents": [
                {
                    "id": "demo-agent",
                    "distribution": {
                        "binary": {
                            "linux-x86_64": {"cmd": "demo-linux"},
                            "windows-x86_64": {
                                "cmd": "demo.exe",
                                "args": ["--acp"],
                                "env": {"NO_COLOR": "1"},
                            },
                        }
                    },
                }
            ],
        }
    )[0]

    assert choose_binary_distribution(agent, target="windows-x86_64") == (
        RegistryDistribution(
            "windows-x86_64", "binary", "demo.exe", ("--acp",), {"NO_COLOR": "1"}
        )
    )


def test_official_uvx_distribution_requires_explicit_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "minion-code.cmd"
    executable.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        "untether.acp_registry.shutil.which",
        lambda name: str(executable) if name == "minion-code" else None,
    )
    agent = parse_registry_agents(
        {
            "version": REGISTRY_DOC_VERSION,
            "agents": [
                {
                    "id": "minion-code",
                    "distribution": {
                        "uvx": {"package": "minion-code@0.1.44", "args": ["acp"]}
                    },
                }
            ],
        }
    )[0]

    record = discover_installation(agent, target="windows-x86_64", cache=None)

    assert record.cmd == ""
    assert record.installed is False
    assert record.executable is None


def test_explicit_command_must_be_absolute() -> None:
    with pytest.raises(ValueError, match="absolute"):
        resolve_explicit_command("demo", base_dir=Path.cwd())


def test_parse_registry_agents_valid_document() -> None:
    agents = parse_registry_agents(
        {
            "version": REGISTRY_DOC_VERSION,
            "agents": [
                {
                    "id": "demo-agent",
                    "version": "1.2.3",
                    "distributions": [
                        {
                            "target": "linux-x86_64",
                            "type": "binary",
                            "cmd": "demo",
                            "args": ["--flag"],
                            "env": {"KEY": "value"},
                        }
                    ],
                    "some_open_field": {"nested": True},
                }
            ],
            "schema": "https://example.com/registry.schema.json",
        }
    )
    assert len(agents) == 1
    assert agents[0].id == "demo-agent"
    assert agents[0].version == "1.2.3"
    dist = agents[0].distributions[0]
    assert dist.target == "linux-x86_64"
    assert dist.type == "binary"
    assert dist.cmd == "demo"
    assert dist.args == ("--flag",)
    assert dist.env == {"KEY": "value"}


def test_parse_registry_agents_converts_scalars_not_id() -> None:
    with pytest.raises(ValueError, match="index 0"):
        parse_registry_agents({"agents": [{"id": 42, "version": "1"}]})


def test_parse_registry_agents_missing_agents_key_raises() -> None:
    with pytest.raises(ValueError, match="agents must be a list"):
        parse_registry_agents({"version": REGISTRY_DOC_VERSION})


def test_parse_registry_agents_non_object_document_raises() -> None:
    with pytest.raises(ValueError, match="document must be an object"):
        parse_registry_agents([{"id": "demo-agent"}])


def test_parse_registry_agents_wrong_document_version_raises() -> None:
    with pytest.raises(ValueError, match="document version"):
        parse_registry_agents({"version": "999", "agents": []})


def test_parse_registry_agents_valid_without_document_version() -> None:
    assert parse_registry_agents({"agents": [{"id": "demo-agent"}]})[0].id == (
        "demo-agent"
    )


def test_parse_registry_agents_rejects_invalid_id_pattern() -> None:
    with pytest.raises(ValueError, match="index 1"):
        parse_registry_agents(
            {
                "agents": [
                    {"id": "ok-agent"},
                    {"id": "Not-Lowercase"},
                ]
            }
        )


def test_parse_registry_agents_tolerates_unknown_extra_fields() -> None:
    agents = parse_registry_agents(
        {
            "agents": [
                {
                    "id": "demo-agent",
                    "future_field": True,
                    "distributions": [
                        {
                            "target": "linux-x86_64",
                            "type": "binary",
                            "cmd": "demo",
                            "extra": "ignored",
                        }
                    ],
                }
            ]
        }
    )
    assert agents[0].distributions[0].cmd == "demo"


def test_install_state_round_trip_shape() -> None:
    record = discover_installation(
        RegistryAgent(
            "demo-agent",
            "1",
            (RegistryDistribution("windows-x86_64", "binary", "demo"),),
        ),
        target="windows-x86_64",
        cache={"installed": False},
    )
    assert build_install_state(record)["checked_at"] > 0
