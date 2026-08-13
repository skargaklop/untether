from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from untether.acp_registry import (
    RegistryAgent,
    RegistryDistribution,
    build_install_state,
    choose_binary_distribution,
    discover_installation,
    fresh_cache,
    normalise_registry_id,
    read_stale_cache,
    resolve_explicit_command,
    select_backends,
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
    assert choose_binary_distribution(agent, target="windows-x86_64").cmd == "demo.exe"


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


def test_collisions_and_explicit_override() -> None:
    agent = RegistryAgent(
        id="demo-agent",
        version="1",
        distributions=(RegistryDistribution("windows-x86_64", "binary", "demo"),),
    )
    chosen = select_backends(
        [agent],
        target="windows-x86_64",
        executables={"demo": "C:/demo.exe"},
        explicit_ids={"demo_agent"},
    )
    assert chosen == []
    with pytest.raises(ValueError, match="absolute"):
        resolve_explicit_command("demo", base_dir=Path.cwd())
    assert (
        select_backends(
            [agent],
            target="windows-x86_64",
            executables={"demo": "C:/demo.exe"},
            explicit_ids=set(),
        )[0].agent_id
        == "demo-agent"
    )


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
