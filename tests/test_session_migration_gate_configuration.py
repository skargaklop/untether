"""Policy contract of ``configure_session_gate`` (untether-native-migration-review).

Covers the explicit-configuration gate applied before runners are built,
using typed policy inputs (no IO):

1. Disabled policy is a no-op — the env var is never touched.
2. Enabled with no persistence profiles is a no-op too.
3. Enabled + profiles with no configured root and no env var refuses
   fail-closed (``SOURCE_GATE_UNCONFIGURED``).
4. Matching configured/env roots are accepted.
5. Conflicting roots refuse (``SOURCE_GATE_ROOT_CONFLICT``) and never
   mutate the env var.
6. An explicit config root is exported to the env var.
Plus: pydantic itself rejects a relative ``gate_root``.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from untether.session_migration.session_gate import (
    GateRefused,
    configure_session_gate,
)
from untether.session_migration.settings import (
    EnginePersistenceProfile,
    EnginePersistenceSettings,
    ReceiverSettings,
    SessionMigrationSettings,
)

ENV = "UNTETHER_SESSION_GATE_ROOT"


def receiver_settings(**overrides):
    return ReceiverSettings.model_validate(
        {
            "endpoint_id": "receiver",
            "engine": "pi",
            "project": "demo",
            "state_dir": "/tmp/state",
            "destination_binding": "enrolled",
            "health_argv": ["health"],
            **overrides,
        }
    )


def test_receiver_project_root_defaults_to_none():
    assert receiver_settings().project_root is None
    assert receiver_settings(project_root=None).project_root is None


@pytest.mark.parametrize(
    "root", ["/srv/demo", r"C:\Projects\demo", r"\\server\share\demo"]
)
def test_receiver_project_root_accepts_absolute_local_paths(root):
    assert receiver_settings(project_root=root).project_root == root


@pytest.mark.parametrize(
    "root",
    [
        "demo",
        "C:demo",
        "/srv/../demo",
        r"C:\Projects\.\demo",
        "/srv/de\x00mo",
        "/srv/de\nmo",
        "/srv/de\rmo",
    ],
)
def test_receiver_project_root_rejects_unsafe_paths(root):
    with pytest.raises(ValidationError):
        receiver_settings(project_root=root)


def policy(
    *, enabled: bool, engines: bool, gate_root: str | None
) -> SessionMigrationSettings:
    settings = SessionMigrationSettings(
        gate_root=gate_root,
        persistence=EnginePersistenceSettings(
            engines={
                "c": EnginePersistenceProfile(
                    format="pi-jsonl-v3",
                    sessions_dir="/tmp/sessions",
                    session_record="events.jsonl",
                    restart_argv=("restart",),
                )
            }
            if engines
            else {},
        ),
    )
    # Isolate gate policy from unrelated enabled-policy peer validation.
    settings.enabled = enabled
    return settings


def test_disabled_never_mutates_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    configure_session_gate(policy(enabled=False, engines=True, gate_root="/tmp/g"))
    assert ENV not in __import__("os").environ


def test_enabled_without_profiles_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    configure_session_gate(policy(enabled=True, engines=False, gate_root=None))
    assert ENV not in __import__("os").environ


def test_enabled_profiles_missing_root_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    with pytest.raises(GateRefused, match="SOURCE_GATE_UNCONFIGURED"):
        configure_session_gate(policy(enabled=True, engines=True, gate_root=None))


def test_matching_roots_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV, "/tmp/g")
    configure_session_gate(policy(enabled=True, engines=True, gate_root="/tmp/g"))
    assert __import__("os").environ[ENV] == "/tmp/g"


def test_conflicting_roots_refuse_env_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV, "/tmp/env-root")
    with pytest.raises(GateRefused, match="SOURCE_GATE_ROOT_CONFLICT"):
        configure_session_gate(policy(enabled=True, engines=True, gate_root="/tmp/cfg"))
    assert __import__("os").environ[ENV] == "/tmp/env-root"


def test_explicit_config_sets_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    configure_session_gate(policy(enabled=True, engines=True, gate_root="/tmp/g"))
    assert __import__("os").environ[ENV] == "/tmp/g"


def test_settings_reject_relative_gate_root() -> None:
    with pytest.raises(ValidationError):
        SessionMigrationSettings(enabled=True, gate_root="relative/gate")
