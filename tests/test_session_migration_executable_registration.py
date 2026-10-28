"""Executable-v1 persistence profile registration routing (no subprocess runs)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from untether.session_migration.file_bundle_adapter import (
    FileBundlePersistenceAdapter,
    register_from_settings,
)
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    require_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.settings import SessionMigrationSettings
from untether.session_migration.subprocess_adapter import (
    ExecutablePersistenceAdapter,
)

ENGINE = "acp-echo"
PI_ENGINE = "pi-engine"
STORE = "/opt/untether/store"


@pytest.fixture
def clean_registry():
    try:
        yield
    finally:
        unregister_persistence_adapter(ENGINE)
        unregister_persistence_adapter(PI_ENGINE)


def executable_profile(**changes):
    profile = {
        "format": "executable-v1",
        "executable": str(sys.executable),
        "store": STORE,
        "interpreter": str(sys.executable),
        "timeout": 120.5,
    }
    return profile | changes


def pi_profile(sessions_dir: str):
    return {
        "format": "pi-jsonl-v3",
        "sessions_dir": sessions_dir,
        "session_record": "pi",
        "restart_argv": ["true", "{session_id}"],
        "restart_timeout_s": 30,
    }


def settings_for(engines: dict) -> SessionMigrationSettings:
    return SessionMigrationSettings.model_validate(
        {"persistence": {"engines": engines}}
    )


@pytest.mark.parametrize("engine", ["codex", "opencode"])
def test_native_profile_routes_to_production_adapter(tmp_path, engine):
    from untether.session_migration.codex_rollout_adapter import (
        CodexRolloutPersistenceAdapter,
    )
    from untether.session_migration.opencode_sync_adapter import OpenCodeSyncAdapter

    store = (tmp_path / "sessions").resolve()
    store.mkdir(mode=0o700)
    if engine == "codex":
        profile = {
            "format": "codex-rollout-v1",
            "sessions_root": str(store),
            "app_server_executable": str(Path(sys.executable).resolve()),
        }
        expected_type = CodexRolloutPersistenceAdapter
    else:
        profile = {
            "format": "opencode-sync-v1",
            "data_root": str(store),
            "executable": str(Path(sys.executable).resolve()),
        }
        expected_type = OpenCodeSyncAdapter
    settings = settings_for({engine: profile})
    try:
        register_from_settings(settings, engine=engine)
        adapter = require_persistence_adapter(engine)
        assert isinstance(adapter, expected_type)
        assert adapter.engine == engine
    finally:
        unregister_persistence_adapter(engine)


def test_executable_profile_routes_to_executable_adapter(clean_registry, tmp_path):
    store = tmp_path / "store"
    store.mkdir()
    settings = settings_for({ENGINE: executable_profile(store=str(store))})
    register_from_settings(settings, engine=ENGINE)
    adapter = require_persistence_adapter(ENGINE)
    assert isinstance(adapter, ExecutablePersistenceAdapter)
    assert adapter.engine == ENGINE
    assert adapter.executable == Path(sys.executable)
    assert adapter.store == store
    assert adapter.interpreter == str(sys.executable)
    assert adapter.timeout == 120.5
    assert adapter._argv() == [
        str(sys.executable),
        str(sys.executable),
        "--native-session-protocol-v1",
        "--store",
        str(store),
    ]


def test_missing_executable_rejects_at_call(clean_registry, tmp_path):
    missing = tmp_path / "absent-tool.exe"
    settings = settings_for(
        {ENGINE: executable_profile(executable=str(missing), interpreter=None)}
    )
    register_from_settings(settings, engine=ENGINE)  # registration never probes
    adapter = require_persistence_adapter(ENGINE)
    with pytest.raises(CapabilityUnavailable, match="PERSISTENCE_EXECUTABLE_NOT_READY"):
        adapter.export_exact("sid-1")


def test_relative_path_profile_refused_at_config(clean_registry):
    with pytest.raises(ValidationError):
        settings_for({ENGINE: executable_profile(executable="rel/bin/tool")})


def test_pi_registration_remains_alongside_executable(clean_registry, tmp_path):
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir(mode=0o700)
    sessions_dir.chmod(0o700)
    settings = settings_for(
        {
            PI_ENGINE: pi_profile(str(sessions_dir)),
            ENGINE: executable_profile(),
        }
    )
    register_from_settings(settings, engine=PI_ENGINE)
    register_from_settings(settings, engine=ENGINE)
    pi_adapter = require_persistence_adapter(PI_ENGINE)
    assert isinstance(pi_adapter, FileBundlePersistenceAdapter)
    assert pi_adapter.engine == PI_ENGINE
    assert pi_adapter.sessions_dir == sessions_dir
    assert isinstance(require_persistence_adapter(ENGINE), ExecutablePersistenceAdapter)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("executable", "rel/tool"),
        ("store", "../store"),
        ("interpreter", "python3"),
        ("timeout", 0),
        ("timeout", 1801),
    ],
)
def test_executable_profile_value_bounds(field, value):
    with pytest.raises(ValidationError):
        settings_for({ENGINE: executable_profile(**{field: value})})


def test_profile_requires_discriminated_format():
    with pytest.raises(ValidationError):
        settings_for({ENGINE: {"executable": str(sys.executable), "store": STORE}})
