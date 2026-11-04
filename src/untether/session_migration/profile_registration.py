"""Persistence-profile registration routing for declared engine profiles.

Single responsibility: turn a declared per-engine persistence profile into the
registered adapter factory. This is profile wiring only — file-store layout
math and record validation stay in :mod:`file_bundle_adapter` and the
concrete adapters. Registration never probes or touches any engine store;
external entry points remain authoritative (a no-op when the engine has no
profile or another registration already claims the engine).
"""

from __future__ import annotations

from pathlib import Path

from .native_adapter import register_persistence_adapter, registered_adapter_engines
from .omp_adapter import OmpPersistenceAdapter
from .settings import SessionMigrationSettings
from .subprocess_adapter import ExecutablePersistenceAdapter


def register_from_settings(settings: SessionMigrationSettings, *, engine: str) -> None:
    """Register the in-tree file-bundle adapter for one declared engine profile.

    No-op when the engine has no persistence profile (plugin entry points stay
    authoritative) or when another registration already claims the engine.
    """
    from .session_gate import configure_session_gate

    configure_session_gate(settings)
    profile = settings.persistence.engines.get(engine)
    if profile is None:
        return
    if profile.format == "omp-v1":
        # Route before file-layout math: OMP owns its realdir validation.
        if engine in registered_adapter_engines():
            return
        register_persistence_adapter(
            engine,
            lambda: OmpPersistenceAdapter(engine=engine, profile=profile),
        )
        return
    if profile.format == "executable-v1":
        # Route before file-layout math: the executable adapter owns its own
        # realdir/executable validation at call time.
        if engine in registered_adapter_engines():
            return
        register_persistence_adapter(
            engine,
            lambda: ExecutablePersistenceAdapter(
                engine=engine,
                executable=Path(profile.executable),
                store=Path(profile.store),
                timeout=profile.timeout,
                interpreter=profile.interpreter,
            ),
        )
        return
    if profile.format == "codex-rollout-v1":
        # Codex rollout journals: the dedicated adapter owns store validation
        # and the app-server resume proof.
        if engine in registered_adapter_engines():
            return
        from .codex_rollout_adapter import CodexRolloutPersistenceAdapter

        register_persistence_adapter(
            engine,
            lambda: CodexRolloutPersistenceAdapter(
                engine=engine,
                sessions_root=Path(profile.sessions_root),
                app_server_executable=Path(profile.app_server_executable),
                resume_timeout_s=profile.resume_timeout_s,
            ),
        )
        return
    if profile.format == "opencode-sync-v1":
        # OpenCode sync events: the adapter owns isolation and serve/replay proof.
        if engine in registered_adapter_engines():
            return
        from .opencode_sync_adapter import OpenCodeSyncAdapter

        register_persistence_adapter(
            engine,
            lambda: OpenCodeSyncAdapter(engine=engine, profile=profile),
        )
        return
    sessions_dir = Path(profile.sessions_dir)
    if engine in registered_adapter_engines():
        return
    from .file_bundle_adapter import FileBundlePersistenceAdapter

    register_persistence_adapter(
        engine,
        lambda: FileBundlePersistenceAdapter(
            engine=engine, profile=profile, sessions_dir=sessions_dir
        ),
    )
