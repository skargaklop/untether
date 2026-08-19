from __future__ import annotations

import os
import shutil
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from .acp_installations import discover_installed_launchers
from .acp_registry import (
    InstallationRecord,
    build_install_state,
    choose_binary_distribution,
    current_platform_target,
    discover_installation,
    explicit_backend,
    load_installation_cache,
    load_registry_agents,
    normalise_registry_id,
    registry_backend,
    write_installation_cache,
)
from .backends import EngineBackend
from .config import ConfigError, ProjectsConfig
from .engines import get_backend, list_backend_ids
from .ids import RESERVED_CHAT_COMMANDS
from .logging import get_logger
from .router import AutoRouter, EngineStatus, RunnerEntry
from .runner import JsonlSubprocessRunner
from .settings import UntetherSettings
from .transport_runtime import TransportRuntime

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RuntimeSpec:
    router: AutoRouter
    projects: ProjectsConfig
    allowlist: list[str] | None
    plugin_configs: Mapping[str, Any] | None
    watch_config: bool = False
    dynamic_engine_ids: frozenset[str] = frozenset()

    def to_runtime(self, *, config_path: Path | None) -> TransportRuntime:
        return TransportRuntime(
            router=self.router,
            projects=self.projects,
            allowlist=self.allowlist,
            config_path=config_path,
            plugin_configs=self.plugin_configs,
            watch_config=self.watch_config,
            dynamic_engine_ids=self.dynamic_engine_ids,
        )

    def apply(self, runtime: TransportRuntime, *, config_path: Path | None) -> None:
        runtime.update(
            router=self.router,
            projects=self.projects,
            allowlist=self.allowlist,
            config_path=config_path,
            plugin_configs=self.plugin_configs,
            watch_config=self.watch_config,
            dynamic_engine_ids=self.dynamic_engine_ids,
        )


def resolve_plugins_allowlist(
    settings: UntetherSettings | None,
) -> list[str] | None:
    if settings is None:
        return None
    enabled = list(settings.plugins.enabled)
    return enabled or None


def resolve_default_engine(
    *,
    override: str | None,
    settings: UntetherSettings,
    config_path: Path,
    engine_ids: list[str],
) -> str:
    default_engine = override or settings.default_engine or "codex"
    if default_engine not in engine_ids:
        available = ", ".join(sorted(engine_ids))
        raise ConfigError(
            f"Unknown default engine {default_engine!r}. Available: {available}."
        )
    return default_engine


def build_router(
    *,
    settings: UntetherSettings,
    config_path: Path,
    backends: list[EngineBackend],
    default_engine: str,
) -> AutoRouter:
    entries: list[RunnerEntry] = []
    # #532: per-issue (engine_id, issue_text, had_user_config) tuples so we
    # can emit ONE summary line + a focused WARN only for engines the user
    # actually configured. Previously every non-default engine without a CLI
    # on PATH fired its own WARN on every config.reload — 5xN log spam on
    # any single-engine host (e.g. channelo runs only claude).
    issues: list[tuple[str, str, bool]] = []

    for backend in backends:
        engine_id = backend.id
        issue: str | None = None
        status: EngineStatus = "ok"
        engine_cfg: dict
        had_user_config = False
        try:
            engine_cfg = settings.engine_config(engine_id, config_path=config_path)
            had_user_config = bool(engine_cfg)
        except ConfigError as exc:
            if engine_id == default_engine:
                raise
            issue = str(exc)
            status = "bad_config"
            engine_cfg = {}
            had_user_config = True  # they tried to configure it, just badly

        if backend.config:
            engine_cfg = {**backend.config, **engine_cfg}
            had_user_config = True

        try:
            runner = backend.build_runner(engine_cfg, config_path)
        except Exception as exc:
            if engine_id == default_engine:
                raise
            issue = issue or str(exc)
            if engine_cfg:
                try:
                    runner = backend.build_runner({}, config_path)
                except Exception as fallback_exc:  # noqa: BLE001
                    issues.append(
                        (engine_id, issue or str(fallback_exc), had_user_config)
                    )
                    continue
                status = "bad_config"
            else:
                status = "load_error"
                issues.append((engine_id, issue, had_user_config))
                continue

        # Apply global [runners] lifecycle settings to runners exposing the
        rs = settings.runners
        configured_runner = cast(JsonlSubprocessRunner, runner)
        if hasattr(runner, "startup_timeout_s"):
            configured_runner.startup_timeout_s = rs.startup_timeout_s
        if hasattr(runner, "idle_timeout_s"):
            configured_runner.idle_timeout_s = rs.idle_timeout_s
        if hasattr(runner, "shutdown_timeout_s"):
            configured_runner.shutdown_timeout_s = rs.shutdown_timeout_s
        if hasattr(runner, "kill_tree_on_cancel"):
            configured_runner.kill_tree_on_cancel = rs.kill_tree_on_cancel
        if hasattr(runner, "retry_max_attempts"):
            configured_runner.retry_max_attempts = rs.retry_max_attempts
        if hasattr(runner, "retry_base_delay_s"):
            configured_runner.retry_base_delay_s = rs.retry_base_delay_s

        cmd = backend.cli_cmd or backend.id
        if shutil.which(cmd) is None:
            status = "missing_cli"
            if issue:
                issue = f"{issue}; {cmd} not found on PATH"
            else:
                issue = f"{cmd} not found on PATH"

        if status != "ok" and engine_id == default_engine:
            raise ConfigError(f"Default engine {engine_id!r} unavailable: {issue}")

        if status != "ok" and engine_id != default_engine:
            issues.append((engine_id, issue or "unknown", had_user_config))

        entries.append(
            RunnerEntry(
                engine=engine_id,
                runner=runner,
                status=status,
                issue=issue,
            )
        )

    _log_setup_summary(entries, issues, default_engine)

    return AutoRouter(entries=entries, default_engine=default_engine)


def _log_setup_summary(
    entries: list[RunnerEntry],
    issues: list[tuple[str, str, bool]],
    default_engine: str,
) -> None:
    """#532: one INFO summary per reload + focused WARN per engine the user
    has actively configured.

    The previous behaviour emitted ``[warning] setup.warning`` per missing
    engine on every ``config.reload.applied``. On a single-engine host that
    is 5 WARNs per reload, padding warn/error filters in
    untether-issue-watcher, /monitor, and Grafana with intentional install
    state.
    """
    found = sorted(e.engine for e in entries if e.status == "ok")
    missing_on_path = sorted(
        engine for engine, issue, _ in issues if "not found on PATH" in issue
    )
    bad_config = sorted(
        engine for engine, issue, _ in issues if "not found on PATH" not in issue
    )

    # Always emit the summary — single line, INFO, intentional state.
    logger.info(
        "setup.summary",
        default_engine=default_engine,
        found=found,
        missing_on_path=missing_on_path,
        bad_config=bad_config,
    )

    # Loud WARN only for engines the user actually tried to configure.
    # Engines with no [engines.<id>] block in untether.toml are not
    # interesting — the user didn't ask for them.
    for engine_id, issue, had_user_config in issues:
        if had_user_config:
            logger.warning(
                "setup.warning",
                engine=engine_id,
                issue=issue,
                reason="user-configured engine has issue",
            )


def load_backends(
    *,
    engine_ids: list[str],
    allowlist: list[str] | None,
    default_engine: str,
) -> list[EngineBackend]:
    backends: list[EngineBackend] = []
    load_issues: list[str] = []
    for engine_id in engine_ids:
        try:
            backend = get_backend(engine_id, allowlist=allowlist)
        except ConfigError as exc:
            if engine_id == default_engine:
                raise
            load_issues.append(f"{engine_id}: {exc}")
            continue
        backends.append(backend)
    if not backends:
        raise ConfigError("No engine backends are available.")
    # #532: backend-load failures are rare (entry-point install issues, plugin
    # allowlist gates) so a per-issue INFO is plenty; the corresponding WARN
    # for user-selected engines will come from build_router downstream.
    for issue in load_issues:
        logger.info("setup.backend_load_skipped", issue=issue)
    return backends


def build_runtime_spec(
    *,
    settings: UntetherSettings,
    config_path: Path,
    default_engine_override: str | None = None,
    reserved: Iterable[str] = RESERVED_CHAT_COMMANDS,
) -> RuntimeSpec:
    allowlist = resolve_plugins_allowlist(settings)
    static_ids = list_backend_ids(allowlist=allowlist)
    backends = load_backends(
        engine_ids=static_ids,
        allowlist=allowlist,
        default_engine=settings.default_engine,
    )
    engine_ids = list(static_ids)
    explicit_ids = set(settings.acp.engines)
    static_id_set = set(static_ids)
    # 6.2(b): an id configured under BOTH a static backend and
    # [acp.engines.<id>] is a hard config conflict — fail loudly rather than
    # silently dropping one of the two backends.
    overlap = static_id_set & explicit_ids
    if overlap:
        conflict = sorted(overlap)[0]
        raise ConfigError(
            f"ACP engine {conflict!r} is configured both as a static backend "
            f"and under [acp.engines.{conflict}]"
        )
    for engine_id, engine_settings in settings.acp.engines.items():
        try:
            backends.append(explicit_backend(engine_id, engine_settings))
        except (ValueError, OSError) as exc:
            raise ConfigError(f"Invalid ACP engine {engine_id!r}: {exc}") from exc
    dynamic_ids: set[str] = set()
    if settings.acp.registry.enabled:
        cache_dir = config_path.parent / "cache"
        cache_path = cache_dir / "acp-registry-v1.json"
        install_path = cache_dir / "acp-install-state-v1.json"
        agents = load_registry_agents(
            cache_path, ttl_days=settings.acp.registry.cache_ttl_days
        )
        install_cache = load_installation_cache(install_path)
        changed = False
        reserved_lower = {value.lower() for value in reserved}
        project_aliases = {alias.lower() for alias in settings.projects}
        target = current_platform_target()
        now = time.time()
        ttl_s = settings.acp.registry.cache_ttl_days * 86400
        installed_launchers = discover_installed_launchers(
            env=os.environ, home=Path.home()
        )
        for agent in agents:
            try:
                engine_id = normalise_registry_id(agent.id)
            except ValueError:
                continue
            # Explicit [acp.engines.<id>] config takes precedence silently.
            if engine_id in explicit_ids:
                continue
            # 6.2(a): a registry engine whose normalized id collides with an
            # existing static engine id or project alias is skipped with a
            # warning; reserved commands are skipped silently.
            collision_reason: str | None = None
            if engine_id in static_id_set:
                collision_reason = "static engine id"
            elif engine_id in project_aliases:
                collision_reason = "project alias"
            elif engine_id in reserved_lower:
                collision_reason = "reserved command"
            if collision_reason is not None:
                logger.warning(
                    "acp.registry.skipped_collision",
                    agent=agent.id,
                    engine_id=engine_id,
                    reason=collision_reason,
                )
                continue
            if engine_id in engine_ids:
                continue
            try:
                distribution = choose_binary_distribution(agent, target=target)
                distribution_key = (
                    distribution.package or distribution.cmd if distribution else ""
                )
                cache_key = f"{agent.id}:{agent.version}:{target}:{distribution_key}"
                cached = install_cache.get(cache_key)
                fresh = (
                    isinstance(cached, dict)
                    and now - float(cached.get("checked_at", 0)) <= ttl_s
                )
                record = (
                    InstallationRecord(
                        agent.id,
                        agent.version,
                        target,
                        str(cached.get("cmd", "")),
                        float(cached.get("checked_at", now)),
                        bool(cached.get("installed")),
                        cached.get("executable"),
                    )
                    if fresh and cached is not None
                    else discover_installation(
                        agent,
                        target=target,
                        cache=None,
                        installed=installed_launchers,
                    )
                )
            except (ValueError, OSError):
                continue
            if not fresh:
                install_cache[cache_key] = build_install_state(record)
                changed = True
            if record.installed and distribution is not None and record.executable:
                backends.append(registry_backend(record, distribution))
                engine_ids.append(engine_id)
                dynamic_ids.add(engine_id)
        if changed:
            write_installation_cache(install_path, install_cache)
    dynamic_engine_ids = frozenset(dynamic_ids)
    engine_ids = [backend.id for backend in backends]
    projects = settings.to_projects_config(
        config_path=config_path,
        engine_ids=engine_ids,
        reserved=reserved,
    )
    default_engine = resolve_default_engine(
        override=default_engine_override,
        settings=settings,
        config_path=config_path,
        engine_ids=engine_ids,
    )
    router = build_router(
        settings=settings,
        config_path=config_path,
        backends=backends,
        default_engine=default_engine,
    )
    return RuntimeSpec(
        router=router,
        projects=projects,
        allowlist=allowlist,
        plugin_configs=settings.plugins.model_extra,
        watch_config=settings.watch_config,
        dynamic_engine_ids=dynamic_engine_ids,
    )
