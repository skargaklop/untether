"""/session Telegram command over the single shared native transfer coordinator.

One command, both directions, explicit sid/engine. The transfer function is
injected so the command layer stays a thin formatting boundary over the same
coordinator the CLI uses. Engine adapter availability is reported honestly:
the in-tree distribution registers no production adapter, and ACP alone is
never claimed as native persistence.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial

import anyio.to_thread
from pydantic import ValidationError

from ..commands import CommandBackend, CommandContext
from ..logging import get_logger
from ..session_catalog import STATE_FILENAME, SessionCatalog
from ..session_migration import transfer
from ..session_migration.native_adapter import CapabilityUnavailable
from ..session_migration.remote import inspect_session
from ..session_migration.settings import SessionMigrationSettings
from ..session_migration.ssh_transport import PreflightError
from ..session_migration.telegram_enrollment import EnrollmentStore
from ..session_migration.transfer import TransferRefusal

logger = get_logger(__name__)

USAGE = (
    "usage: /session push <peer> <project> <sid> <engine> | "
    "/session pull <peer> <project> <sid> <engine> [chat] [topic]"
)


def describe_engine_adapters() -> list[str]:
    """Honest adapter inventory; never claims ACP engines are supported."""
    installed = transfer.registered_adapter_engines()
    if not installed:
        return [
            "engine adapters: none installed",
            "native persistence requires an untether.session_persistence "
            "entry-point plugin per engine; ACP alone does not import state",
        ]
    return [f"engine adapters: {', '.join(sorted(installed))}"]


def _default_settings() -> SessionMigrationSettings:
    from ..config import ConfigError
    from ..settings import load_settings

    try:
        settings, _ = load_settings()
        return settings.session_migration
    except (ConfigError, OSError, ValueError, TypeError):
        return SessionMigrationSettings()


class EngineResolutionRefused(RuntimeError):
    """Outbound engine discovery refused; message is a redacted code."""


def _session_catalog(ctx: CommandContext) -> SessionCatalog:
    from pathlib import Path

    base = ctx.config_path.parent if ctx.config_path is not None else Path.cwd()
    return SessionCatalog(base / STATE_FILENAME)


async def _resolve_discovered_engine(
    ctx: CommandContext, cfg: SessionMigrationSettings, sid: str
) -> str:
    """Resolve an omitted --engine for outbound moves via catalog evidence.

    Catalog presence is engine-resolution evidence only, never ownership
    authorization. Each configured/registered engine is probed individually
    with catalog.get; there is no full-catalog scan and no unique-engine
    guessing: an undiscovered sid refuses rather than transfer.
    """
    catalog = _session_catalog(ctx)
    candidates = sorted(
        set(cfg.persistence.engines) | set(transfer.registered_adapter_engines())
    )
    matched = [
        engine for engine in candidates if await catalog.get(engine, sid) is not None
    ]
    if len(matched) == 1:
        return matched[0]
    if not matched:
        raise EngineResolutionRefused("SESSION_NOT_DISCOVERED")
    raise EngineResolutionRefused("ENGINE_AMBIGUOUS: specify --engine")


class SessionCommand:
    id = "session"
    description = "Transfer a session between peers (native, exact-restore)"

    def __init__(
        self,
        settings: Callable[[], SessionMigrationSettings]
        | SessionMigrationSettings
        | None = None,
        *,
        perform: Callable[..., transfer.TransferResult] | None = None,
    ) -> None:
        self._settings: Callable[[], SessionMigrationSettings]
        if isinstance(settings, SessionMigrationSettings):
            self._settings = lambda: settings
        else:
            self._settings = settings or _default_settings
        self._perform = perform or transfer.perform

    async def handle(self, ctx: CommandContext):
        parts = ctx.args_text.split()
        if not parts:
            return self._text(USAGE + "\n" + "\n".join(describe_engine_adapters()))
        verb, rest = parts[0].lower(), parts[1:]
        if verb == "move":
            if len(rest) < 3 or len(rest) % 2 != 1:
                return self._text(
                    "usage: /session move <sid> --to|--from <peer> [--project <alias>] [--engine <id>]"
                )
            sid, *flags = rest
            options = {}
            for index in range(0, len(flags), 2):
                key, value = flags[index : index + 2]
                if (
                    key not in ("--to", "--from", "--project", "--engine")
                    or key in options
                ):
                    return self._text("session transfer refused: ARGUMENT_INVALID")
                options[key] = value
            if ("--to" in options) == ("--from" in options):
                return self._text("session transfer refused: DIRECTION_INVALID")
            cfg = self._settings()
            if "--to" in options:
                peer_name = options["--to"]
                direction = "to"
            else:
                peer_name = options["--from"]
                direction = "from"
            peer = cfg.peers.get(peer_name)
            if peer is None:
                return self._text("session transfer refused: PEER_UNKNOWN")
            project = options.get("--project")
            if project is None:
                if len(peer.projects) != 1:
                    return self._text(
                        "session transfer refused: PROJECT_AMBIGUOUS: specify --project"
                    )
                project = next(iter(peer.projects))
            engine = options.get("--engine")
            if engine is None:
                if direction == "to":
                    try:
                        engine = await _resolve_discovered_engine(ctx, cfg, sid)
                    except EngineResolutionRefused as exc:
                        return self._text(f"session transfer refused: {exc}")
                else:
                    try:
                        engine = await anyio.to_thread.run_sync(
                            partial(
                                inspect_session,
                                peer,
                                project=project,
                                session_id=sid,
                                timeout_s=cfg.probe_timeout_s,
                            )
                        )
                    except PreflightError:
                        logger.info(
                            "session.transfer.refused", code="REMOTE_INSPECTION_FAILED"
                        )
                        return self._text(
                            "session transfer refused: REMOTE_INSPECTION_FAILED"
                        )
            return await self._run(ctx, direction, [peer_name, project, sid, engine])
        if verb == "push" and len(rest) == 4:
            return await self._run(ctx, "to", rest)
        if verb == "pull" and len(rest) in (4, 5, 6):
            return await self._run(ctx, "from", rest)
        return self._text(USAGE + "\n" + "\n".join(describe_engine_adapters()))

    async def handle_push(self, ctx: CommandContext, peer, project, sid, engine):
        return await self._run(ctx, "to", [peer, project, sid, engine])

    async def handle_pull(self, ctx: CommandContext, peer, project, sid, engine):
        return await self._run(ctx, "from", [peer, project, sid, engine])

    async def _run(
        self,
        ctx: CommandContext,
        direction: str,
        rest: list[str],
    ):
        peer_name, project, sid, engine, *optional = rest
        # Local pull binds only the authenticated chat/thread: the destination
        # defaults to the caller's ctx chat (and topic when scoped). Explicit
        # IDs are accepted solely when they match the caller; foreign explicit
        # IDs are refused before any transfer work starts.
        auth_chat = int(ctx.message.channel_id)
        auth_topic: int | None = None
        if ctx.message.thread_id is not None:
            try:
                auth_topic = int(ctx.message.thread_id)
            except ValueError:
                logger.info("session.transfer.refused", code="DESTINATION_INVALID")
                return self._text("session transfer refused: DESTINATION_INVALID")
        try:
            destination_chat = int(optional[0]) if optional else auth_chat
            destination_topic = int(optional[1]) if len(optional) > 1 else auth_topic
        except ValueError:
            logger.info("session.transfer.refused", code="DESTINATION_INVALID")
            return self._text("session transfer refused: DESTINATION_INVALID")
        if destination_chat != auth_chat or (
            destination_topic is not None and destination_topic != auth_topic
        ):
            logger.info("session.transfer.refused", code="DESTINATION_FOREIGN")
            return self._text("session transfer refused: DESTINATION_FOREIGN")
        try:
            cfg = self._settings()
            result = await anyio.to_thread.run_sync(
                lambda: self._perform(
                    cfg,
                    peer_name=peer_name,
                    project=project,
                    session_id=sid,
                    engine=engine,
                    direction=direction,
                    root=_state_root(ctx),
                    config_path=ctx.config_path,
                    destination_chat=destination_chat,
                    destination_topic=destination_topic,
                )
            )
        except (TransferRefusal, CapabilityUnavailable) as exc:
            # TransferRefusal subclasses CapabilityUnavailable; both carry
            # redacted code-only messages that are safe to render in chat.
            logger.info("session.transfer.refused", error=str(exc))
            return self._text(f"session transfer refused: {exc}")
        except ValidationError as exc:
            # Contract validation failures never leak payload detail.
            logger.warning("session.transfer.refused", error=str(exc))
            return self._text("session transfer refused: REQUEST_INVALID")
        except OSError:
            logger.warning("session.transfer.unavailable")
            return self._text("session transfer refused: TRANSFER_UNAVAILABLE")
        lines = [
            f"session {result.session_id} ({result.engine}) → {result.peer}",
            f"state: {result.state} · native id: {result.native_id}",
            f"receipt: {result.receipt_sha256[:12]}…",
        ]
        if direction == "from":
            if result.state == "bound":
                lines.append("destination chat bound after verified restore")
            else:
                lines.append(
                    "destination chat binding requires chat/topic argument; unverified restores never bind"
                )
        return self._text("\n".join(lines))

    def _text(self, body: str):
        from ..transport import RenderedMessage

        return RenderedMessage(text=body)


def _state_root(ctx: CommandContext):
    from pathlib import Path

    base = ctx.config_path.parent if ctx.config_path is not None else Path.cwd()
    return base / "session-migration"


def _enrollment_store(ctx: CommandContext) -> EnrollmentStore:
    return EnrollmentStore(_state_root(ctx) / "enrollment.json")


BACKEND: CommandBackend = SessionCommand()
