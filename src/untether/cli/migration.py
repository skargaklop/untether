"""Guarded operator CLI for read-only migration diagnostics.

The actual transfer verbs remain disabled until complete source journal,
destination receiver, and owner fencing contracts are implemented.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer

from ..session_migration import transfer
from ..session_migration.file_bundle_adapter import register_from_settings
from ..session_migration.native_adapter import CapabilityUnavailable
from ..session_migration.ssh_transport import PreflightError, inspect_peer
from ..settings import load_settings

SERVE_VERBS = frozenset({"inspect", "receive", "export", "status", "export-complete"})
SERVE_STDIN_CHUNK_BYTES = 65536
SERVE_MAX_STDIN_BYTES = 1_048_576


def _read_bounded_stdin(max_bytes: int) -> bytes:
    """Read stdin in chunks, refusing payloads beyond max_bytes before buffering."""
    stream = getattr(sys.stdin, "buffer", None)
    chunks: list[bytes] = []
    total = 0
    while True:
        if stream is not None:
            chunk = stream.read(SERVE_STDIN_CHUNK_BYTES)
        else:
            chunk = sys.stdin.read(SERVE_STDIN_CHUNK_BYTES).encode("utf-8")
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(
                f"STDIN_PAYLOAD_TOO_LARGE: serve stdin exceeds {max_bytes} byte cap"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _build_receiver_endpoint(*, engine: str, project: str, cfg):
    """Build only the explicitly configured endpoint, never a chat-supplied one."""
    import platform

    from ..session_migration.health_command import run_health
    from ..session_migration.native_receiver import NativeReceiver
    from ..session_migration.receiver_protocol import ReceiverEndpoint

    profile = cfg.receiver
    if profile is None:
        raise CapabilityUnavailable("RECEIVER_ENDPOINT_UNAVAILABLE")
    if profile.engine != engine or profile.project != project:
        raise CapabilityUnavailable("RECEIVER_SCOPE_MISMATCH")
    return ReceiverEndpoint(
        endpoint_id=profile.endpoint_id,
        execution_mode=profile.execution_mode,
        destination_binding=profile.destination_binding,
        project=profile.project,
        project_root=profile.project_root,
        authorized_peer=profile.authorized_peer,
        deployment="untether-native",
        engine=profile.engine,
        arch=platform.machine(),
        probe_health=lambda: run_health(profile.health_argv, cfg.probe_timeout_s),
        native_receiver=NativeReceiver(
            Path(profile.state_dir),
            max_archive=cfg.max_archive_bytes,
            max_expanded=cfg.max_expanded_bytes,
        ),
    )


def build_migrate_app() -> typer.Typer:
    app = typer.Typer(
        help="Inspect and transfer native sessions through configured SSH peers.",
    )

    @app.callback()
    def migrate_group() -> None:
        """Keep 'inspect' as an explicit subcommand, even if it is the only one."""

    @app.command("inspect")
    def inspect(
        peer: str = typer.Argument(..., help="Configured peer name"),
        project: str = typer.Option(..., "--project", help="Mapped project alias"),
        direction: str = typer.Option("to", "--direction"),
    ) -> None:
        """Fresh SSH and receiver health probe; never copies session data."""
        try:
            settings, _config_path = load_settings()
            cfg = settings.session_migration
            if not cfg.enabled:
                raise PreflightError(
                    "MIGRATION_DISABLED: explicitly enable after receiver installation"
                )
            profile = cfg.peers.get(peer)
            if profile is None:
                raise PreflightError("PEER_UNKNOWN: select a configured peer")
            result = inspect_peer(
                profile,
                project=project,
                direction=direction,
                timeout_s=cfg.connect_timeout_s,
            )
        except (PreflightError, ValueError) as exc:
            typer.echo(f"preflight refused: {exc}", err=True)
            raise typer.Exit(code=2) from exc
        typer.echo(
            "Read-only preflight passed for endpoint "
            f"{result['endpoint_id']} ({result['execution_mode']}). "
            "No session was transferred by this inspection."
        )

    @app.command("transfer")
    def transfer_cmd(
        peer: str = typer.Argument(..., help="Configured peer name"),
        project: str = typer.Option(..., "--project", help="Mapped project alias"),
        session_id: str = typer.Option(..., "--session-id", help="Exact session id"),
        engine: str = typer.Option(..., "--engine", help="Exact engine id"),
        direction: str = typer.Option("to", "--direction", help="'to' or 'from'"),
        chat: int | None = typer.Option(
            None, "--chat", help="Optional destination chat id"
        ),
        topic: int | None = typer.Option(
            None, "--topic", help="Optional destination topic id"
        ),
    ) -> None:
        """Native exact-restore session transfer; requires a verified persistence adapter."""
        try:
            settings, config_path = load_settings()
            cfg = settings.session_migration
            from pathlib import Path

            root = (
                config_path.parent if config_path else Path.cwd()
            ) / "session-migration"
            result = transfer.perform(
                cfg,
                peer_name=peer,
                project=project,
                session_id=session_id,
                engine=engine,
                direction=direction,
                root=root,
                config_path=config_path,
                destination_chat=chat,
                destination_topic=topic,
            )
        except (PreflightError, CapabilityUnavailable, ValueError) as exc:
            typer.echo(f"session transfer refused: {exc}", err=True)
            raise typer.Exit(code=2) from exc
        typer.echo(
            f"Transfer {result.operation_id} {result.state}: "
            f"session {result.session_id} ({result.engine}) "
            f"native-id={result.native_id} receipt={result.receipt_sha256[:12]}…"
        )

    @app.command("serve")
    def serve(
        verb: str = typer.Argument(
            ..., help="Fixed verb: inspect|receive|export|status|export-complete"
        ),
        engine: str = typer.Option(..., "--engine", help="Exact engine id"),
        project: str = typer.Option(..., "--project", help="Mapped project alias"),
        config: Annotated[
            Path | None, typer.Option("--config", help="Receiver configuration file")
        ] = None,
    ) -> None:
        """Serve one bounded receiver request for the configured endpoint."""
        if verb not in SERVE_VERBS:
            typer.echo(
                "serve refused: VERB_INVALID: use one of "
                "inspect|receive|export|status|export-complete",
                err=True,
            )
            raise typer.Exit(code=2)
        if config is not None and not config.is_file():
            typer.echo(f"serve refused: CONFIG_MISSING: {config}", err=True)
            raise typer.Exit(code=2)
        try:
            payload = _read_bounded_stdin(SERVE_MAX_STDIN_BYTES)
            settings, _config_path = (
                load_settings(config) if config is not None else load_settings()
            )
            cfg = settings.session_migration
            if not cfg.enabled:
                raise PreflightError(
                    "MIGRATION_DISABLED: explicitly enable after receiver installation"
                )
            register_from_settings(cfg, engine=engine)
            endpoint = _build_receiver_endpoint(engine=engine, project=project, cfg=cfg)
            from ..session_migration.receiver_protocol import handle_receiver

            response = handle_receiver(endpoint, verb=verb, payload=payload)
        except (PreflightError, CapabilityUnavailable, ValueError) as exc:
            typer.echo(f"serve refused: {exc}", err=True)
            raise typer.Exit(code=2) from exc
        output = getattr(sys.stdout, "buffer", None)
        if output is not None:
            output.write(response)
            output.flush()
        else:
            sys.stdout.write(response.decode("utf-8"))

    return app
