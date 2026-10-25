"""Guarded operator CLI for read-only migration diagnostics.

The actual transfer verbs remain disabled until complete source journal,
destination receiver, and owner fencing contracts are implemented.
"""

from __future__ import annotations

import typer

from ..session_migration.ssh_transport import PreflightError, inspect_peer
from ..settings import load_settings


def build_migrate_app() -> typer.Typer:
    app = typer.Typer(
        help="Inspect SSH session-migration prerequisites (no transfer).",
    )

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
            "Session transfer is not yet implemented."
        )

    return app
