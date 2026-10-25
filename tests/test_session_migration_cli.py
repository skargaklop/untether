"""CLI read-only migration wiring: no network and no real configuration."""
from __future__ import annotations

from types import SimpleNamespace

from typer.testing import CliRunner

from untether.cli import migration
from untether.session_migration.settings import MigrationPeer, SessionMigrationSettings

PIN = "SHA256:" + "A" * 43


def profile():
    return MigrationPeer(
        host="fixture.invalid", user="operator", identity_dir="fake",
        identity_file="selected", known_hosts_file="fake",
        host_key_fingerprint=PIN, receiver_path="/usr/bin/fixture",
        expected_endpoint_id="fixture", destination_binding="enrolled",
        projects={"demo": "/srv/demo"},
    )


def test_cli_inspect_refuses_disabled(monkeypatch):
    monkeypatch.setattr(
        migration, "load_settings",
        lambda: (SimpleNamespace(session_migration=SessionMigrationSettings()), None),
    )
    result = CliRunner().invoke(
        migration.build_migrate_app(), ["inspect", "vps", "--project", "demo"],
    )
    assert result.exit_code == 2
    assert "MIGRATION_DISABLED" in result.output


def test_cli_inspect_shares_endpoint_preflight(monkeypatch):
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": profile()})
    monkeypatch.setattr(
        migration, "load_settings",
        lambda: (SimpleNamespace(session_migration=cfg), None),
    )
    calls = []

    def fake_inspect(peer, **kwargs):
        calls.append((peer, kwargs))
        return {"endpoint_id": "fixture", "execution_mode": "native"}

    monkeypatch.setattr(migration, "inspect_peer", fake_inspect)
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        ["inspect", "vps", "--project", "demo", "--direction", "from"],
    )
    assert result.exit_code == 0, result.output
    assert "Session transfer is not yet implemented" in result.output
    assert calls[0][1]["direction"] == "from"
    assert calls[0][1]["project"] == "demo"
