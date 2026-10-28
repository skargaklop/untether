"""CLI read-only migration wiring: no network and no real configuration."""

from __future__ import annotations

from types import SimpleNamespace

from typer.testing import CliRunner

from untether.cli import migration
from untether.session_migration.settings import MigrationPeer, SessionMigrationSettings

PIN = "SHA256:" + "A" * 43


def profile():
    return MigrationPeer(
        host="fixture.invalid",
        user="operator",
        identity_dir="fake",
        identity_file="selected",
        known_hosts_file="fake",
        host_key_fingerprint=PIN,
        receiver_path="/usr/bin/fixture",
        expected_endpoint_id="fixture",
        destination_binding="enrolled",
        projects={"demo": "/srv/demo"},
    )


def test_cli_inspect_refuses_disabled(monkeypatch):
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda: (SimpleNamespace(session_migration=SessionMigrationSettings()), None),
    )
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        ["inspect", "vps", "--project", "demo"],
    )
    assert result.exit_code == 2
    assert "MIGRATION_DISABLED" in result.output


def test_cli_inspect_shares_endpoint_preflight(monkeypatch):
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": profile()})
    monkeypatch.setattr(
        migration,
        "load_settings",
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
    assert "Read-only preflight passed" in result.output
    assert "No session was transferred" in result.output
    assert calls[0][1]["direction"] == "from"
    assert calls[0][1]["project"] == "demo"


def test_cli_transfer_calls_single_shared_coordinator(monkeypatch, tmp_path):
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": profile()})
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda: (SimpleNamespace(session_migration=cfg), tmp_path / "untether.toml"),
    )
    calls = []

    def fake_perform(settings, **kwargs):
        calls.append(kwargs)
        from untether.session_migration.transfer import TransferResult

        return TransferResult(
            operation_id="op-123",
            session_id=kwargs["session_id"],
            engine=kwargs["engine"],
            direction=kwargs["direction"],
            peer=kwargs["peer_name"],
            state="completed",
            native_id=kwargs["session_id"],
            receipt_sha256="b" * 64,
        )

    monkeypatch.setattr(migration.transfer, "perform", fake_perform)
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        [
            "transfer",
            "vps",
            "--project",
            "demo",
            "--session-id",
            "sid-1",
            "--engine",
            "fixture",
            "--direction",
            "to",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Transfer op-123 completed" in result.output
    assert calls[0]["session_id"] == "sid-1"
    assert calls[0]["engine"] == "fixture"
    assert calls[0]["direction"] == "to"
    assert calls[0]["peer_name"] == "vps"


def test_cli_transfer_refusal_is_redacted(monkeypatch, tmp_path):
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": profile()})
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda: (SimpleNamespace(session_migration=cfg), tmp_path / "untether.toml"),
    )

    def failing_perform(*_args, **_kwargs):
        from untether.session_migration.native_adapter import CapabilityUnavailable

        raise CapabilityUnavailable(
            "ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE: ACP session/load does not import state"
        )

    monkeypatch.setattr(migration.transfer, "perform", failing_perform)
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        [
            "transfer",
            "vps",
            "--project",
            "demo",
            "--session-id",
            "sid-1",
            "--engine",
            "acp",
        ],
    )
    assert result.exit_code == 2
    assert "session transfer refused" in result.output
    assert "ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE" in result.output


def test_cli_transfer_pull_calls_coordinator_with_chat_and_topic(monkeypatch, tmp_path):
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": profile()})
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda: (SimpleNamespace(session_migration=cfg), tmp_path / "untether.toml"),
    )
    calls = []

    def fake_perform(settings, **kwargs):
        calls.append(kwargs)
        from untether.session_migration.transfer import TransferResult

        return TransferResult(
            operation_id="op-456",
            session_id=kwargs["session_id"],
            engine=kwargs["engine"],
            direction=kwargs["direction"],
            peer=kwargs["peer_name"],
            state="bound",
            native_id=kwargs["session_id"],
            receipt_sha256="c" * 64,
        )

    monkeypatch.setattr(migration.transfer, "perform", fake_perform)
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        [
            "transfer",
            "vps",
            "--project",
            "demo",
            "--session-id",
            "sid-2",
            "--engine",
            "fixture",
            "--direction",
            "from",
            "--chat",
            "-100123",
            "--topic",
            "42",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Transfer op-456 bound" in result.output
    assert calls[0]["session_id"] == "sid-2"
    assert calls[0]["engine"] == "fixture"
    assert calls[0]["direction"] == "from"
    assert calls[0]["destination_chat"] == -100123
    assert calls[0]["destination_topic"] == 42
    assert calls[0]["config_path"] == tmp_path / "untether.toml"


def test_cli_serve_refuses_invalid_verb():
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        ["serve", "deploy", "--engine", "acp", "--project", "demo"],
    )
    assert result.exit_code == 2
    assert "VERB_INVALID" in result.output
    assert "inspect|receive|export|status" in result.output


def test_cli_serve_refuses_oversize_stdin():
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        ["serve", "receive", "--engine", "acp", "--project", "demo"],
        input=b"x" * (migration.SERVE_MAX_STDIN_BYTES + 1),
    )
    assert result.exit_code == 2
    assert "STDIN_PAYLOAD_TOO_LARGE" in result.output


def test_cli_serve_refuses_when_endpoint_runtime_unavailable(monkeypatch):
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": profile()})
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda: (SimpleNamespace(session_migration=cfg), None),
    )
    registered = []
    monkeypatch.setattr(
        migration,
        "register_from_settings",
        lambda settings, *, engine: registered.append(settings),
    )
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        ["serve", "status", "--engine", "acp", "--project", "demo"],
        input="{}",
    )
    assert result.exit_code == 2
    assert "RECEIVER_ENDPOINT_UNAVAILABLE" in result.output
    assert registered == [cfg]


def test_cli_serve_inspect_success(monkeypatch):
    import json
    import sys

    from untether.session_migration.settings import ReceiverSettings
    from untether.session_migration.ssh_transport import _REQUIRED_CHECKS

    health_argv = (
        sys.executable,
        "-c",
        "import json,sys;json.dump({k: True for k in "
        + repr(list(_REQUIRED_CHECKS))
        + "},sys.stdout)",
    )
    cfg = SessionMigrationSettings(
        enabled=True,
        peers={"vps": profile()},
        receiver=ReceiverSettings(
            endpoint_id="fixture",
            engine="acp",
            project="demo",
            state_dir="C:/tmp/fixture-state",
            destination_binding="enrolled",
            health_argv=health_argv,
        ),
    )
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda *a: (SimpleNamespace(session_migration=cfg), None),
    )
    monkeypatch.setattr(
        migration,
        "register_from_settings",
        lambda settings, *, engine: None,
    )
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        ["serve", "inspect", "--engine", "acp", "--project", "demo"],
        input="",
    )
    assert result.exit_code == 0, result.output
    response = json.loads(result.output)
    assert response["protocol"] == 1
    assert response["endpoint_id"] == "fixture"
    assert response["project"] == "demo"
    assert response["execution_mode"] == "native"
    assert all(response["checks"].get(k) is True for k in _REQUIRED_CHECKS)


def test_cli_serve_forwards_config_path_to_load_settings(monkeypatch, tmp_path):
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": profile()})
    config_path = tmp_path / "untether.toml"
    config_path.write_text("", encoding="utf-8")
    seen = []
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda *args: (
            seen.append(args),
            (SimpleNamespace(session_migration=cfg), config_path),
        )[1],
    )
    monkeypatch.setattr(
        migration,
        "register_from_settings",
        lambda settings, *, engine: None,
    )
    result = CliRunner().invoke(
        migration.build_migrate_app(),
        [
            "serve",
            "status",
            "--engine",
            "acp",
            "--project",
            "demo",
            "--config",
            str(config_path),
        ],
        input="{}",
    )
    assert result.exit_code == 2
    assert "RECEIVER_ENDPOINT_UNAVAILABLE" in result.output
    assert seen == [(config_path,)]
