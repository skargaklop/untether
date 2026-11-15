"""Schema contracts for SSH migration profiles (no real home or network access)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from untether.session_migration.settings import MigrationPeer, SessionMigrationSettings

FINGERPRINT = "SHA256:" + "A" * 43


def peer_data(**changes):
    data = {
        "host": "example.invalid",
        "port": 2222,
        "user": "operator",
        "identity_dir": "/tmp/ssh-keys",
        "identity_file": "selected_ed25519",
        "known_hosts_file": "/tmp/known-hosts",
        "host_key_fingerprint": FINGERPRINT,
        "receiver_path": "/opt/untether/bin/untether",
        "expected_endpoint_id": "remote-test",
        "destination_binding": "enrolled-test",
        "projects": {"demo": "/srv/demo"},
    }
    return data | changes


@pytest.mark.parametrize("mode", ["native", "container-wrapper"])
def test_execution_modes_share_schema(mode):
    peer = MigrationPeer.model_validate(peer_data(execution_mode=mode))
    assert peer.execution_mode == mode
    assert peer.projects["demo"] == "/srv/demo"


def test_native_is_default_and_disabled():
    assert MigrationPeer.model_validate(peer_data()).execution_mode == "native"
    assert SessionMigrationSettings().enabled is False


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("receiver_path", "docker exec -it root sh"),
        ("receiver_path", "/opt/untether;curl attacker"),
        ("receiver_path", "/opt/../bin/untether"),
        ("identity_file", "../private-key"),
        ("identity_file", "/tmp/private-key"),
        ("host", "-oProxyCommand=evil"),
        ("host", "a;id"),
        ("user", "operator@elsewhere"),
        ("host_key_fingerprint", "SHA256:REQUIRED"),
        ("projects", {"demo": "../../tmp"}),
        ("projects", {"demo": "/srv/../secrets"}),
        ("execution_mode", "coolify"),
    ],
)
def test_rejects_unsafe_peer_fields(key, value):
    with pytest.raises(ValidationError):
        MigrationPeer.model_validate(peer_data(**{key: value}))


def test_unknown_fields_not_silently_ignored():
    with pytest.raises(ValidationError):
        MigrationPeer.model_validate(
            peer_data(extra_ssh_options="-oStrictHostKeyChecking=no")
        )


def test_bounds_enforced():
    with pytest.raises(ValidationError):
        SessionMigrationSettings(max_archive_bytes=0)
    with pytest.raises(ValidationError):
        SessionMigrationSettings(connect_timeout_s=0)


def test_template_can_remain_disabled_without_enabling_network():
    assert SessionMigrationSettings(peers={"vps": peer_data()}).enabled is False
