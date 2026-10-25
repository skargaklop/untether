"""SSH receiver RPC uses a fixed verb and rejects unverified replies."""
from __future__ import annotations

import hashlib
import json
import subprocess
from uuid import uuid4

import pytest

from untether.session_migration.remote import receiver_request
from untether.session_migration.settings import MigrationPeer
from untether.session_migration.ssh_transport import PreflightError

PIN = "SHA256:" + "A" * 43
CHECKS = [
    "installed_receiver", "runtime_ready", "agent_installed",
    "agent_version_compatible", "adapter_capable", "provider_auth",
    "provider_connectivity", "staging_writable", "disk_headroom",
    "inodes_available", "workspace_git_parity", "quiescent",
    "telegram_enrolled", "distinct_bot", "destination_binding",
    "resume_probe_available", "container_ready", "container_uid", "mount_mapping",
]


def fixture_peer(tmp_path, mode):
    keys = tmp_path / "keys"
    keys.mkdir()
    (keys / "selected").write_text("test-only")
    known = tmp_path / "known"
    known.write_text("key")
    return MigrationPeer(
        host="example.invalid", user="operator",
        identity_dir=str(keys), identity_file="selected",
        known_hosts_file=str(known), host_key_fingerprint=PIN,
        receiver_path="/usr/local/bin/fixed-receiver", execution_mode=mode,
        expected_endpoint_id="endpoint-one", destination_binding="enrolled",
        projects={"demo": "/srv/demo"},
    )


@pytest.mark.parametrize("direction", ["to", "from"])
@pytest.mark.parametrize("mode", ["native", "container-wrapper"])
def test_binary_receiver_both_directions(tmp_path, mode, direction):
    peer = fixture_peer(tmp_path, mode)
    op = str(uuid4())
    archive = b"opaque context zip bytes"
    digest = hashlib.sha256(archive).hexdigest()
    transport_calls = []
    def fake_preflight(argv, **_kwargs):
        if argv[0] == "ssh-keygen":
            return subprocess.CompletedProcess(argv, 0, PIN, "")
        return subprocess.CompletedProcess(argv, 0, json.dumps({
            "protocol": 1, "endpoint_id": "endpoint-one",
            "execution_mode": mode, "destination_binding": "enrolled",
            "os": "linux", "arch": "x86_64", "project": "demo",
            "checks": dict.fromkeys(CHECKS, True),
        }), "")
    def fake_transport(argv, **kwargs):
        transport_calls.append((argv, kwargs["input"]))
        return subprocess.CompletedProcess(argv, 0, json.dumps({
            "protocol": 1, "operation_id": op,
            "endpoint_id": "endpoint-one", "package_sha256": digest,
            "status": "verified", "native_id": "fresh-destination-id",
            "importer_version": "1",
        }).encode(), b"")
    result = receiver_request(
        peer, direction=direction, project="demo", action="receive",
        operation_id=op, package=archive, run=fake_transport,
        preflight_run=fake_preflight,
    )
    assert result["native_id"] == "fresh-destination-id"
    args, payload = transport_calls[0]
    assert args[-1] == "/usr/local/bin/fixed-receiver receive"
    header, _, rest = payload.partition(b"\n")
    assert json.loads(header)["sha256"] == digest
    assert rest == archive
    assert "BatchMode=yes" in args
    assert "StrictHostKeyChecking=yes" in args


def test_reject_unverified_receipt(tmp_path):
    peer = fixture_peer(tmp_path, "native")
    with pytest.raises(PreflightError, match="RECEIVER_ACTION_FORBIDDEN"):
        receiver_request(
            peer, project="demo", direction="to",
            action="sh -c whoami", operation_id=str(uuid4()),
        )
