"""Isolated fake SSH endpoint tests; NO live SSH connection is made."""
from __future__ import annotations

import json
import subprocess

import pytest

from untether.session_migration.settings import MigrationPeer
from untether.session_migration.ssh_transport import PreflightError, inspect_peer

FINGERPRINT = "SHA256:" + "A" * 43
CHECKS = (
    "installed_receiver", "runtime_ready", "agent_installed",
    "agent_version_compatible", "adapter_capable", "provider_auth",
    "provider_connectivity", "staging_writable", "disk_headroom",
    "inodes_available", "workspace_git_parity", "quiescent",
    "telegram_enrolled", "distinct_bot", "destination_binding",
    "resume_probe_available", "container_ready", "container_uid", "mount_mapping",
)


def peer(tmp_path, mode="native"):
    keys = tmp_path / "keys"
    keys.mkdir()
    (keys / "selected").write_text("fixture key file only")
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("fake host public key fixture")
    return MigrationPeer(
        host="example.invalid",
        port=2222,
        user="operator",
        identity_dir=str(keys),
        identity_file="selected",
        known_hosts_file=str(known_hosts),
        host_key_fingerprint=FINGERPRINT,
        receiver_path="/usr/local/bin/untether-migration-receiver",
        execution_mode=mode,
        expected_endpoint_id="endpoint-one",
        destination_binding="enrolled-one",
        projects={"demo": "/srv/demo"},
    )


def response(mode, *, deployment, checks=None):
    return {
        "protocol": 1,
        "endpoint_id": "endpoint-one",
        "execution_mode": mode,
        "deployment": deployment,
        "os": "linux",
        "arch": "x86_64",
        "checks": dict.fromkeys(CHECKS, True) if checks is None else checks,
        "destination_binding": "enrolled-one",
        "project": "demo",
    }


@pytest.mark.parametrize(
    ("mode", "deployment"),
    [
        ("native", "direct-linux"),
        ("container-wrapper", "podman-or-docker"),
        ("container-wrapper", "coolify-managed"),
    ],
)
@pytest.mark.parametrize("direction", ["to", "from"])
def test_same_ssh_protocol_both_directions_and_deployment_labels(
    tmp_path, mode, deployment, direction
):
    profile = peer(tmp_path, mode=mode)
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        if argv[0] == "ssh-keygen":
            return subprocess.CompletedProcess(argv, 0, FINGERPRINT, "")
        if argv[0] == "ssh":
            return subprocess.CompletedProcess(
                argv, 0, json.dumps(response(mode, deployment=deployment)), ""
            )
        raise AssertionError(f"Unexpected binary: {argv[0]}")

    result = inspect_peer(profile, project="demo", direction=direction, run=fake_run)
    assert result["deployment"] == deployment
    assert result["execution_mode"] == mode
    assert all(not opts.get("shell", False) for _, opts in calls)
    ssh_argv = calls[-1][0]
    assert "-T" in ssh_argv
    assert any("StrictHostKeyChecking=yes" in arg for arg in ssh_argv)
    assert ssh_argv[-1].split()[-1] == "inspect"


def test_changed_host_fingerprint_refused_before_ssh(tmp_path):
    p = peer(tmp_path)
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "SHA256:" + "B" * 43, "")

    with pytest.raises(PreflightError, match="HOST_KEY_PIN_MISMATCH"):
        inspect_peer(p, project="demo", direction="to", run=fake_run)
    assert len(calls) == 1


def test_symlinked_private_identity_refused(tmp_path, monkeypatch):
    p = peer(tmp_path)
    key = tmp_path / "keys" / "selected"
    key.unlink()
    key.symlink_to(tmp_path / "known_hosts")
    with pytest.raises(PreflightError, match="IDENTITY_UNSAFE"):
        inspect_peer(p, project="demo", direction="to", run=lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0, "", ""))


def test_missing_capability_fails_closed(tmp_path):
    p = peer(tmp_path)
    checks = {k: True for k in CHECKS}
    checks["provider_auth"] = False

    def fake_run(argv, **kwargs):
        stdout = FINGERPRINT if argv[0] == "ssh-keygen" else json.dumps(
            response("native", deployment="direct-linux", checks=checks)
        )
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    with pytest.raises(PreflightError, match="provider_auth"):
        inspect_peer(p, project="demo", direction="to", run=fake_run)


def test_endpoint_identity_conflict_rejected(tmp_path):
    p = peer(tmp_path)

    def fake_run(argv, **kwargs):
        data = response("native", deployment="direct-linux")
        data["endpoint_id"] = "other-endpoint"
        return subprocess.CompletedProcess(
            argv, 0, FINGERPRINT if argv[0] == "ssh-keygen" else json.dumps(data), ""
        )

    with pytest.raises(PreflightError, match="ENDPOINT_MISMATCH"):
        inspect_peer(p, project="demo", direction="from", run=fake_run)


def test_receiver_transport_error_redacts_output(tmp_path):
    p = peer(tmp_path)

    def fake_run(argv, **kwargs):
        if argv[0] == "ssh-keygen":
            return subprocess.CompletedProcess(argv, 0, FINGERPRINT, "")
        return subprocess.CompletedProcess(argv, 255, "", "private secret and host")

    with pytest.raises(PreflightError, match="SSH_FAILED") as exc:
        inspect_peer(p, project="demo", direction="to", run=fake_run)
    assert "private secret" not in str(exc.value)


def test_unrecognised_project_cannot_reach_ssh(tmp_path):
    p = peer(tmp_path)
    with pytest.raises(PreflightError, match="PROJECT_UNMAPPED"):
        inspect_peer(p, project="unknown", direction="to", run=lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0, "", ""))
