"""Actual isolated subprocess transport fixtures, with no network access."""

from __future__ import annotations

import json
import stat
import subprocess
import sys

import pytest

from untether.session_migration.settings import MigrationPeer
from untether.session_migration.ssh_transport import PreflightError, inspect_peer

PIN = "SHA256:" + "A" * 43
BASE_CHECKS = [
    "installed_receiver",
    "runtime_ready",
    "agent_installed",
    "agent_version_compatible",
    "adapter_capable",
    "provider_auth",
    "provider_connectivity",
    "staging_writable",
    "disk_headroom",
    "inodes_available",
    "workspace_git_parity",
    "quiescent",
    "telegram_enrolled",
    "distinct_bot",
    "destination_binding",
    "resume_probe_available",
    "container_ready",
    "container_uid",
    "mount_mapping",
]


@pytest.fixture
def subprocess_endpoints(tmp_path, monkeypatch):
    """Executable shell-free Python fakes that receive real subprocess arguments."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    for executable in ("ssh", "ssh-keygen"):
        script = bin_dir / executable
        script.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "from pathlib import Path\n"
            "Path(os.environ['MIGRATION_ARGV_LOG']).open('a').write("
            "json.dumps([os.path.basename(sys.argv[0]), *sys.argv[1:]])+'\\n')\n"
            "if os.path.basename(sys.argv[0]) == 'ssh-keygen':\n"
            " print(os.environ['MIGRATION_FAKE_PIN'])\n"
            "else:\n"
            " print(os.environ['MIGRATION_FAKE_RESPONSE'])\n"
        )
        script.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    # Windows does not invoke extensionless shebang scripts through subprocess.\n    # Execute each fake under the current Python interpreter on all platforms.
    monkeypatch.setenv("MIGRATION_FAKE_PIN", PIN)
    monkeypatch.setenv("MIGRATION_ARGV_LOG", str(tmp_path / "argv.jsonl"))
    keys = tmp_path / "keys"
    keys.mkdir()
    (keys / "selected").write_text("NOT-A-PRIVATE-KEY")
    known = tmp_path / "known"
    known.write_text("fixture host pin")
    peer = MigrationPeer(
        host="example.invalid",
        port=2222,
        user="operator",
        identity_dir=str(keys),
        identity_file="selected",
        known_hosts_file=str(known),
        host_key_fingerprint=PIN,
        receiver_path="/opt/untether/bin/receiver",
        expected_endpoint_id="destination",
        destination_binding="pre-enrolled",
        projects={"project": "/srv/project"},
    )

    def run_fake(argv, **kwargs):
        executable = bin_dir / argv[0]
        return subprocess.run([sys.executable, str(executable), *argv[1:]], **kwargs)

    return peer, tmp_path / "argv.jsonl", run_fake


@pytest.mark.parametrize(
    ("execution_mode", "deployment"),
    [
        ("native", "native-linux"),
        ("container-wrapper", "docker"),
        ("container-wrapper", "coolify"),
        ("container-wrapper", "podman"),
    ],
)
@pytest.mark.parametrize("direction", ["to", "from"])
def test_real_subprocess_fixture(
    subprocess_endpoints, monkeypatch, execution_mode, deployment, direction
):
    peer, log, run_fake = subprocess_endpoints
    peer = peer.model_copy(update={"execution_mode": execution_mode})
    reply = {
        "protocol": 1,
        "os": "linux",
        "arch": "x86_64",
        "endpoint_id": "destination",
        "destination_binding": "pre-enrolled",
        "project": "project",
        "execution_mode": execution_mode,
        "deployment": deployment,
        "checks": dict.fromkeys(BASE_CHECKS, True),
    }
    monkeypatch.setenv("MIGRATION_FAKE_RESPONSE", json.dumps(reply))
    result = inspect_peer(peer, project="project", direction=direction, run=run_fake)
    assert result["deployment"] == deployment
    calls = [json.loads(s) for s in log.read_text().splitlines()]
    assert [c[0] for c in calls] == ["ssh-keygen", "ssh"]
    assert calls[-1][-1] == "/opt/untether/bin/receiver inspect"
    assert "StrictHostKeyChecking=yes" in calls[-1]
    assert "BatchMode=yes" in calls[-1]


def test_receiver_refuses_unauthenticated_provider(subprocess_endpoints, monkeypatch):
    peer, _, run_fake = subprocess_endpoints
    checks = dict.fromkeys(BASE_CHECKS, True)
    checks["provider_auth"] = False
    monkeypatch.setenv(
        "MIGRATION_FAKE_RESPONSE",
        json.dumps(
            {
                "protocol": 1,
                "os": "linux",
                "arch": "x86_64",
                "endpoint_id": "destination",
                "destination_binding": "pre-enrolled",
                "project": "project",
                "execution_mode": "native",
                "checks": checks,
            }
        ),
    )
    with pytest.raises(PreflightError, match="provider_auth"):
        inspect_peer(peer, project="project", direction="to", run=run_fake)
