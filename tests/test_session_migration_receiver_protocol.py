"""Receiver protocol over framed bytes, with no root, Docker or SSH access."""

from __future__ import annotations

import hashlib
import json
from uuid import uuid4

import pytest

from untether.session_migration.package import pack_context
from untether.session_migration.receiver import StagingReceiver
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    ReceiverProtocolError,
    handle_receiver,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS


def make_endpoint(tmp_path, mode):
    receiver = StagingReceiver(
        tmp_path / "state",
        max_archive=8192,
        max_expanded=16384,
        min_free_bytes=1,
    )
    checks = dict.fromkeys(_REQUIRED_CHECKS, True)
    checks.update(container_ready=True, container_uid=True, mount_mapping=True)
    endpoint = ReceiverEndpoint(
        endpoint_id="receiver-1",
        execution_mode=mode,
        destination_binding="enrolled",
        project="demo",
        deployment="direct-linux" if mode == "native" else "container",
        arch="x86_64",
        receiver=receiver,
        probe_health=lambda: checks,
        import_and_probe=lambda _pkg: "new-session",
    )
    return endpoint, checks


def frame(operation_id, archive, action):
    return (
        json.dumps(
            {
                "protocol": 1,
                "action": action,
                "operation_id": operation_id,
                "project": "demo",
                "size": len(archive),
                "sha256": hashlib.sha256(archive).hexdigest(),
            }
        ).encode()
        + b"\n"
        + archive
    )


@pytest.mark.parametrize("mode", ["native", "container-wrapper"])
@pytest.mark.parametrize("direction", ["to", "from"])
def test_receiver_same_protocol_for_portable_deployments(tmp_path, mode, direction):
    endpoint, _checks = make_endpoint(tmp_path / direction, mode)
    health = json.loads(handle_receiver(endpoint, verb="inspect"))
    assert health["execution_mode"] == mode
    archive = pack_context(engine="acp", project="demo", content=b"x", max_bytes=8192)
    op = str(uuid4())
    received = json.loads(
        handle_receiver(
            endpoint,
            verb="receive",
            payload=frame(op, archive, "receive"),
        )
    )
    assert received["status"] == "verified"
    assert received["native_id"] == "new-session"
    status = json.loads(
        handle_receiver(
            endpoint,
            verb="status",
            payload=frame(op, b"", "status"),
        )
    )
    assert status["status"] == "verified"


def test_fail_closed_if_provider_not_healthy(tmp_path):
    endpoint, checks = make_endpoint(tmp_path, "native")
    checks["provider_connectivity"] = False
    with pytest.raises(ReceiverProtocolError, match="provider_connectivity"):
        handle_receiver(endpoint, verb="inspect")


def test_reject_archive_tampering(tmp_path):
    endpoint, _checks = make_endpoint(tmp_path, "native")
    op = str(uuid4())
    archive = pack_context(engine="acp", project="demo", content=b"x", max_bytes=8192)
    data = frame(op, archive, "receive")
    with pytest.raises(ReceiverProtocolError, match="FRAME_HASH_OR_SIZE_MISMATCH"):
        handle_receiver(endpoint, verb="receive", payload=data[:-1] + b"z")


def test_refuses_when_no_proven_importer(tmp_path):
    endpoint, _checks = make_endpoint(tmp_path, "native")
    endpoint = ReceiverEndpoint(
        endpoint_id=endpoint.endpoint_id,
        execution_mode=endpoint.execution_mode,
        destination_binding=endpoint.destination_binding,
        project=endpoint.project,
        deployment=endpoint.deployment,
        arch=endpoint.arch,
        receiver=endpoint.receiver,
        probe_health=endpoint.probe_health,
        import_and_probe=None,
    )
    blob = pack_context(engine="acp", project="demo", content=b"x", max_bytes=8192)
    with pytest.raises(ReceiverProtocolError, match="ADAPTER_UNAVAILABLE"):
        handle_receiver(
            endpoint, verb="receive", payload=frame(str(uuid4()), blob, "receive")
        )
