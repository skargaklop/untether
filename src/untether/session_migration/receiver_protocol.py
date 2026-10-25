"""Fixed receiver protocol, intentionally separate from privilege-bearing wrappers.

Operators bind this library to an installed forced-command SSH account or a
container wrapper that exposes the same three fixed verbs. Without fresh
verified health and a real importer, it refuses; no static green responses.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from .package import ContextPackage
from .receiver import StagingReceiver
from .ssh_transport import _REQUIRED_CHECKS

MAX_HEADER = 4096


class ReceiverProtocolError(ValueError):
    pass


@dataclass(frozen=True)
class ReceiverEndpoint:
    endpoint_id: str
    execution_mode: str
    destination_binding: str
    project: str
    deployment: str
    arch: str
    receiver: StagingReceiver
    probe_health: Callable[[], dict[str, bool]]
    import_and_probe: Callable[[ContextPackage], str] | None = None


def _health(endpoint: ReceiverEndpoint) -> dict[str, bool]:
    if endpoint.execution_mode not in ("native", "container-wrapper"):
        raise ReceiverProtocolError("EXECUTION_MODE_INVALID")
    checks = endpoint.probe_health()
    if not isinstance(checks, dict):
        raise ReceiverProtocolError("HEALTH_PROBE_INVALID")
    required = list(_REQUIRED_CHECKS)
    if endpoint.execution_mode == "container-wrapper":
        required.extend(("container_ready", "container_uid", "mount_mapping"))
    for key in required:
        if checks.get(key) is not True:
            raise ReceiverProtocolError("HEALTH_FAILED: " + key)
    return checks


def handle_receiver(
    endpoint: ReceiverEndpoint,
    *,
    verb: str,
    payload: bytes = b"",
) -> bytes:
    """Validate and return a bounded JSON response; caller controls SSH identity."""
    if verb not in ("inspect", "status", "receive"):
        raise ReceiverProtocolError("ACTION_FORBIDDEN")
    checks = _health(endpoint)
    if verb == "inspect":
        if payload:
            raise ReceiverProtocolError("INSPECT_HAS_DATA")
        result: dict[str, Any] = {
            "protocol": 1,
            "endpoint_id": endpoint.endpoint_id,
            "destination_binding": endpoint.destination_binding,
            "execution_mode": endpoint.execution_mode,
            "deployment": endpoint.deployment,
            "os": "linux",
            "arch": endpoint.arch,
            "project": endpoint.project,
            "checks": checks,
        }
    else:
        header, sep, data = payload.partition(b"\n")
        if not sep or len(header) > MAX_HEADER:
            raise ReceiverProtocolError("FRAME_INVALID")
        try:
            info = json.loads(header)
        except (UnicodeError, ValueError) as exc:
            raise ReceiverProtocolError("FRAME_INVALID") from exc
        if not isinstance(info, dict) or set(info) != {
            "protocol",
            "action",
            "operation_id",
            "project",
            "size",
            "sha256",
        }:
            raise ReceiverProtocolError("FRAME_SCHEMA_INVALID")
        op = info["operation_id"]
        try:
            if not isinstance(op, str) or str(UUID(op)) != op:
                raise ValueError
        except ValueError as exc:
            raise ReceiverProtocolError("OPERATION_ID_INVALID") from exc
        if (
            info["protocol"] != 1
            or info["action"] != verb
            or info["project"] != endpoint.project
        ):
            raise ReceiverProtocolError("FRAME_METADATA_MISMATCH")
        if (
            type(info["size"]) is not int
            or len(data) != info["size"]
            or len(data) > endpoint.receiver.max_archive
            or hashlib.sha256(data).hexdigest() != info["sha256"]
        ):
            raise ReceiverProtocolError("FRAME_HASH_OR_SIZE_MISMATCH")
        if verb == "status":
            if data:
                raise ReceiverProtocolError("STATUS_HAS_PACKAGE")
            receipt = endpoint.receiver.receipts.read(op)
            if receipt is None:
                raise ReceiverProtocolError("NEEDS_RECONCILE: no verified receipt")
        else:
            if endpoint.import_and_probe is None:
                raise ReceiverProtocolError("ADAPTER_UNAVAILABLE")
            receipt = endpoint.receiver.receive(
                op,
                data,
                expected_project=endpoint.project,
                import_and_probe=endpoint.import_and_probe,
            )
        result = {
            "protocol": 1,
            "endpoint_id": endpoint.endpoint_id,
            "operation_id": op,
            "package_sha256": receipt.package_sha256,
            "importer_version": receipt.importer_version,
            "native_id": receipt.native_id,
            "status": receipt.status,
        }
    output = json.dumps(result, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(output) > 16384:
        raise ReceiverProtocolError("RESPONSE_TOO_LARGE")
    return output
