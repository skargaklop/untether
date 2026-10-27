"""Fixed receiver protocol, intentionally separate from privilege-bearing wrappers.

Operators bind this library to an installed forced-command SSH account or a
container wrapper that exposes the same three fixed verbs. Without fresh
verified health and a real importer, it refuses; no static green responses.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any
from uuid import UUID

from .native_adapter import require_persistence_adapter
from .native_archive import _safe_name
from .native_receiver import NativeReceiver
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
    probe_health: Callable[[], dict[str, bool]]
    engine: str = ""
    receiver: StagingReceiver | None = None
    import_and_probe: Callable[[ContextPackage], str] | None = None
    native_receiver: NativeReceiver | None = None
    project_root: str | None = None
    authorized_peer: str | None = None

    @property
    def max_archive(self) -> int:
        guard = self.native_receiver or self.receiver
        if guard is None:
            raise ReceiverProtocolError("ADAPTER_UNAVAILABLE")
        return guard.max_archive

    def status_receipt(self, operation_id: str):
        if self.native_receiver is not None:
            return self.native_receiver.status_receipt(operation_id)
        if self.receiver is not None:
            return self.receiver.receipts.read(operation_id)
        return None


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


def _inspect_session(endpoint: ReceiverEndpoint, payload: bytes) -> dict[str, Any]:
    """Read only through the configured adapter's public project-scoped lookup."""
    try:
        if len(payload) > MAX_HEADER or not payload.endswith(b"\n"):
            raise ValueError
        header, _, body = payload.partition(b"\n")
        if body:
            raise ValueError
        text = header.decode("utf-8")
        if not text.lstrip().startswith("{"):
            raise ValueError
        pairs = json.loads(text, object_pairs_hook=list)
        if not isinstance(pairs, list):
            raise ValueError
        info = dict(pairs)
        if len(info) != len(pairs) or set(info) != {
            "protocol",
            "action",
            "operation_id",
            "project",
            "session_id",
        }:
            raise ValueError
        op = info["operation_id"]
        sid = info["session_id"]
        if (
            type(info["protocol"]) is not int
            or info["protocol"] != 1
            or info["action"] != "inspect"
            or info["project"] != endpoint.project
            or not isinstance(op, str)
            or str(UUID(op)) != op
            or not isinstance(sid, str)
            or not 1 <= len(sid) <= 128
            or not _safe_name(sid)
            or "/" in sid
            or PureWindowsPath(sid).name != sid
            or any(ord(char) == 127 for char in sid)
            or endpoint.native_receiver is None
            or not endpoint.engine
            or not endpoint.project_root
        ):
            raise ValueError
        root = PureWindowsPath(endpoint.project_root)
        project_root = root if root.drive else PurePosixPath(endpoint.project_root)
        adapter = require_persistence_adapter(endpoint.engine)
        lookup = getattr(adapter, "lookup_for_project", None)
        if not callable(lookup) or lookup(sid, project_root) != sid:
            raise ValueError
    except Exception:  # noqa: BLE001 -- redact untrusted plugin failures at protocol boundary
        # Plugin errors can contain paths or session content; never echo them.
        raise ReceiverProtocolError("SESSION_PROJECT_REFUSED") from None
    return {
        "protocol": 1,
        "endpoint_id": endpoint.endpoint_id,
        "operation_id": op,
        "project": endpoint.project,
        "session_id": sid,
        "engine": endpoint.engine,
        "status": "found",
    }


def handle_receiver(
    endpoint: ReceiverEndpoint,
    *,
    verb: str,
    payload: bytes = b"",
) -> bytes:
    """Validate and return a bounded JSON response; caller controls SSH identity."""
    if verb not in ("inspect", "status", "receive", "export", "export-complete"):
        raise ReceiverProtocolError("ACTION_FORBIDDEN")
    checks = _health(endpoint)
    if verb == "inspect" and payload:
        result = _inspect_session(endpoint, payload)
    elif verb == "inspect":
        result = {
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
        if not isinstance(info, dict) or set(info) not in (
            {"protocol", "action", "operation_id", "project", "size", "sha256"},
            {
                "protocol",
                "action",
                "operation_id",
                "project",
                "size",
                "sha256",
                "engine",
            },
            {
                "protocol",
                "action",
                "operation_id",
                "project",
                "size",
                "sha256",
                "engine",
                "session_id",
            },
        ):
            raise ReceiverProtocolError("FRAME_SCHEMA_INVALID")
        op = info["operation_id"]
        try:
            if not isinstance(op, str) or str(UUID(op)) != op:
                raise ValueError
        except ValueError as exc:
            raise ReceiverProtocolError("OPERATION_ID_INVALID") from exc
        protocol = info["protocol"]
        if protocol not in (1, 2) or info["action"] != verb:
            raise ReceiverProtocolError("FRAME_METADATA_MISMATCH")
        if info["project"] != endpoint.project:
            raise ReceiverProtocolError("FRAME_METADATA_MISMATCH")
        if (
            type(info["size"]) is not int
            or len(data) != info["size"]
            or len(data) > endpoint.max_archive
            or hashlib.sha256(data).hexdigest() != info["sha256"]
        ):
            raise ReceiverProtocolError("FRAME_HASH_OR_SIZE_MISMATCH")
        if verb == "status":
            if data:
                raise ReceiverProtocolError("STATUS_HAS_PACKAGE")
            receipt = endpoint.status_receipt(op)
            if receipt is None:
                raise ReceiverProtocolError("NEEDS_RECONCILE: no verified receipt")
            engine = getattr(receipt, "engine", None)
            result = {
                "protocol": 1,
                "endpoint_id": endpoint.endpoint_id,
                "operation_id": op,
                "package_sha256": receipt.package_sha256,
                "importer_version": receipt.importer_version,
                "native_id": receipt.native_id,
                "status": receipt.status,
            }
            if engine is not None:
                result["engine"] = engine
        elif verb == "export":
            if data:
                raise ReceiverProtocolError("EXPORT_HAS_PACKAGE")
            engine = info.get("engine")
            session_id = info.get("session_id")
            if not isinstance(engine, str) or not engine:
                raise ReceiverProtocolError("ENGINE_REQUIRED")
            if not isinstance(session_id, str) or not session_id:
                raise ReceiverProtocolError("SESSION_ID_REQUIRED")
            if endpoint.engine and endpoint.engine != engine:
                raise ReceiverProtocolError("ENGINE_ENDPOINT_MISMATCH")
            if endpoint.native_receiver is None:
                raise ReceiverProtocolError("ADAPTER_UNAVAILABLE")
            try:
                package = endpoint.native_receiver.export_exact(
                    session_id,
                    engine=engine,
                    operation_id=op,
                    peer_name=endpoint.authorized_peer,
                    project=endpoint.project,
                )
            except Exception as exc:
                raise ReceiverProtocolError(str(exc) or "EXPORT_REFUSED") from exc
            if len(package) > endpoint.max_archive:
                # Reject before base64 so an oversized export can never be
                # encoded into a reply buffer.
                raise ReceiverProtocolError("EXPORT_PACKAGE_TOO_LARGE")
            package_digest = hashlib.sha256(package).hexdigest()
            result = {
                "protocol": 1,
                "endpoint_id": endpoint.endpoint_id,
                "operation_id": op,
                "session_id": session_id,
                "engine": engine,
                "package_sha256": package_digest,
                "package_base64": base64.b64encode(package).decode("ascii"),
                "status": "exported",
            }
        elif verb == "export-complete":
            try:
                if len(data) > 128:
                    raise ValueError
                completion = json.loads(data)
                if not isinstance(completion, dict) or set(completion) != {
                    "package_sha256"
                }:
                    raise ValueError
                claimed_digest = completion["package_sha256"]
                if (
                    not isinstance(claimed_digest, str)
                    or len(claimed_digest) != 64
                    or any(char not in "0123456789abcdef" for char in claimed_digest)
                ):
                    raise ValueError
            except (ValueError, UnicodeError) as exc:
                raise ReceiverProtocolError("COMPLETION_PAYLOAD_INVALID") from exc
            engine = info.get("engine")
            session_id = info.get("session_id")
            if not isinstance(engine, str) or not engine:
                raise ReceiverProtocolError("ENGINE_REQUIRED")
            if not isinstance(session_id, str) or not session_id:
                raise ReceiverProtocolError("SESSION_ID_REQUIRED")
            if endpoint.engine and endpoint.engine != engine:
                raise ReceiverProtocolError("ENGINE_ENDPOINT_MISMATCH")
            if endpoint.native_receiver is None:
                raise ReceiverProtocolError("ADAPTER_UNAVAILABLE")
            try:
                receipt = endpoint.native_receiver.complete_export(
                    op,
                    engine=engine,
                    session_id=session_id,
                    package_sha256=claimed_digest,
                    peer_name=endpoint.authorized_peer,
                    project=endpoint.project,
                )
            except Exception as exc:
                raise ReceiverProtocolError(str(exc) or "COMPLETION_REFUSED") from exc
            digest = (
                receipt.get("package_sha256") if isinstance(receipt, dict) else None
            )
            if digest != claimed_digest:
                raise ReceiverProtocolError("COMPLETION_RECEIPT_INVALID")
            result = {
                "protocol": 1,
                "endpoint_id": endpoint.endpoint_id,
                "operation_id": op,
                "session_id": session_id,
                "engine": engine,
                "package_sha256": digest,
                "status": "export_completed",
            }
        else:
            engine = info.get("engine")
            if protocol == 2:
                if not isinstance(engine, str) or not engine:
                    raise ReceiverProtocolError("ENGINE_REQUIRED")
                if endpoint.engine and endpoint.engine != engine:
                    raise ReceiverProtocolError("ENGINE_ENDPOINT_MISMATCH")
                if endpoint.native_receiver is None:
                    raise ReceiverProtocolError("ADAPTER_UNAVAILABLE")
                try:
                    receipt = endpoint.native_receiver.receive(
                        op,
                        data,
                        engine=engine,
                        peer_name=endpoint.authorized_peer,
                        project=endpoint.project,
                    )
                except Exception as exc:
                    raise ReceiverProtocolError(str(exc) or "RECEIVE_REFUSED") from exc
            else:
                if endpoint.receiver is None or endpoint.import_and_probe is None:
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
            if engine is not None:
                result["engine"] = engine
    output = json.dumps(result, separators=(",", ":"), sort_keys=True).encode("utf-8")
    max_output = endpoint.max_archive * 2 + 16384 if verb == "export" else 16384
    if len(output) > max_output:
        raise ReceiverProtocolError("RESPONSE_TOO_LARGE")
    return output
