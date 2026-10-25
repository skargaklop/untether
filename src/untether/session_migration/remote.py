"""Versioned fixed-verb SSH receiver RPC; never accepts shell commands from chat."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Callable
from typing import Any

from .settings import MigrationPeer
from .ssh_transport import PreflightError, _identity, inspect_peer

_ALLOWED_ACTIONS = frozenset({"status", "receive"})


def receiver_request(
    peer: MigrationPeer,
    *,
    project: str,
    direction: str,
    action: str,
    operation_id: str,
    package: bytes = b"",
    max_package_bytes: int = 67108864,
    timeout_s: float = 30,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
    preflight_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Fresh preflight, then exact receiver verb and bounded binary framed request.

    The remote receiver must be an operator-installed restricted executable or
    forced-command wrapper. No CLI arguments originate in untrusted chat text.
    """
    if action not in _ALLOWED_ACTIONS:
        raise PreflightError("RECEIVER_ACTION_FORBIDDEN")
    if not 0 <= len(package) <= max_package_bytes or (action == "status" and package):
        raise PreflightError("PACKAGE_BOUNDARY")
    from uuid import UUID

    try:
        if str(UUID(operation_id)) != operation_id:
            raise ValueError
    except ValueError as exc:
        raise PreflightError("OPERATION_ID_INVALID") from exc

    # Fresh full availability check on every operation, including resumption.
    inspect_peer(
        peer,
        project=project,
        direction=direction,
        timeout_s=timeout_s,
        run=preflight_run,
    )
    identity, known = _identity(peer)
    digest = hashlib.sha256(package).hexdigest()
    header = json.dumps(
        {
            "protocol": 1,
            "action": action,
            "operation_id": operation_id,
            "project": project,
            "size": len(package),
            "sha256": digest,
        },
        separators=(",", ":"),
    ).encode("ascii") + b"\n"
    if len(header) > 4096:
        raise PreflightError("FRAME_TOO_LARGE")
    target = f"{peer.user}@{peer.host}"
    command = peer.receiver_path + " " + action
    argv = [
        "ssh", "-T", "-F", os.devnull, "-p", str(peer.port),
        "-i", str(identity), "-o", "BatchMode=yes",
        "-o", "NumberOfPasswordPrompts=0", "-o", "StrictHostKeyChecking=yes",
        "-o", "UpdateHostKeys=no", "-o", f"UserKnownHostsFile={known}",
        "-o", "GlobalKnownHostsFile=none", "-o", "IdentitiesOnly=yes",
        "-o", "ForwardAgent=no", "-o", "ForwardX11=no",
        "-o", "ClearAllForwardings=yes", "-o", "PermitLocalCommand=no",
        "-o", "ControlMaster=no", "-o", f"ConnectTimeout={max(1, int(timeout_s))}",
        "--", target, command,
    ]
    try:
        result = run(
            argv, input=header + package,
            capture_output=True, timeout=timeout_s + 5, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PreflightError("RECEIVER_UNREACHABLE") from exc
    if result.returncode != 0:
        raise PreflightError("RECEIVER_REFUSED")
    if len(result.stdout) > 16384:
        raise PreflightError("RECEIVER_REPLY_OVERSIZE")
    try:
        reply = json.loads(result.stdout)
    except (UnicodeError, ValueError) as exc:
        raise PreflightError("RECEIVER_REPLY_MALFORMED") from exc
    if not isinstance(reply, dict) or reply.get("protocol") != 1:
        raise PreflightError("RECEIVER_PROTOCOL_MISMATCH")
    if reply.get("operation_id") != operation_id:
        raise PreflightError("RECEIVER_OPERATION_MISMATCH")
    if reply.get("endpoint_id") != peer.expected_endpoint_id:
        raise PreflightError("RECEIVER_ENDPOINT_MISMATCH")
    if reply.get("package_sha256") != digest:
        raise PreflightError("RECEIVER_PACKAGE_MISMATCH")
    if action == "receive":
        if (
            reply.get("status") != "verified"
            or not isinstance(reply.get("native_id"), str)
            or not reply["native_id"]
            or not isinstance(reply.get("importer_version"), str)
            or not reply["importer_version"]
        ):
            raise PreflightError("RECEIVER_NOT_VERIFIED")
    return reply
