"""Versioned fixed-verb SSH receiver RPC; never accepts shell commands from chat."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import re
import subprocess
import threading
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from .settings import MigrationPeer
from .ssh_transport import PreflightError, _identity, inspect_peer

_ALLOWED_ACTIONS = frozenset(
    {"status", "receive", "export", "export-complete", "inspect"}
)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate reply field")
        result[key] = value
    return result


def inspect_session(
    peer: MigrationPeer,
    project: str,
    session_id: str,
    *,
    timeout_s: float = 30,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
    preflight_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    """Return only the engine proven by the pinned remote session lookup."""
    reply = receiver_request(
        peer,
        project=project,
        direction="from",
        action="inspect",
        operation_id=str(uuid4()),
        session_id=session_id,
        timeout_s=timeout_s,
        run=run,
        preflight_run=preflight_run,
    )
    return reply["engine"]


_POLL_INTERVAL_S = 0.05
_REAP_TIMEOUT_S = 5.0


class _ReplyOverflow(Exception):
    """Child stdout exceeded the derived reply cap."""


def _bounded_run(
    argv: list[str], *, payload: bytes, timeout_s: float, stdout_cap: int
) -> tuple[int, bytes]:
    """Run argv draining stdout through a bounded concurrent reader.

    stdin is written by a helper thread so a child that never reads input
    cannot deadlock the caller. The reader stops after stdout_cap + 1 bytes.
    stdin write, stdout read, and process exit share one monotonic deadline:
    overflow kills the child immediately, otherwise the exit wait uses the
    remaining budget and the child is killed and reaped once it is spent;
    stderr is discarded. Returns (returncode, stdout).
    """
    proc = subprocess.Popen(
        argv,
        shell=False,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    stdin, stdout_stream = proc.stdin, proc.stdout
    assert stdin is not None and stdout_stream is not None
    out: list[bytes] = [b""]
    read_done = threading.Event()

    def _reader() -> None:
        try:
            out[0] = stdout_stream.read(stdout_cap + 1) or b""
        except (OSError, ValueError):
            pass
        finally:
            read_done.set()

    def _writer() -> None:
        try:
            stdin.write(payload)
            stdin.close()
        except (OSError, ValueError):
            pass

    reader = threading.Thread(target=_reader, daemon=True)
    writer = threading.Thread(target=_writer, daemon=True)
    reader.start()
    writer.start()

    deadline = time.monotonic() + timeout_s
    try:
        while not read_done.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout_s)
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=min(remaining, _POLL_INTERVAL_S))
        stdout = out[0]
        overflow = len(stdout) > stdout_cap
        remaining = deadline - time.monotonic()
        if not overflow and remaining <= 0:
            raise subprocess.TimeoutExpired(argv, timeout_s)
        writer.join(min(remaining, _REAP_TIMEOUT_S))
        if proc.poll() is None:
            if not overflow:
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proc.wait(timeout=max(0.0, deadline - time.monotonic()))
            if proc.poll() is None:
                proc.kill()
                if not overflow:
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        proc.wait(timeout=_REAP_TIMEOUT_S)
                    raise subprocess.TimeoutExpired(argv, timeout_s)
        if overflow:
            raise _ReplyOverflow()
        return proc.returncode, stdout
    finally:
        if proc.poll() is None:
            proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=_REAP_TIMEOUT_S)
        if not read_done.is_set():
            read_done.wait(_REAP_TIMEOUT_S)
        for stream in (stdin, stdout_stream):
            if stream is not None:
                with contextlib.suppress(OSError, ValueError):
                    stream.close()


def receiver_request(
    peer: MigrationPeer,
    *,
    project: str,
    direction: str,
    action: str,
    operation_id: str,
    package: bytes = b"",
    engine: str | None = None,
    session_id: str | None = None,
    max_package_bytes: int = 67108864,
    timeout_s: float = 30,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
    preflight_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    completion_sha256: str | None = None,
) -> dict[str, Any]:
    """Fresh preflight, then exact receiver verb and bounded binary framed request.

    The remote receiver must be an operator-installed restricted executable or
    forced-command wrapper. No CLI arguments originate in untrusted chat text.
    """
    if action not in _ALLOWED_ACTIONS:
        raise PreflightError("RECEIVER_ACTION_FORBIDDEN")
    if action == "inspect":
        if not isinstance(session_id, str) or not session_id or len(session_id) > 128:
            raise PreflightError("SESSION_ID_INVALID")
        if engine is not None or package:
            raise PreflightError("RECEIVER_INSPECT_INVALID")
    if engine is not None and (not isinstance(engine, str) or len(engine) > 64):
        raise PreflightError("ENGINE_INVALID")
    if not 0 <= len(package) <= max_package_bytes or (
        action in ("status", "export") and package
    ):
        raise PreflightError("PACKAGE_BOUNDARY")
    if action == "export":
        if (
            session_id is None
            or not isinstance(session_id, str)
            or len(session_id) > 128
        ):
            raise PreflightError("SESSION_ID_INVALID")
        if engine is None:
            raise PreflightError("ENGINE_REQUIRED")
    if action == "export-complete":
        if (
            session_id is None
            or not isinstance(session_id, str)
            or len(session_id) > 128
        ):
            raise PreflightError("SESSION_ID_INVALID")
        if engine is None:
            raise PreflightError("ENGINE_REQUIRED")
        package = (
            json.dumps(
                {"package_sha256": completion_sha256},
                separators=(",", ":"),
            ).encode("ascii")
            if completion_sha256
            else package
        )
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
    header_fields: dict[str, Any] = {
        "protocol": 1,
        "action": action,
        "operation_id": operation_id,
        "project": project,
        "size": len(package),
        "sha256": digest,
    }
    if action == "inspect":
        del header_fields["size"], header_fields["sha256"]
    if engine is not None:
        header_fields["engine"] = engine
    if session_id is not None:
        header_fields["session_id"] = session_id
    header = json.dumps(header_fields, separators=(",", ":")).encode("ascii") + b"\n"
    if len(header) > 4096:
        raise PreflightError("FRAME_TOO_LARGE")
    target = f"{peer.user}@{peer.host}"
    command = peer.receiver_path + " " + action
    argv = [
        "ssh",
        "-T",
        "-F",
        os.devnull,
        "-p",
        str(peer.port),
        "-i",
        str(identity),
        "-o",
        "BatchMode=yes",
        "-o",
        "NumberOfPasswordPrompts=0",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "UpdateHostKeys=no",
        "-o",
        f"UserKnownHostsFile={known}",
        "-o",
        "GlobalKnownHostsFile=none",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ForwardAgent=no",
        "-o",
        "ForwardX11=no",
        "-o",
        "ClearAllForwardings=yes",
        "-o",
        "PermitLocalCommand=no",
        "-o",
        "ControlMaster=no",
        "-o",
        f"ConnectTimeout={max(1, int(timeout_s))}",
        "--",
        target,
        command,
    ]
    stdout_cap = max_package_bytes * 4 // 3 + 16384 if action == "export" else 16384
    try:
        if run is subprocess.run:
            # Real transport: concurrent stdin write and bounded stdout read,
            # so an over-producing or stalling child is killed, never buffered.
            # timeout_s is the total monotonic budget; no slack is added.
            code, out = _bounded_run(
                argv,
                payload=header + package,
                timeout_s=timeout_s,
                stdout_cap=stdout_cap,
            )
            result = subprocess.CompletedProcess(argv, code, out, b"")
        else:
            result = run(
                argv,
                input=header + package,
                capture_output=True,
                timeout=timeout_s,
                check=False,
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PreflightError("RECEIVER_UNREACHABLE") from exc
    except _ReplyOverflow as exc:
        raise PreflightError("RECEIVER_REPLY_OVERSIZE") from exc
    if result.returncode != 0:
        raise PreflightError("RECEIVER_REFUSED")
    max_reply_bytes = max_package_bytes * 2 + 16384 if action == "export" else 16384
    if len(result.stdout) > max_reply_bytes:
        raise PreflightError("RECEIVER_REPLY_OVERSIZE")
    try:
        reply = json.loads(
            result.stdout,
            object_pairs_hook=_unique_object if action == "inspect" else None,
        )
    except (UnicodeError, ValueError) as exc:
        raise PreflightError("RECEIVER_REPLY_MALFORMED") from exc
    if action == "inspect" and (
        not isinstance(reply, dict)
        or set(reply)
        != {
            "protocol",
            "endpoint_id",
            "operation_id",
            "project",
            "session_id",
            "engine",
            "status",
        }
        or type(reply["protocol"]) is not int
        or any(
            not isinstance(value, str)
            for key, value in reply.items()
            if key != "protocol"
        )
        or reply["project"] != project
        or reply["session_id"] != session_id
        or reply["status"] != "found"
        or re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", reply["engine"]) is None
    ):
        raise PreflightError("RECEIVER_INSPECT_INVALID")
    if not isinstance(reply, dict) or reply.get("protocol") != 1:
        raise PreflightError("RECEIVER_PROTOCOL_MISMATCH")
    if reply.get("operation_id") != operation_id:
        raise PreflightError("RECEIVER_OPERATION_MISMATCH")
    if reply.get("endpoint_id") != peer.expected_endpoint_id:
        raise PreflightError("RECEIVER_ENDPOINT_MISMATCH")
    if action == "export":
        if (
            reply.get("status") != "exported"
            or reply.get("session_id") != session_id
            or reply.get("engine") != engine
            or not isinstance(reply.get("package_base64"), str)
            or not isinstance(reply.get("package_sha256"), str)
        ):
            raise PreflightError("RECEIVER_EXPORT_INVALID")
        try:
            pkg = base64.b64decode(reply["package_base64"], validate=True)
        except (ValueError, TypeError) as exc:
            raise PreflightError("RECEIVER_PACKAGE_MALFORMED") from exc
        if hashlib.sha256(pkg).hexdigest() != reply["package_sha256"]:
            raise PreflightError("RECEIVER_PACKAGE_MISMATCH")
        reply["package"] = pkg
    if action == "receive" and (
        reply.get("status") != "verified"
        or not isinstance(reply.get("native_id"), str)
        or not reply["native_id"]
        or not isinstance(reply.get("importer_version"), str)
        or not reply["importer_version"]
    ):
        raise PreflightError("RECEIVER_NOT_VERIFIED")
    if action == "receive" and engine is not None and reply.get("engine") != engine:
        raise PreflightError("RECEIVER_ENGINE_MISMATCH")
    receipt_hash = reply.get("package_sha256")
    if action == "receive" and receipt_hash != digest:
        raise PreflightError("RECEIVER_PACKAGE_MISMATCH")
    if action == "status" and (
        not isinstance(receipt_hash, str)
        or len(receipt_hash) != 64
        or any(c not in "0123456789abcdef" for c in receipt_hash)
    ):
        raise PreflightError("RECEIVER_RECEIPT_HASH_INVALID")
    if action == "export-complete" and (
        reply.get("status") != "export_completed"
        or reply.get("session_id") != session_id
        or reply.get("engine") != engine
        or receipt_hash != completion_sha256
    ):
        raise PreflightError("RECEIVER_COMPLETION_MISMATCH")
    return reply
