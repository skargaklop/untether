"""Read-only fresh SSH preflight using a fixed, operator-installed receiver command.

The receiver is a single restricted endpoint, not an arbitrary remote shell.
No migration is enabled by this module; it refuses all missing health signals.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .settings import MigrationPeer

_REQUIRED_CHECKS = (
    "installed_receiver", "runtime_ready", "agent_installed",
    "agent_version_compatible", "adapter_capable", "provider_auth",
    "provider_connectivity", "staging_writable", "disk_headroom",
    "inodes_available", "workspace_git_parity", "quiescent",
    "telegram_enrolled", "distinct_bot", "destination_binding",
    "resume_probe_available",
)


class PreflightError(RuntimeError):
    """Redacted code-only error; no SSH stderr or sensitive paths included."""


def _identity(peer: MigrationPeer) -> tuple[Path, Path]:
    folder = Path(peer.identity_dir).expanduser()
    filename = folder / peer.identity_file
    known = Path(peer.known_hosts_file).expanduser()
    if (
        folder.is_symlink()
        or filename.is_symlink()
        or not folder.is_dir()
        or not filename.is_file()
        or not filename.resolve().is_relative_to(folder.resolve())
    ):
        raise PreflightError("IDENTITY_UNSAFE: select an existing regular key inside identity_dir")
    if known.is_symlink() or not known.is_file():
        raise PreflightError("KNOWN_HOSTS_MISSING: verify and pin the host key out of band")
    return filename, known


def inspect_peer(
    peer: MigrationPeer,
    *,
    project: str,
    direction: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    timeout_s: float = 10,
) -> dict[str, Any]:
    """Probe installed endpoint every invocation; never cache a green health result."""
    if direction not in ("to", "from"):
        raise PreflightError("DIRECTION_INVALID")
    if project not in peer.projects:
        raise PreflightError("PROJECT_UNMAPPED: configure an explicit project mapping")
    identity, known = _identity(peer)
    target = f"{peer.user}@{peer.host}"
    host_spec = f"[{peer.host}]:{peer.port}" if peer.port != 22 else peer.host
    try:
        key_info = run(
            ["ssh-keygen", "-F", host_spec, "-f", str(known), "-l", "-E", "sha256"],
            capture_output=True, text=True, timeout=timeout_s, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PreflightError("HOST_KEY_VERIFY_UNAVAILABLE") from exc
    if key_info.returncode != 0 or not re.search(
        r"(?<![A-Za-z0-9+/])" + re.escape(peer.host_key_fingerprint) + r"(?![A-Za-z0-9+/])",
        key_info.stdout,
    ):
        raise PreflightError("HOST_KEY_PIN_MISMATCH: confirm the known_hosts entry out of band")
    argv = [
        "ssh", "-T", "-F", os.devnull,
        "-p", str(peer.port),
        "-i", str(identity),
        "-o", "BatchMode=yes",
        "-o", "NumberOfPasswordPrompts=0",
        "-o", "StrictHostKeyChecking=yes",
        "-o", "UpdateHostKeys=no",
        "-o", f"UserKnownHostsFile={known}",
        "-o", "GlobalKnownHostsFile=/dev/null",
        "-o", "IdentitiesOnly=yes",
        "-o", "ForwardAgent=no",
        "-o", "ForwardX11=no",
        "-o", "ClearAllForwardings=yes",
        "-o", "PermitLocalCommand=no",
        "-o", "ControlMaster=no",
        "-o", f"ConnectTimeout={max(1, int(timeout_s))}",
        "--", target, f"{peer.receiver_path} inspect",
    ]
    try:
        outcome = run(
            argv, capture_output=True, text=True, timeout=timeout_s + 5,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise PreflightError("SSH_TIMEOUT: check reachability and authentication") from exc
    except OSError as exc:
        raise PreflightError("SSH_UNAVAILABLE: install OpenSSH client locally") from exc
    if outcome.returncode != 0:
        raise PreflightError("SSH_FAILED: check connectivity, host identity and receiver permissions")
    if len(outcome.stdout) > 65536:
        raise PreflightError("PROTOCOL_OVERSIZE")
    try:
        data = json.loads(outcome.stdout)
    except (ValueError, TypeError) as exc:
        raise PreflightError("PROTOCOL_MALFORMED") from exc
    if not isinstance(data, dict) or data.get("protocol") != 1:
        raise PreflightError("PROTOCOL_MISMATCH: upgrade installed receiver explicitly")
    if data.get("endpoint_id") != peer.expected_endpoint_id:
        raise PreflightError("ENDPOINT_MISMATCH: confirm enrolled destination identity")
    if data.get("execution_mode") != peer.execution_mode:
        raise PreflightError("EXECUTION_MODE_MISMATCH: verify installation profile")
    if data.get("destination_binding") != peer.destination_binding:
        raise PreflightError("TELEGRAM_BINDING_MISMATCH: enrol destination bot/chat/topic")
    if data.get("os") != "linux" or not isinstance(data.get("arch"), str) or not data["arch"]:
        raise PreflightError("PLATFORM_MISMATCH: Linux endpoint and architecture required")
    if data.get("project") != project:
        raise PreflightError("PROJECT_MISMATCH: verify destination project root")
    checks = data.get("checks")
    if not isinstance(checks, dict):
        raise PreflightError("HEALTH_MISSING: receiver must attest fresh health checks")
    for required in _REQUIRED_CHECKS:
        if checks.get(required) is not True:
            raise PreflightError(f"HEALTH_FAILED: {required}; inspect receiver prerequisites")
    if peer.execution_mode == "container-wrapper":
        for required in ("container_ready", "container_uid", "mount_mapping"):
            if checks.get(required) is not True:
                raise PreflightError(f"HEALTH_FAILED: {required}; inspect wrapper installation")
    return data
