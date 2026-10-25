"""Fresh, least-privilege receiver probes for native and container-wrapper modes.

Never installs packages, opens a public listener, executes a shell or scans home.
Missing provider, Telegram, quiescence, or container attestations are FALSE.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProbeSpec:
    receiver: Path
    project: Path
    staging: Path
    expected_git_head: str
    agent_executable: Path
    min_free_bytes: int
    min_free_inodes: int = 256
    expected_uid: int | None = None
    execution_mode: str = "native"


@dataclass
class ProbeCallbacks:
    # These must be installed, real, side-effect-free probes supplied by the
    # running Untether engine/Telegram transport; no static success defaults.
    agent_version_compatible: Callable[[], bool] | None = None
    adapter_capable: Callable[[], bool] | None = None
    provider_auth: Callable[[], bool] | None = None
    provider_connectivity: Callable[[], bool] | None = None
    quiescent: Callable[[], bool] | None = None
    telegram_enrolled: Callable[[], bool] | None = None
    distinct_bot: Callable[[], bool] | None = None
    destination_binding: Callable[[], bool] | None = None
    resume_probe_available: Callable[[], bool] | None = None
    container_ready: Callable[[], bool] | None = None
    container_uid: Callable[[], bool] | None = None
    mount_mapping: Callable[[], bool] | None = None


def probe_endpoint(spec: ProbeSpec, hooks: ProbeCallbacks) -> dict[str, bool]:
    """Evaluate every check fresh, retaining only boolean diagnostics."""
    def check(callback: Callable[[], bool] | None) -> bool:
        if callback is None:
            return False
        try:
            return callback() is True
        except Exception:  # noqa: BLE001 - any probe failure MUST refuse
            return False

    def git_parity() -> bool:
        if not spec.project.is_dir() or not spec.expected_git_head:
            return False
        try:
            outcome = subprocess.run(
                ["git", "-C", str(spec.project), "rev-parse", "--verify", "HEAD"],
                capture_output=True, text=True, timeout=5, check=False,
            )
            return outcome.returncode == 0 and outcome.stdout.strip() == spec.expected_git_head
        except (OSError, subprocess.TimeoutExpired):
            return False

    def inode_capacity() -> bool:
        if os.name != "posix":
            return False
        try:
            stat = os.statvfs(spec.staging)
            return stat.f_favail >= spec.min_free_inodes
        except OSError:
            return False

    def disk_capacity() -> bool:
        try:
            return shutil.disk_usage(spec.staging).free >= spec.min_free_bytes
        except OSError:
            return False

    def writable_stage() -> bool:
        try:
            if not spec.staging.is_dir() or spec.staging.is_symlink():
                return False
            # An existing owner-writable directory can still be a read-only
            # mount: perform an actual O_EXCL create-and-unlink probe.
            name = spec.staging / (".untether-preflight-" + str(os.getpid()))
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            fd = os.open(name, flags, 0o600)
            try:
                os.close(fd)
            finally:
                name.unlink(missing_ok=True)
            return True
        except OSError:
            return False

    uid_ok = (
        os.name == "posix"
        and hasattr(os, "geteuid")
        and (spec.expected_uid is None or os.geteuid() == spec.expected_uid)
    )
    result = {
        "installed_receiver": spec.receiver.is_file()
        and not spec.receiver.is_symlink()
        and os.access(spec.receiver, os.X_OK),
        "runtime_ready": sys.platform.startswith("linux") and uid_ok,
        "agent_installed": spec.agent_executable.is_file()
        and not spec.agent_executable.is_symlink()
        and os.access(spec.agent_executable, os.X_OK),
        "agent_version_compatible": check(hooks.agent_version_compatible),
        "adapter_capable": check(hooks.adapter_capable),
        "provider_auth": check(hooks.provider_auth),
        "provider_connectivity": check(hooks.provider_connectivity),
        "staging_writable": writable_stage(),
        "disk_headroom": disk_capacity(),
        "inodes_available": inode_capacity(),
        "workspace_git_parity": git_parity(),
        "quiescent": check(hooks.quiescent),
        "telegram_enrolled": check(hooks.telegram_enrolled),
        "distinct_bot": check(hooks.distinct_bot),
        "destination_binding": check(hooks.destination_binding),
        "resume_probe_available": check(hooks.resume_probe_available),
    }
    if spec.execution_mode == "container-wrapper":
        result.update({
            "container_ready": check(hooks.container_ready),
            "container_uid": check(hooks.container_uid),
            "mount_mapping": check(hooks.mount_mapping),
        })
    return result
