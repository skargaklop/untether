"""Bounded launch and guaranteed teardown for the external engine restart probe.

The probe runs with every standard stream on ``DEVNULL``: nothing a hung or
hostile engine writes can accumulate in this process. Whatever the probe does
— exit zero, fail, or hang past the deadline — its privately owned process
group (POSIX) or process tree (Windows) is killed and the leader reaped before
this function returns, so a failed restart probe cannot leave an engine orphan
behind it. No global process scan and no signal ever leaves the group or tree
this module created.
"""

from __future__ import annotations

import os
import signal
import subprocess

from .native_adapter import CapabilityUnavailable

# Teardown budget: the probe deadline belongs to the caller, the cleanup that
# follows it must be bounded too.
_REAP_TIMEOUT_S = 5.0
_TASKKILL_TIMEOUT_S = 5.0
# `taskkill` exits 128 when the PID (and thus the tree) is already gone.
_TASKKILL_ALREADY_GONE = 128


def _spawn(argv: list[str]) -> subprocess.Popen[bytes]:
    """Start *argv* as the leader of a fresh, privately owned group.

    ``argv`` is validated operator configuration, never chat input, so
    ``shell=False`` is kept explicit.
    """
    if os.name == "nt":
        return subprocess.Popen(  # nosec B603
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            shell=False,
        )
    return subprocess.Popen(  # nosec B603
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        # pid == pgid == sid for this probe alone: the group is ours to kill.
        start_new_session=True,
        shell=False,
    )


def _kill_owned_tree(proc: subprocess.Popen[bytes]) -> str | None:
    """Kill the probe's own group/tree. Return a reason when that failed.

    POSIX signals ``os.killpg(proc.pid, SIGKILL)`` — the group id created by
    ``start_new_session=True``, so the signal is confined to this probe's
    session and its descendants. Windows walks the OS tree by parent PID with
    a bounded ``taskkill /T /F``, falling back to killing the direct child.
    """
    if os.name == "nt":
        reason: str | None = None
        try:
            result = subprocess.run(  # nosec B603
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=_TASKKILL_TIMEOUT_S,
                shell=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            reason = f"taskkill unavailable or failed: {exc!r}"
        else:
            if result.returncode not in (0, _TASKKILL_ALREADY_GONE):
                reason = f"taskkill rc={result.returncode}"
        if proc.poll() is None:
            proc.kill()
        return reason
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return None  # group already empty
    except OSError as exc:
        return f"killpg failed: {exc!r}"
    return None


def _reap_leader(proc: subprocess.Popen[bytes]) -> bool:
    """Reap the direct child inside the teardown budget. False means alive."""
    try:
        proc.wait(timeout=_REAP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return False
    return True


def run_probe(argv: list[str], timeout_s: float) -> None:
    """Run an engine probe to completion and reap everything it started.

    Args:
        argv: Operator-configured probe command (no shell, no chat input).
        timeout_s: Hard probe deadline in seconds.

    Raises:
        CapabilityUnavailable: ``RESTART_PROBE_FAILED`` when the probe cannot
            be started, exits nonzero, exceeds ``timeout_s``, or when its
            process group/tree cannot be proven dead — an unproven teardown is
            never reported as success.
    """
    try:
        proc = _spawn(argv)
    except (OSError, ValueError) as exc:
        raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc

    failure: BaseException | None = None
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        failure = exc
    finally:
        cleanup = _kill_owned_tree(proc)
        if not _reap_leader(proc):
            cleanup = cleanup or "probe leader still unreaped after SIGKILL"

    if cleanup is not None and failure is None:
        failure = RuntimeError(cleanup)
    if failure is None and proc.returncode != 0:
        failure = RuntimeError(f"probe exited rc={proc.returncode}")
    if failure is not None:
        raise CapabilityUnavailable("RESTART_PROBE_FAILED") from failure
