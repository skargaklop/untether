"""Bounded launch and guaranteed teardown for the external engine restart probe.

The probe runs with every standard stream on ``DEVNULL``: nothing a hung or
hostile engine writes can accumulate in this process. Whatever the probe does
— exit zero, fail, or hang past the deadline — its privately owned process
group (POSIX) or Job Object (Windows) is killed and the leader reaped before
this function returns, so a failed restart probe cannot leave an engine orphan
behind it. No global process scan and no signal ever leaves the group or job
this module created.

Windows teardown closes the job the probe was created in, which kills probes
whose leader exited with children still alive — the case ``taskkill /T /F``
cannot prove, since it only reports ``rc=128`` for a leader that is already
gone (see ``windows_probe_job``).
"""

from __future__ import annotations

import os
import signal
import subprocess
from typing import Protocol

from .native_adapter import CapabilityUnavailable

# Teardown budget: the probe deadline belongs to the caller, the cleanup that
# follows it must be bounded too.
_REAP_TIMEOUT_S = 5.0


class _OwnedProbe(Protocol):
    """The probe-handle surface ``run_probe`` relies on, ``Popen`` compatible."""

    @property
    def pid(self) -> int: ...

    @property
    def returncode(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def poll(self) -> int | None: ...


def _spawn(argv: list[str]) -> _OwnedProbe:
    """Start *argv* as the leader of a fresh, privately owned group or job.

    ``argv`` is validated operator configuration, never chat input, so
    ``shell=False`` is kept explicit. On Windows the leader is created
    suspended and assigned to a kill-on-close job before it can run.
    """
    if os.name == "nt":
        from .windows_probe_job import spawn_suspended_in_job

        return spawn_suspended_in_job(argv)
    return subprocess.Popen(  # nosec B603
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        # pid == pgid == sid for this probe alone: the group is ours to kill.
        start_new_session=True,
        shell=False,
    )


def _kill_owned_tree(proc: _OwnedProbe) -> str | None:
    """Kill the probe's own group/tree. Return a reason when that failed.

    POSIX signals ``os.killpg(proc.pid, SIGKILL)`` — the group id created by
    ``start_new_session=True``, so the signal is confined to this probe's
    session and its descendants. Windows tears down the job object that has
    owned the probe and every descendant since before it was resumed.
    """
    if os.name == "nt":
        return _teardown_job(proc)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return None  # group already empty
    except OSError as exc:
        return f"killpg failed: {exc!r}"
    return None


def _teardown_job(proc: _OwnedProbe) -> str | None:
    """Windows: terminate the probe's job and close the handle that owns it."""
    from .windows_probe_job import JobProbeProcess

    if not isinstance(proc, JobProbeProcess):  # pragma: no cover - wiring bug
        return "probe handle is not a Windows job probe"
    return proc.teardown()


def _release_owned_handles(proc: _OwnedProbe) -> str | None:
    """Drop any residual OS handle. A no-op for POSIX process groups."""
    if os.name != "nt":
        return None
    from .windows_probe_job import JobProbeProcess

    if not isinstance(proc, JobProbeProcess):
        return None
    return proc.release()


def _reap_leader(proc: _OwnedProbe) -> bool:
    """Reap the direct child inside the teardown budget. False means alive."""
    try:
        proc.wait(timeout=_REAP_TIMEOUT_S)
    except (subprocess.TimeoutExpired, OSError):
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
            process group/job cannot be proven dead — an unproven teardown is
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
            cleanup = cleanup or "probe leader still unreaped after kill"
        cleanup = _release_owned_handles(proc) or cleanup

    if cleanup is not None and failure is None:
        failure = RuntimeError(cleanup)
    if failure is None and proc.returncode != 0:
        failure = RuntimeError(f"probe exited rc={proc.returncode}")
    if failure is not None:
        raise CapabilityUnavailable("RESTART_PROBE_FAILED") from failure
