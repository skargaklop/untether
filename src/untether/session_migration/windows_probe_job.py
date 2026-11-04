"""Windows Job Object containment for the external engine restart probe.

The probe leader is created *suspended*, assigned to a private Job Object that
carries ``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE``, and only then resumed, so no
probe code — and therefore no descendant of it — can ever run outside the job.
The job handle is held for the probe's whole lifetime and closed during
teardown: whatever the leader left behind dies with it, including children that
outlive a leader which has already exited normally.

``taskkill /T /F`` is deliberately not used: once the leader is gone it only
reports ``rc=128`` and cannot prove that the rest of the tree is dead.

Windows only.  ``probe_process`` imports this module lazily from inside its
``os.name == "nt"`` branch, so POSIX never touches ``ctypes.WinDLL``.
"""

from __future__ import annotations

import ctypes
import subprocess
from typing import Any, ClassVar

__all__ = ["JobProbeProcess", "spawn_suspended_in_job"]

# --- Win32 constants ---------------------------------------------------------
_CREATE_SUSPENDED = 0x00000004
_CREATE_NEW_PROCESS_GROUP = 0x00000200
_STARTF_USESTDHANDLES = 0x00000100
_GENERIC_READ_WRITE = 0x80000000 | 0x40000000
_FILE_SHARE_READ_WRITE = 0x00000001 | 0x00000002
_OPEN_EXISTING = 3
_WAIT_OBJECT_0 = 0x00000000
_WAIT_TIMEOUT = 0x00000102
# CreateFileW reports failure with this sentinel rather than NULL, so a
# NULL-only test would pass the sentinel through as a usable handle.
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_INFINITE = 0xFFFFFFFF
_RESUME_FAILED = 0xFFFFFFFF
_KILL_ON_JOB_CLOSE = 0x00002000
_EXTENDED_LIMIT_INFO_CLASS = 9
_PROBE_FAILED_EXIT_CODE = 1


# --- Win32 structures --------------------------------------------------------
class _SECURITY_ATTRIBUTES(ctypes.Structure):
    _fields_: ClassVar[list[tuple[str, Any]]] = [
        ("nLength", ctypes.c_uint32),
        ("lpSecurityDescriptor", ctypes.c_void_p),
        ("bInheritHandle", ctypes.c_int32),
    ]


class _IO_COUNTERS(ctypes.Structure):
    _fields_: ClassVar[list[tuple[str, Any]]] = [
        ("ReadOperationCount", ctypes.c_uint64),
        ("WriteOperationCount", ctypes.c_uint64),
        ("OtherOperationCount", ctypes.c_uint64),
        ("ReadTransferCount", ctypes.c_uint64),
        ("WriteTransferCount", ctypes.c_uint64),
        ("OtherTransferCount", ctypes.c_uint64),
    ]


class _BASIC_LIMIT_INFORMATION(ctypes.Structure):
    _fields_: ClassVar[list[tuple[str, Any]]] = [
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", ctypes.c_uint32),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", ctypes.c_uint32),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", ctypes.c_uint32),
        ("SchedulingClass", ctypes.c_uint32),
    ]


class _EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
    _fields_: ClassVar[list[tuple[str, Any]]] = [
        ("BasicLimitInformation", _BASIC_LIMIT_INFORMATION),
        ("IoInfo", _IO_COUNTERS),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _STARTUP_INFO(ctypes.Structure):
    _fields_: ClassVar[list[tuple[str, Any]]] = [
        ("cb", ctypes.c_uint32),
        ("lpReserved", ctypes.c_wchar_p),
        ("lpDesktop", ctypes.c_wchar_p),
        ("lpTitle", ctypes.c_wchar_p),
        ("dwX", ctypes.c_uint32),
        ("dwY", ctypes.c_uint32),
        ("dwXSize", ctypes.c_uint32),
        ("dwYSize", ctypes.c_uint32),
        ("dwXCountChars", ctypes.c_uint32),
        ("dwYCountChars", ctypes.c_uint32),
        ("dwFillAttribute", ctypes.c_uint32),
        ("dwFlags", ctypes.c_uint32),
        ("wShowWindow", ctypes.c_uint16),
        ("cbReserved2", ctypes.c_uint16),
        ("lpReserved2", ctypes.c_void_p),
        ("hStdInput", ctypes.c_void_p),
        ("hStdOutput", ctypes.c_void_p),
        ("hStdError", ctypes.c_void_p),
    ]


class _PROCESS_INFO(ctypes.Structure):
    _fields_: ClassVar[list[tuple[str, Any]]] = [
        ("hProcess", ctypes.c_void_p),
        ("hThread", ctypes.c_void_p),
        ("dwProcessId", ctypes.c_uint32),
        ("dwThreadId", ctypes.c_uint32),
    ]


# --- Win32 bindings ---------------------------------------------------------
# ``use_last_error=True`` is what makes ``ctypes.get_last_error()`` meaningful.
_KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
_KERNEL32.CreateJobObjectW.restype = ctypes.c_void_p
_KERNEL32.CreateJobObjectW.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p)
_KERNEL32.SetInformationJobObject.restype = ctypes.c_int
_KERNEL32.SetInformationJobObject.argtypes = (
    ctypes.c_void_p,
    ctypes.c_int,
    ctypes.c_void_p,
    ctypes.c_uint32,
)
_KERNEL32.AssignProcessToJobObject.restype = ctypes.c_int
_KERNEL32.AssignProcessToJobObject.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
_KERNEL32.TerminateJobObject.restype = ctypes.c_int
_KERNEL32.TerminateJobObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
_KERNEL32.CreateProcessW.restype = ctypes.c_int
_KERNEL32.CreateProcessW.argtypes = (
    ctypes.c_wchar_p,
    ctypes.c_wchar_p,
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_int,
    ctypes.c_uint32,
    ctypes.c_void_p,
    ctypes.c_wchar_p,
    ctypes.POINTER(_STARTUP_INFO),
    ctypes.POINTER(_PROCESS_INFO),
)
_KERNEL32.ResumeThread.restype = ctypes.c_uint32
_KERNEL32.ResumeThread.argtypes = (ctypes.c_void_p,)
_KERNEL32.WaitForSingleObject.restype = ctypes.c_uint32
_KERNEL32.WaitForSingleObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
_KERNEL32.GetExitCodeProcess.restype = ctypes.c_int
_KERNEL32.GetExitCodeProcess.argtypes = (
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_uint32),
)
_KERNEL32.TerminateProcess.restype = ctypes.c_int
_KERNEL32.TerminateProcess.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
_KERNEL32.CloseHandle.restype = ctypes.c_int
_KERNEL32.CloseHandle.argtypes = (ctypes.c_void_p,)
_KERNEL32.CreateFileW.restype = ctypes.c_void_p
_KERNEL32.CreateFileW.argtypes = (
    ctypes.c_wchar_p,
    ctypes.c_uint32,
    ctypes.c_uint32,
    ctypes.POINTER(_SECURITY_ATTRIBUTES),
    ctypes.c_uint32,
    ctypes.c_uint32,
    ctypes.c_void_p,
)


def _close(handle: int) -> int | None:
    """Close *handle*; return the Win32 error code when that failed."""
    if not handle:
        return None
    if _KERNEL32.CloseHandle(handle):
        return None
    return ctypes.get_last_error()


def _create_job() -> int:
    """Create a job whose last handle closing kills every process in it."""
    job = _KERNEL32.CreateJobObjectW(None, None)
    if not job:
        raise OSError(ctypes.get_last_error(), "CreateJobObjectW")
    limits = _EXTENDED_LIMIT_INFORMATION()
    limits.BasicLimitInformation.LimitFlags = _KILL_ON_JOB_CLOSE
    if not _KERNEL32.SetInformationJobObject(
        job,
        _EXTENDED_LIMIT_INFO_CLASS,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    ):
        code = ctypes.get_last_error()
        _close(job)
        raise OSError(code, "SetInformationJobObject")
    return job


def _nul_handle() -> int:
    """Open an inheritable handle to NUL: the probe's equivalent of DEVNULL.

    ``CreateFileW`` reports failure as ``INVALID_HANDLE_VALUE``, a truthy -1
    pointer, so both it and ``NULL`` are refusals here.
    """
    attrs = _SECURITY_ATTRIBUTES()
    attrs.nLength = ctypes.sizeof(_SECURITY_ATTRIBUTES)
    attrs.bInheritHandle = 1
    handle = _KERNEL32.CreateFileW(
        "NUL",
        _GENERIC_READ_WRITE,
        _FILE_SHARE_READ_WRITE,
        ctypes.byref(attrs),
        _OPEN_EXISTING,
        0,
        None,
    )
    if handle in (None, 0, _INVALID_HANDLE_VALUE):
        raise OSError(ctypes.get_last_error(), "CreateFileW(NUL)")
    return handle


def _create_suspended(argv: list[str], nul: int) -> tuple[int, int, int]:
    """Create *argv* suspended with all three standard streams on NUL.

    Returns ``(process handle, primary thread handle, pid)``. The thread handle
    is returned rather than closed by the creator because resuming the leader
    needs it.
    """
    startup = _STARTUP_INFO()
    startup.cb = ctypes.sizeof(_STARTUP_INFO)
    startup.dwFlags = _STARTF_USESTDHANDLES
    startup.hStdInput = startup.hStdOutput = startup.hStdError = nul
    info = _PROCESS_INFO()
    # CreateProcessW may rewrite lpCommandLine, so hand it a mutable buffer.
    command = ctypes.create_unicode_buffer(subprocess.list2cmdline(argv))
    created = _KERNEL32.CreateProcessW(
        None,
        ctypes.cast(command, ctypes.c_wchar_p),
        None,
        None,
        True,
        _CREATE_SUSPENDED | _CREATE_NEW_PROCESS_GROUP,
        None,
        None,
        ctypes.byref(startup),
        ctypes.byref(info),
    )
    if not created:
        raise OSError(ctypes.get_last_error(), "CreateProcessW")
    return info.hProcess, info.hThread, info.dwProcessId


def _abandon(job: int, process: int, thread: int, nul: int) -> None:
    """Release everything a failed spawn opened.

    The child is still suspended when this runs, so it cannot have descendants:
    terminating it and closing the job is a complete cleanup.
    """
    _close(thread)
    if process:
        _KERNEL32.TerminateProcess(process, _PROBE_FAILED_EXIT_CODE)
        _close(process)
    _KERNEL32.TerminateJobObject(job, _PROBE_FAILED_EXIT_CODE)
    _close(job)
    _close(nul)


def spawn_suspended_in_job(argv: list[str]) -> JobProbeProcess:
    """Start *argv* suspended, put it in a kill-on-close job, then resume it.

    Args:
        argv: Operator-configured probe command (no shell, no chat input).

    Returns:
        A handle owning the leader's process handle and the job itself.

    Raises:
        OSError: When ``CreateFileW``, ``CreateProcessW``, job creation, job
            assignment or ``ResumeThread`` failed. A probe that cannot be
            proven to belong to the job is never run: the suspended child, the
            job and the NUL handle are all released first.
    """
    job = _create_job()
    nul = 0
    process = 0
    thread = 0
    pid = 0
    try:
        nul = _nul_handle()
        process, thread, pid = _create_suspended(argv, nul)
    except OSError:
        _abandon(job, process, thread, nul)
        raise
    if not _KERNEL32.AssignProcessToJobObject(job, process):
        code = ctypes.get_last_error()
        _abandon(job, process, thread, nul)
        raise OSError(code, "AssignProcessToJobObject")
    if _KERNEL32.ResumeThread(thread) == _RESUME_FAILED:
        code = ctypes.get_last_error()
        _abandon(job, process, thread, nul)
        raise OSError(code, "ResumeThread")
    _close(thread)
    _close(nul)
    return JobProbeProcess(pid, process, job)


def _wait_status(process: int, milliseconds: int) -> int:
    """Wait on *process* and reject any status that proves nothing.

    A process handle can only report ``WAIT_OBJECT_0`` or ``WAIT_TIMEOUT``.
    ``WAIT_FAILED`` — and every other value — leaves the wait unproven, so it
    raises instead of being read as "still running": a probe whose wait cannot
    be trusted must never look like a probe that exited zero.
    """
    status = _KERNEL32.WaitForSingleObject(process, milliseconds)
    if status not in (_WAIT_OBJECT_0, _WAIT_TIMEOUT):
        raise OSError(
            ctypes.get_last_error(), f"WaitForSingleObject status={status:#010x}"
        )
    return status


class JobProbeProcess:
    """A probe leader owned by one kill-on-close Job Object.

    Mirrors the slice of ``subprocess.Popen`` that ``run_probe`` uses. The job
    handle is held from creation until :meth:`teardown`, so a leader that exits
    while its children are still alive leaves the whole tree killable.
    """

    def __init__(self, pid: int, process: int, job: int) -> None:
        self.pid = pid
        self._process = process
        self._job = job
        self._returncode: int | None = None

    @property
    def returncode(self) -> int | None:
        return self.poll()

    def poll(self) -> int | None:
        if (
            self._returncode is None
            and self._process
            and _wait_status(self._process, 0) == _WAIT_OBJECT_0
        ):
            self._returncode = self._exit_code()
        return self._returncode

    def wait(self, timeout: float | None = None) -> int:
        """Wait for the leader inside *timeout* seconds, else raise.

        An unproven wait or exit-code read raises ``OSError`` rather than being
        reported as a successful exit.
        """
        if timeout is None:
            _wait_status(self._process, _INFINITE)
        elif _wait_status(self._process, max(0, int(timeout * 1000))) == _WAIT_TIMEOUT:
            raise subprocess.TimeoutExpired(f"probe pid {self.pid}", timeout)
        self._returncode = self._exit_code()
        return self._returncode

    def teardown(self) -> str | None:
        """Terminate every process in the job and close the job handle.

        Closing the last job handle is the durable kill: children that outlived
        their leader die here even though the leader's PID is long gone.
        """
        if not self._job:
            return None
        reason: str | None = None
        if not _KERNEL32.TerminateJobObject(self._job, _PROBE_FAILED_EXIT_CODE):
            reason = f"TerminateJobObject rc={ctypes.get_last_error()}"
        error = _close(self._job)
        self._job = 0
        if error is not None and reason is None:
            reason = f"CloseHandle(job) rc={error}"
        return reason

    def release(self) -> str | None:
        """Close the leader's process handle, after the bounded reap."""
        error = _close(self._process)
        self._process = 0
        if error is None:
            return None
        return f"CloseHandle(leader) rc={error}"

    def _exit_code(self) -> int:
        code = ctypes.c_uint32()
        if not _KERNEL32.GetExitCodeProcess(self._process, ctypes.byref(code)):
            raise OSError(ctypes.get_last_error(), "GetExitCodeProcess")
        return int(code.value)
