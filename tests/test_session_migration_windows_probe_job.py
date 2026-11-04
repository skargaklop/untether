"""Win32 failure-path tests for ``session_migration.windows_probe_job``.

Every Win32 call is stubbed: the failures below are provoked without creating a
job object, a file handle or a process, so a rejection is proven by the returned
status alone. ``CreateProcessW`` aborts on purpose, which makes "nothing was
launched" an assertion rather than a promise.

Windows only: ``ctypes.WinDLL`` does not exist elsewhere, and
``probe_process`` imports the module under test lazily behind ``os.name``.
"""

from __future__ import annotations

import ctypes
import subprocess
import sys
from typing import Any

import pytest

if sys.platform != "win32":
    pytest.skip("requires the Windows kernel32 bindings", allow_module_level=True)

from untether.session_migration import windows_probe_job as wpj

_LEADER_PROCESS = 0x1234
_LEADER_JOB = 0x5678
_ACCESS_DENIED = 5
_ERROR_INVALID_HANDLE = 6
_WAIT_OBJECT_0 = 0x00000000
_WAIT_ABANDONED = 0x00000080
_WAIT_TIMEOUT = 0x00000102
_WAIT_FAILED = 0xFFFFFFFF
_WAIT_FAILED_MATCH = r"WaitForSingleObject status=0xffffffff"


class _FakeKernel32:
    """Stub ``kernel32`` that records calls and returns one configured outcome."""

    def __init__(
        self,
        *,
        wait_status: int = _WAIT_OBJECT_0,
        exit_code: int = 0,
        exit_code_ok: bool = True,
        nul_handle: Any = 0x90,
    ) -> None:
        self.wait_status = wait_status
        self.exit_code = exit_code
        self.exit_code_ok = exit_code_ok
        self.nul_handle = nul_handle
        self.calls: list[str] = []

    def WaitForSingleObject(self, process: Any, milliseconds: Any) -> int:
        self.calls.append("WaitForSingleObject")
        ctypes.set_last_error(_ERROR_INVALID_HANDLE)
        return self.wait_status

    def GetExitCodeProcess(self, process: Any, out: Any) -> int:
        self.calls.append("GetExitCodeProcess")
        ctypes.set_last_error(_ERROR_INVALID_HANDLE)
        if not self.exit_code_ok:
            return 0
        out._obj.value = self.exit_code
        return 1

    def CreateFileW(self, *args: Any) -> Any:
        self.calls.append("CreateFileW")
        ctypes.set_last_error(_ACCESS_DENIED)
        return self.nul_handle

    def CreateProcessW(self, *args: Any) -> Any:
        raise AssertionError("CreateProcessW must never be called by these tests")


def _install(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> _FakeKernel32:
    """Point the module at a stub kernel32; no Win32 object is ever created."""
    fake = _FakeKernel32(**kwargs)
    monkeypatch.setattr(wpj, "_KERNEL32", fake)
    return fake


def _probe(
    monkeypatch: pytest.MonkeyPatch, **kwargs: Any
) -> tuple[wpj.JobProbeProcess, _FakeKernel32]:
    """A job probe bound to stub handles, with nothing spawned."""
    fake = _install(monkeypatch, **kwargs)
    handle = wpj.JobProbeProcess(pid=4242, process=_LEADER_PROCESS, job=_LEADER_JOB)
    return handle, fake


@pytest.mark.parametrize("bad_handle", [ctypes.c_void_p(-1).value, 0, None])
def test_nul_handle_fails_closed_on_any_failure_sentinel(
    monkeypatch: pytest.MonkeyPatch, bad_handle: Any
) -> None:
    """INVALID_HANDLE_VALUE and NULL are both refusals, never usable handles."""
    _install(monkeypatch, nul_handle=bad_handle)
    with pytest.raises(OSError, match=r"CreateFileW\(NUL\)") as excinfo:
        wpj._nul_handle()
    assert excinfo.value.errno == _ACCESS_DENIED


def test_nul_handle_still_returns_a_real_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, nul_handle=0x90)
    assert wpj._nul_handle() == 0x90


def test_poll_rejects_wait_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    probe, _ = _probe(monkeypatch, wait_status=_WAIT_FAILED)
    with pytest.raises(OSError, match=_WAIT_FAILED_MATCH) as excinfo:
        probe.poll()
    assert excinfo.value.errno == _ERROR_INVALID_HANDLE


def test_wait_rejects_wait_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    probe, _ = _probe(monkeypatch, wait_status=_WAIT_FAILED)
    with pytest.raises(OSError, match=_WAIT_FAILED_MATCH):
        probe.wait(timeout=1.0)
    with pytest.raises(OSError, match=_WAIT_FAILED_MATCH):
        probe.wait()  # an infinite wait on a failed handle is no better


def test_wait_rejects_an_unexpected_status(monkeypatch: pytest.MonkeyPatch) -> None:
    probe, _ = _probe(monkeypatch, wait_status=_WAIT_ABANDONED)
    with pytest.raises(OSError, match=r"WaitForSingleObject status=0x00000080"):
        probe.wait(timeout=0.5)


def test_poll_rejects_a_failed_exit_code_read(monkeypatch: pytest.MonkeyPatch) -> None:
    probe, fake = _probe(monkeypatch, exit_code_ok=False)
    with pytest.raises(OSError, match="GetExitCodeProcess") as excinfo:
        probe.poll()
    assert excinfo.value.errno == _ERROR_INVALID_HANDLE
    assert "GetExitCodeProcess" in fake.calls


def test_wait_rejects_a_failed_exit_code_read(monkeypatch: pytest.MonkeyPatch) -> None:
    probe, _ = _probe(monkeypatch, exit_code_ok=False)
    with pytest.raises(OSError, match="GetExitCodeProcess"):
        probe.wait()


def test_poll_reports_still_running_without_claiming_an_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe, fake = _probe(monkeypatch, wait_status=_WAIT_TIMEOUT)
    assert probe.poll() is None
    assert probe.returncode is None
    assert fake.calls.count("GetExitCodeProcess") == 0


def test_wait_timeout_is_still_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    probe, _ = _probe(monkeypatch, wait_status=_WAIT_TIMEOUT)
    with pytest.raises(subprocess.TimeoutExpired) as excinfo:
        probe.wait(timeout=0.25)
    assert excinfo.value.timeout == 0.25


@pytest.mark.parametrize("exit_code", [0, 3])
def test_an_observed_exit_code_is_preserved(
    monkeypatch: pytest.MonkeyPatch, exit_code: int
) -> None:
    """A real exit still reads back, including a legitimate zero."""
    probe, fake = _probe(monkeypatch, exit_code=exit_code)
    assert probe.poll() == exit_code
    assert probe.returncode == exit_code
    assert probe.wait() == exit_code
    assert fake.calls.count("GetExitCodeProcess") == 2
    assert "CreateProcessW" not in fake.calls
