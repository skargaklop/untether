"""Disposable-subprocess tests for the owned-tree engine probe launcher.

Every probe here is a throwaway Python child launched with ``sys.executable``;
no real engine, no live process, and no global process discovery is involved.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.probe_process import run_probe

_EXIT_ZERO = "raise SystemExit(0)"
_EXIT_THREE = "raise SystemExit(3)"

# Grandchild: outlives the probe deadline, then tries to leave evidence.
_GRANDCHILD = (
    "import pathlib, sys, time\n"
    "time.sleep(float(sys.argv[2]))\n"
    "pathlib.Path(sys.argv[1]).write_text('leaked', encoding='utf-8')\n"
)
# Probe: spawns the grandchild inside its own group, then hangs past its deadline.
_HANG_WITH_GRANDCHILD = (
    "import subprocess, sys, time\n"
    "subprocess.Popen([sys.executable, '-c', sys.argv[3], sys.argv[1], sys.argv[2]])\n"
    "time.sleep(300)\n"
)

_GRANDCHILD_DELAY_S = 3.0
_PROBE_TIMEOUT_S = 2.0


def test_success_returns_none() -> None:
    run_probe([sys.executable, "-c", _EXIT_ZERO], timeout_s=30.0)


def test_nonzero_exit_refuses() -> None:
    with pytest.raises(CapabilityUnavailable) as excinfo:
        run_probe([sys.executable, "-c", _EXIT_THREE], timeout_s=30.0)
    assert str(excinfo.value) == "RESTART_PROBE_FAILED"


def test_unlaunchable_probe_refuses(tmp_path: Path) -> None:
    with pytest.raises(CapabilityUnavailable) as excinfo:
        run_probe([str(tmp_path / "no-such-engine-probe")], timeout_s=5.0)
    assert str(excinfo.value) == "RESTART_PROBE_FAILED"


def test_timeout_kills_owned_tree_before_grandchild_marker(tmp_path: Path) -> None:
    """A hung probe and its grandchild must both die with the deadline.

    The grandchild writes a marker only after ``_GRANDCHILD_DELAY_S``; the
    probe hangs far longer. If teardown leaked the owned tree, the marker
    would appear while this test waits past that delay.
    """
    marker = tmp_path / "grandchild-marker.txt"
    argv = [
        sys.executable,
        "-c",
        _HANG_WITH_GRANDCHILD,
        str(marker),
        str(_GRANDCHILD_DELAY_S),
        _GRANDCHILD,
    ]
    started = time.monotonic()
    with pytest.raises(CapabilityUnavailable) as excinfo:
        run_probe(argv, timeout_s=_PROBE_TIMEOUT_S)
    elapsed = time.monotonic() - started

    assert str(excinfo.value) == "RESTART_PROBE_FAILED"
    assert elapsed >= _PROBE_TIMEOUT_S, "deadline was not honoured"
    assert not marker.exists(), "grandchild wrote its marker during the deadline"

    remaining = max(0.0, _GRANDCHILD_DELAY_S - elapsed) + 1.0
    time.sleep(remaining)
    assert not marker.exists(), "leaked grandchild survived probe teardown"
