"""Bounded health-command runner for session migration.

Runs a probe as a subprocess and parses its stdout as a JSON object whose
values are all booleans. Output is drained by a bounded reader thread so an
over-producing child can never buffer unbounded data or fill a disk file:
the reader stops after MAX_OUTPUT_BYTES + 1 bytes, the supervisor kills the
child on overflow or deadline, and failures surface only as redacted
HEALTH_* ValueError codes (no child output is ever echoed).
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import threading
import time

MAX_OUTPUT_BYTES = 65536
_READ_LIMIT = MAX_OUTPUT_BYTES + 1
_POLL_INTERVAL_S = 0.05
_REAP_TIMEOUT_S = 5.0

_TIMEOUT = "HEALTH_TIMEOUT"
_OVERFLOW = "HEALTH_OUTPUT_OVERFLOW"
_BAD_JSON = "HEALTH_BAD_JSON"

__all__ = ["MAX_OUTPUT_BYTES", "run_health"]


def run_health(argv: tuple[str, ...], timeout_s: float) -> dict[str, bool]:
    """Run ``argv`` and return its stdout parsed as ``{str: bool}``.

    Raises ValueError with a redacted HEALTH_* code on deadline exceeded,
    output larger than MAX_OUTPUT_BYTES, or output that is not a JSON
    object of string keys mapped to booleans. Retry/decision policy is the
    caller's business.
    """
    result: list[bytes] = [b""]
    drained = threading.Event()
    proc = subprocess.Popen(
        list(argv),
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )
    stream = proc.stdout
    assert stream is not None  # Popen was constructed with stdout=PIPE.

    def _drain() -> None:
        try:
            result[0] = stream.read(_READ_LIMIT) or b""
        except (OSError, ValueError):
            pass
        finally:
            drained.set()

    reader = threading.Thread(target=_drain, daemon=True)
    reader.start()

    deadline = time.monotonic() + timeout_s
    try:
        while not drained.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError(f"{_TIMEOUT}: probe exceeded {timeout_s:g}s")
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=min(remaining, _POLL_INTERVAL_S))
        reader.join(_REAP_TIMEOUT_S)

        data = result[0]
        if len(data) > MAX_OUTPUT_BYTES:
            raise ValueError(f"{_OVERFLOW}: output exceeded {MAX_OUTPUT_BYTES} bytes")
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError(f"{_BAD_JSON}: stdout was not valid UTF-8 JSON") from None
        if not isinstance(payload, dict):
            raise ValueError(f"{_BAD_JSON}: JSON was not an object")
        for key, value in payload.items():
            if not isinstance(key, str) or not isinstance(value, bool):
                raise ValueError(
                    f"{_BAD_JSON}: expected string keys with boolean values"
                )
        return payload
    finally:
        if proc.poll() is None:
            proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=_REAP_TIMEOUT_S)
        drained.wait(_REAP_TIMEOUT_S)
        if stream is not None:
            stream.close()
