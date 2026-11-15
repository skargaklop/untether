"""Tests for untether.session_migration.health_command.run_health."""

from __future__ import annotations

import sys
import time

import pytest

from untether.session_migration.health_command import run_health


def _child(code: str) -> tuple[str, ...]:
    return (sys.executable, "-X", "utf8", "-c", code)


def test_success_returns_boolean_dict() -> None:
    out = run_health(
        _child('print(\'{"ok": true, "dry": false, "bound": true}\')'),
        15.0,
    )
    assert out == {"ok": True, "dry": False, "bound": True}
    assert all(isinstance(v, bool) for v in out.values())


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        pytest.param("print('not-json-at-all-zz')", "HEALTH_BAD_JSON", id="not-json"),
        pytest.param("print('[1, 2]')", "HEALTH_BAD_JSON", id="list-json"),
        pytest.param("print('{\"a\": 1}')", "HEALTH_BAD_JSON", id="wrong-keys"),
    ],
)
def test_malformed_output_redacted_errors(code: str, expected: str) -> None:
    with pytest.raises(ValueError, match=expected) as exc_info:
        run_health(_child(code), 15.0)
    # Redacted: child output must not leak into the message.
    assert "zz" not in str(exc_info.value)


def test_oversize_output_killed_quickly() -> None:
    code = (
        "import sys, time;"
        "sys.stdout.write('x' * 70000);"
        "sys.stdout.flush();"
        "time.sleep(30)"
    )
    started = time.monotonic()
    with pytest.raises(ValueError, match="HEALTH_OUTPUT_OVERFLOW"):
        run_health(_child(code), 15.0)
    # Overflow kill must not wait out the sleeping child's full lifetime.
    assert time.monotonic() - started < 14.0


def test_timeout_short_child() -> None:
    started = time.monotonic()
    with pytest.raises(ValueError, match="HEALTH_TIMEOUT"):
        run_health(_child("import time; time.sleep(30)"), 0.5)
    assert time.monotonic() - started < 14.0
