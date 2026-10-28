#!/usr/bin/env bash
# Reproducible all-engine session-migration compatibility check.
#
# Runs the installed-engine native restore/restart/follow-up matrix on
# disposable stores through the registered production persistence adapters.
# Docs: docs/reference/session-migration-verification.md
#
# Usage: scripts/check-engine-matrix.sh [-k selection]
# Exit 0 only when every non-skipped check passes. Engines not installed on
# this host skip with explicit reasons (never pass silently).

set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONUTF8=1

PY=".venv/Scripts/python.exe"
[ -x "$PY" ] || PY=".venv/bin/python"
[ -x "$PY" ] || PY="python"

# Optional OpenCode executable export (Windows npm location when installed).
if [ -z "${UNTETHER_TEST_OPENCODE_EXE:-}" ]; then
  for candidate in \
    "$APPDATA/npm/node_modules/opencode-ai/bin/opencode.exe" \
    "${APPDATA:-}/npm/node_modules/opencode-ai/bin/opencode.exe"; do
    if [ -f "$candidate" ]; then
      export UNTETHER_TEST_OPENCODE_EXE="$candidate"
      break
    fi
  done
fi

exec "$PY" -m pytest \
  tests/test_session_migration_pi_runtime.py \
  tests/test_session_migration_omp_runtime.py \
  tests/test_session_migration_codex_runtime.py \
  tests/test_session_migration_opencode_runtime.py \
  -q -o addopts='' "${@}"
