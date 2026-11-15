# Session-migration engine-compatibility verification

Runs the four named installed-engine native restore/restart/shell-follow-up
profiles locally. This supplemental roster is not ACP-wide compatibility
coverage. Each named engine uses disposable stores through its registered
persistence adapter; nothing touches live sessions or stores.

## Command (Windows PowerShell, from the repository root)

```powershell
$env:PYTHONUTF8 = "1"
$env:UNTETHER_TEST_OPENCODE_EXE = "$env:APPDATA\npm\node_modules\opencode-ai\bin\opencode.exe"
.venv\Scripts\python.exe -m pytest tests/test_session_migration_pi_runtime.py `
    tests/test_session_migration_omp_runtime.py `
    tests/test_session_migration_codex_runtime.py `
    tests/test_session_migration_opencode_runtime.py `
    tests/test_session_migration_pi_coordinator.py `
    tests/test_session_migration_omp_coordinator.py `
    tests/test_session_migration_codex_coordinator.py `
    tests/test_session_migration_opencode_coordinator.py `
    -q -o addopts=""
```

Omit `UNTETHER_TEST_OPENCODE_EXE` when OpenCode is not installed; that file
skips with an explicit reason rather than failing.

## What each engine proves

| Engine | Restore | Independent restart | Native follow-up | Evolved archive return |
| --- | --- | --- | --- | --- |
| Pi | two sessions cold-load from A→B→fresh return store, exact bytes/history | yes | RPC `bash` marker, `bashExecution` persisted | yes |
| OMP | production `OmpPersistenceAdapter` import, SDK cold-load | yes | native RPC `bash` marker persisted | yes |
| Codex | production rollout import, identity/history checks | yes | `thread/shellCommand` echo, persisted user-shell result | yes |
| OpenCode | sync/replay import, full event equality | yes | `/session/{id}/shell` marker persisted | yes |

Limits stated honestly: these are native shell persistence proofs, not
LLM/model continuation; coordinator-integrated SSH round trips are covered by
the separate transfer suites (`tests/test_session_migration_transfer_end_to_end.py`).

A later bounded run of `bash scripts/check-engine-matrix.sh -vv --tb=short`
passed all 15 named-profile checks without skips in 256.62 seconds (420-second
outer bound). The earlier 14-check result was on a prior revision. This covers
native shell persistence and coordinator tests, not independent model-generated
continuation, every ACP-compatible engine, or a real remote SSH migration.

Skips are explicit and environmental (missing engine executable, Windows
symlink/gate privileges). A passing row requires the installed engine; absence
of an engine skips, it never passes silently.
