# Takopi Feature Port Audit — Takopi → Untether

**Date:** 2026-08-10
**Untether baseline:** `4fdf694` on `takopi-gap-closure`; post-health migration repairs are pending commit.
**Takopi reference:** `d28e5ba7cf449608634c4a3ab6206998e6d4f0ae`.
**Authoritative execution plan:** `D:/Projects/takopi/docs/plans/2026-08-11-takopi-untether-comprehensive-gap-closure-plan.md`.
**Historical comparison:** `D:/Projects/takopi/docs/reference/untether-comparison.md` remains evidence only; its pre-migration conclusions are not current truth.

## Method

Source behavior and deterministic tests in the Untether tree override earlier completion prose. Classifications are `implemented`, `partial`, `missing`, `runtime-unverified`, `superseded`, `not-applicable`, and `stale`. This ledger separates source behavior, deterministic proof, live-runtime evidence, and roadmap-only work; it does not print credentials, identifiers, or state values.

---

## A. Already-source-verified behavior

The migration behavior is wired in the current Untether source and covered by deterministic tests.

| # | Capability | Takopi evidence | Untether evidence | Disposition |
|---|---|---|---|---|
| A1 | Hardened scheduler cancellation (`CancelQueuedResult`/`CancelQueuedStatus`) | `src/takopi/scheduler.py:197-214` | `src/untether/scheduler.py:49-74` (`CancelQueuedStatus`, `CancelQueuedResult` with `__post_init__` validation) | present |
| A2 | Scheduler claim/requeue (steering) | `src/takopi/scheduler.py:216-242` (`claim_queued`, `requeue_front`) | `src/untether/scheduler.py` — `claim_queued`/`requeue_front` methods present | present |
| A3 | Job lifecycle observers (`on_job_claimed`/`on_job_failed`) | `src/takopi/scheduler.py:38-39,97-103` | `src/untether/scheduler.py:37-39,83-99` (`JobClaimed`/`JobFailed` callables, `_noop_*` defaults, constructor params) | present |
| A4 | Enqueue disposition (`EnqueueDisposition` QUEUED/CLAIMABLE) | `src/takopi/scheduler.py:42-46` | `src/untether/scheduler.py:42-47` | present |
| A5 | Prompt batching | `src/takopi/telegram/prompt_batch.py` | `src/untether/telegram/prompt_batch.py` | present |
| A6 | `EngineRunOptions`/`ThreadJob` directive transport | `EngineRunOptions`, `ThreadJob.run_options`, queue and loop dispatch | implemented — directives now cross queued/resumed paths as one options object; no positional propagation remains |
| A7 | Pi unknown-event decoding (`PiUnknownEvent`) | `src/takopi/schemas/pi.py:95-103` | `src/untether/schemas/pi.py` (`PiUnknownEvent` + peek-decode) | present |
| A8 | Pi numeric retry delay (`AutoRetryStart.delayMs: int|float|None`) | `src/takopi/schemas/pi.py` | `src/untether/schemas/pi.py` | present |
| A9 | OMP full session IDs (`shorten_session_id=False`) | `src/takopi/runners/omp.py:152-159` | `src/untether/runners/omp.py` (`OmpRunner.new_state` sets `shorten_session_id=False`) | present |
| A10 | OMP/Grok/Agy runner registration | `src/takopi/runners/{omp,grok,agy}.py:BACKEND` | `src/untether/runners/{omp,grok,agy}.py` + `pyproject.toml` entry-points | present |
| A11 | Compact core/mixins/ACP (data layer) | `src/takopi/compact.py`, `_compact_mixin.py`, `_acp.py` | `src/untether/compact.py`, `runners/_compact_mixin.py`, `runners/_acp.py` | present |
| A12 | Windows tree cleanup / stream closure | `src/takopi/utils/subprocess.py` | `src/untether/utils/subprocess.py` (`taskkill /T /F`, `close_process_streams`) | present |
| A13 | Three-OS CI test matrix | `takopi/.github/workflows/ci.yml` | `untether/.github/workflows/ci.yml` `test-cross-os` job (ubuntu/macos/windows) | present |
| A14 | 81% coverage gate | `takopi/pyproject.toml --cov-fail-under=81` | `untether/pyproject.toml:108 --cov-fail-under=81`; ci.yml coverage-gate step | present |
| A15 | Transient failure classifier + clean formatter | `src/takopi/utils/transient_failures.py` | `src/untether/utils/transient_failures.py` (`classify_transient_failure`, `format_transient_failure`, `_BARE_STATUS_PREFIX_RE`) | present |
| A16 | Subagent injection | Grok, Claude, and OpenCode runner argument construction | implemented — Claude/OpenCode consume `run_options.subagent`; no unsupported-engine injection is claimed |

## B. Untether-only features to preserve (not regressed)

Triggers (`triggers/`), cost tracking (`cost_tracker.py`), quarantine (`session_quarantine.py`), hot reload/migrations (`config_watch.py`, `config_migrations.py`), outbox delivery (`outbox_delivery.py`), stats/health/browse/auth/config commands, environment policy/audit (`utils/env_policy.py`, `utils/env_audit.py`), diagnostics (`utils/proc_diag.py`), persistence (`progress_persistence.py`), systemd notify (`sdnotify.py`), security/release CI jobs, Gemini/Amp runners. All `present` in Untether and untouched by this audit.

## C. Superseded / not applicable

| # | Item | Disposition | Reason |
|---|---|---|---|
| C1 | Takopi namespace/package entry-point migration | not-applicable | Untether already owns its namespace and entry points. |
| C2 | Takopi release chores | not-applicable | Per-version release management, not migration behavior. |
| C3 | `[[takopi-send: path]]` | superseded | `.untether-outbox/` delivery is the live contract. |
| C4 | `pi.plan_flag` | superseded | Extension detection is the live contract. |
| C5 | Removed image configuration keys | not-applicable | No corresponding Untether configuration surface. |

## D. Superseded configuration

| # | Item | Superseded by |
|---|---|---|
| D1 | Legacy `[[takopi-send: path]]` marker protocol | `.untether-outbox/` directory delivery (`outbox_delivery.py`). |
| D2 | Legacy claim that `[logging]` is absent | tested TOML `[logging]` compatibility (`UntetherSettings.logging`, CLI wiring, redaction) |
| D3 | `pi.plan_flag` config flag | Extension detection (see E4). |

## E. Gap-closure ledger

| Item | Current classification | Deterministic evidence | Runtime evidence |
|---|---|---|---|
| Compact/handoff approval and lifecycle | implemented | `test_telegram_compact_dispatch.py`, `test_scheduler_queue.py`; opaque scoped callbacks, expiry, allowlist gate, one-card queue/cancel/failure rendering | runtime-unverified — needs authorized Telegram smoke |
| Transactional handoff routing | implemented | focused rollback test; destination `RunOutcome` validates before route commit | runtime-unverified — needs successful and failed destination probes |
| Meta commands and directive consumers | implemented | meta form classification, stores, runner argument and queue tests | runtime-unverified — needs Telegram menu/prompt smoke |
| Runner lifecycle and ACP timeout ownership | implemented | subprocess, settings, runner/ACP tests | runtime-unverified — native ACP probe unavailable |
| Retry and timeout terminal semantics | implemented | retry/timeout contract tests, including a terminal timeout event with the runner engine | runtime-unverified — provider transient probe unavailable |
| Pi goal-list extension seeding | implemented | Pi runner tests cover fresh XML seeding, escaping, fallback, and plan precedence | runtime-unverified — installed extension probe unavailable |
| CI hard type gate and 3-OS static matrix | partial | `.github/workflows/ci.yml` enforces formatting, Ruff, and `ty check src tests` across Ubuntu, macOS, and Windows; fresh production `ty check src` and Ruff gates pass locally | runtime-unverified — test-fixture Ty debt and hosted macOS/Linux jobs await CI execution |
| Live config/state/poller cutover | partial | restricted config/state backup created; config loader, `untether doctor`, installed imports, Startup target, and state schema/mtime comparison verified | runtime-unverified — no poller was running; authorized Telegram and native-engine probes require external credentials/CLIs |
| Installed `/health` command reliability | implemented | registry loads the `health` entry point; generic dispatch gives catalog misses a visible error; immutable command status plus bounded progressive diagnostic delivery are covered by `test_command_registry.py`, `test_health_command.py`, `test_scheduler_queue.py`, `test_telegram_bridge.py`, and `test_transport_runtime.py` | runtime-unverified — authorized Telegram smoke still requires credentials and a single owned poller |

## F. Verification snapshot

- Focused migration validation for installed health reliability: **151 passed, 2 skipped** across resolver, health, command-registry, scheduler, and Telegram bridge contracts.
- Current focused migration regressions: **66 passed** across Codex app-server, OMP stdin transport, configured voice backends, and installed command registry.
- Fresh Windows full-suite verification: **3360 passed, 35 skipped, 9 warnings** in 204.22 seconds. The existing branch-inclusive coverage gate passed at **81.01%**; `--cov-branch` and `--cov-fail-under=81` remain unchanged.
- Fresh local static checks pass: `ruff format --check src tests`, `ruff check src tests`, and `ty check src tests`.
- `uv lock --check`, `scripts/validate_release.py`, and `git diff --check` pass. Rebuild artifacts after these pending changes are committed; hosted macOS/Linux CI and credentialed Telegram/native-engine probes remain runtime-unverified.
- A clean isolated wheel environment previously imported Untether, `untether.lockfile`, Telegram transport, and Codex, Claude, Pi, OMP, Grok, Agy, OpenCode, Gemini, and Amp runners.

## G. Roadmap-only carryover

Tasks 4, 20, and 23 appear exactly once under Untether’s **Future** roadmap section. They are requested future work, not shipped migration claims. Task 24 and E12/E13 are intentionally absent from the roadmap.

## H. Historical claims corrected

Earlier blanket-completion language was stale: parser/data-field presence did not prove meta dispatch, sticky plan behavior, one-card compact/handoff lifecycle, or Claude/OpenCode agent injection. The implemented source is covered by focused tests; the runtime-only and CI-type-gate limitations remain recorded in sections E–F. Skill is intentionally one-shot carried data; source-backed badges/context show plan and goal only.
## I. Complete commit-ledger reconciliation

The durable ordered evidence ledger is `D:/Projects/takopi/docs/plans/2026-08-11-takopi-commit-contract-ledger.md`. It records the first 453 ordered Takopi commits exactly once, from `75fa95752feac42d05dc635c450027c69aa6ae17` through `d28e5ba7cf449608634c4a3ab6206998e6d4f0ae`, with per-commit `git diff-tree` changed-path anchors. The ledger deliberately uses conservative `partial` and `runtime-unverified` dispositions where current Untether source plus observable tests/probes do not establish parity; historical subjects and documentation alone are not treated as implementation evidence. Ordinals 115–152 and 343–380 were reconstructed from actual diffs. This section supersedes no source behavior and makes no new implementation claim.