# Takopi Feature Port Audit — Takopi → Untether

**Date:** 2026-08-10
**Untether baseline:** `f23881123522a5336201934d7c0b9962a25772fe32331f625`; gap-closure work is uncommitted on `takopi-gap-closure`.
**Takopi reference:** `3fea288a8aed5fd30f08fdc8dd5c9fbb716192ac`.
**Authoritative execution plan:** `D:/Projects/takopi/docs/plans/2026-08-10-takopi-untether-gap-closure-plan.md`.
**Historical comparison:** `D:/Projects/takopi/docs/reference/untether-comparison.md` remains evidence only; its pre-migration conclusions are not current truth.

## Method

Source behavior and deterministic tests in the Untether tree override earlier completion prose. Classifications are `implemented`, `partial`, `missing`, `runtime-unverified`, `superseded`, `not-applicable`, and `stale`. This ledger separates source behavior, deterministic proof, live-runtime evidence, and roadmap-only work; it does not print credentials, identifiers, or state values.

---

## A. Already-source-verified `present` (migration commit landed these)

These were ported by `daca548c` and are confirmed wired in current Untether source.

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
| D2 | Takopi `[logging]` config table | Untether logging env controls (already richer). |
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
| CI hard type gate and 3-OS static matrix | partial | `ty check src tests` reports 363 diagnostics on Windows; CI correctly keeps it informational | missing until diagnostics are fixed and the matrix is enforced |
| Live config/state/poller cutover | runtime-unverified | restricted backup made; only obsolete top-level `[logging]` removed; config loader and `untether doctor` passed; no competing process observed | needs authorized running-poller ownership check and Telegram smoke |

## F. Verification snapshot

- Focused migration validation passed: **529 passed, 15 skipped**. The later timeout-event regression also passed. The resolver fixture suite was refreshed for the new sticky plan/subagent store methods: **9 passed**.
- Formatting, Ruff, compilation, lockfile, Bandit, and `pip-audit` passed. Bandit reported only the existing scoped `# nosec` findings; `pip-audit` found no known vulnerabilities.
- `uv build`, `twine check`, `check-wheel-contents`, and a clean-wheel import smoke passed. A fresh Windows full-suite run completed but failed: **61 failed, 3163 passed, 34 skipped**. Eight failures were stale resolver test doubles and now pass in their focused suite; the remaining failures are cross-platform fixture/path assumptions, POSIX-only socket/process behavior, and unavailable timezone data. Full site generation remains unavailable because Zensical is absent from the active dependency environment; `scripts/docs_prebuild.py` passed.

## G. Roadmap-only carryover

Tasks 4, 20, and 23 appear exactly once under Untether’s **Future** roadmap section. They are requested future work, not shipped migration claims. Task 24 and E12/E13 are intentionally absent from the roadmap.

## H. Historical claims corrected

Earlier blanket-completion language was stale: parser/data-field presence did not prove meta dispatch, sticky plan behavior, one-card compact/handoff lifecycle, or Claude/OpenCode agent injection. The implemented source is covered by focused tests; the runtime-only and CI-type-gate limitations remain recorded in sections E–F. Skill is intentionally one-shot carried data; source-backed badges/context show plan and goal only.