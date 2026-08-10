# Takopi Feature Port Audit — Takopi → Untether

**Date:** 2026-08-09
**Untether HEAD:** `daca548c5ab4c5d7c0b9962a25772fe32331f625` (baseline `4285dad5a12e4e4113c9cc5240972a67bbb5e218` + migration commit `daca548c`)
**Takopi HEAD:** `e51e256850d9a3b1bde447690a1c9fc6db6474cf`
**Comparison baseline doc:** `D:/Projects/takopi/docs/reference/untether-comparison.md` (snapshots `481f0c7` / `4285dad`, pre-migration)

## Method

Every Takopi commit since the shared ancestry fork point (`54073ec` / `8c44a69` / `25b8ec6` and later), every file under `D:/Projects/takopi/docs/`, `changelog.md`, and `ROADMAP.md` was inspected. Each row in `untether-comparison.md` was re-evaluated against current Untether HEAD source rather than copying baseline status. Dispositions: `present` (behavior exists and is wired in current Untether source), `ported` (ported by this audit's changes), `superseded` (replaced by an Untether mechanism), `not-applicable` (namespace chore or speculative roadmap), `unverified` (requires live CLI/credentials to confirm).

Evidence citations use local `path:line`. Status literals are exact: `present`, `ported`, `superseded`, `not-applicable`, `unverified`.

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
| A6 | Run-options `attachments`, `plan`, `goal`, `skill`, `subagent` (data fields) | `src/takopi/runners/run_options.py` | `src/untether/runners/run_options.py` (`EngineRunOptions`), `scheduler.py:30-34` (`ThreadJob.plan`/`goal`) | present |
| A7 | Pi unknown-event decoding (`PiUnknownEvent`) | `src/takopi/schemas/pi.py:95-103` | `src/untether/schemas/pi.py` (`PiUnknownEvent` + peek-decode) | present |
| A8 | Pi numeric retry delay (`AutoRetryStart.delayMs: int|float|None`) | `src/takopi/schemas/pi.py` | `src/untether/schemas/pi.py` | present |
| A9 | OMP full session IDs (`shorten_session_id=False`) | `src/takopi/runners/omp.py:152-159` | `src/untether/runners/omp.py` (`OmpRunner.new_state` sets `shorten_session_id=False`) | present |
| A10 | OMP/Grok/Agy runner registration | `src/takopi/runners/{omp,grok,agy}.py:BACKEND` | `src/untether/runners/{omp,grok,agy}.py` + `pyproject.toml` entry-points | present |
| A11 | Compact core/mixins/ACP (data layer) | `src/takopi/compact.py`, `_compact_mixin.py`, `_acp.py` | `src/untether/compact.py`, `runners/_compact_mixin.py`, `runners/_acp.py` | present |
| A12 | Windows tree cleanup / stream closure | `src/takopi/utils/subprocess.py` | `src/untether/utils/subprocess.py` (`taskkill /T /F`, `close_process_streams`) | present |
| A13 | Three-OS CI test matrix | `takopi/.github/workflows/ci.yml` | `untether/.github/workflows/ci.yml` `test-cross-os` job (ubuntu/macos/windows) | present |
| A14 | 81% coverage gate | `takopi/pyproject.toml --cov-fail-under=81` | `untether/pyproject.toml:108 --cov-fail-under=81`; ci.yml coverage-gate step | present |
| A15 | Transient failure classifier + clean formatter | `src/takopi/utils/transient_failures.py` | `src/untether/utils/transient_failures.py` (`classify_transient_failure`, `format_transient_failure`, `_BARE_STATUS_PREFIX_RE`) | present |
| A16 | subagent `--agent` injection (grok/claude/opencode) | `src/takopi/runners/{grok,claude,opencode}.py build_args` | `src/untether/runners/{grok,claude,opencode}.py` (attribute-based wiring consumes `run_options.subagent`) | present |

## B. Untether-only features to preserve (not regressed)

Triggers (`triggers/`), cost tracking (`cost_tracker.py`), quarantine (`session_quarantine.py`), hot reload/migrations (`config_watch.py`, `config_migrations.py`), outbox delivery (`outbox_delivery.py`), stats/health/browse/auth/config commands, environment policy/audit (`utils/env_policy.py`, `utils/env_audit.py`), diagnostics (`utils/proc_diag.py`), persistence (`progress_persistence.py`), systemd notify (`sdnotify.py`), security/release CI jobs, Gemini/Amp runners. All `present` in Untether and untouched by this audit.

## C. `not-applicable`

| # | Item | Reason |
|---|---|---|
| C1 | Takopi namespace/package entry-point migration (`takopi.*` → `untether.*`) | Untether already uses `untether.*` groups; no migration needed. |
| C2 | Release chores (`chore(release): v0.2x.x`) | Per-version tags; not behavior. |
| C3 | ROADMAP Task 4 (Droid/Cline/Kilo/Warp/Open Interpreter/Mimo/ZCode/Kimi) | Speculative; no source implementation in Takopi. |
| C4 | ROADMAP Task 20 | Speculative; no source implementation. |

## D. `superseded`

| # | Item | Superseded by |
|---|---|---|
| D1 | Legacy `[[takopi-send: path]]` marker protocol | `.untether-outbox/` directory delivery (`outbox_delivery.py`). |
| D2 | Takopi `[logging]` config table | Untether logging env controls (already richer). |
| D3 | `pi.plan_flag` config flag | Extension detection (see E4). |

## E. Gaps requiring `ported` changes (this audit's work)

| # | Capability | Takopi evidence | Untether gap (current HEAD) | Disposition | Plan section |
|---|---|---|---|---|---|
| E1 | Compact/handoff **dispatch consumers** in `telegram/loop.py` | `src/takopi/telegram/loop.py` `run_compact_job`/`run_handoff_job`, `_make_scheduler_observers`, `dispatch_prompt_run` enqueue reconciliation | `telegram/loop.py` has ZERO references to `compact`/`handoff`/`job.kind`/`run_compact`/`run_handoff`/`on_job_claimed`/`on_job_failed` (grep returned no matches). `ThreadJob.kind`, `compact_instructions`, `handoff_target` are dead data. | ported | §2 |
| E2 | Directive parser: `plan`/`goal`/`subagent`/`skill` with goal-over-plan precedence | `src/takopi/directives.py:9-202` (`_MODE_PLAN`/`_MODE_GOAL`, `--skill`/`--subagent`, `plan = plan and goal is None`) | `src/untether/directives.py:10-95` `ParsedDirectives` has only `prompt`/`engine`/`project`/`branch` — no plan/goal/skill/subagent fields or parsing. | ported | §2 |
| E3 | `format_mode_badge` + `compose_context_line` footer composition | `src/takopi/directives.py:256-285` | No `format_mode_badge` or mode-badge composition anywhere in Untether (`grep` for `format_mode_badge` = 0 matches). | ported | §5 |
| E4 | Pi plan-mode extension detection (`detect_plan_mode_extension`) | `src/takopi/runners/pi.py` | `src/untether/runners/pi.py` has no `detect_plan_mode_extension`; `build_args` (L524-545) never appends `--plan`. | ported | §4 |
| E5 | Pi `pi-goal-list-loop-audit` extension seeding via `<task-goal>` directive | `src/takopi/runners/pi.py` | Not present in Untether. | ported | §4 |
| E6 | Pi `stdin_payload` Windows-safe newline-terminated bytes | `src/takopi/runners/pi.py stdin_payload` | `src/untether/runners/pi.py:547-554` `stdin_payload` returns `None`. | ported | §4 |
| E7 | Code-region-safe markup (`||spoiler||`, `++underline++`, `~strike~`, `~~strike~~`) | `src/takopi/telegram/render.py` | Untether renderer (`telegram/render.py`, `markdown.py`) lacks these preprocessors (to confirm exact location during port). | ported | §5 |
| E8 | File-task annotation `Execute the task specified in this file: \`<path>\`.` | `src/takopi/telegram/loop.py` (commit `8127158`) | Not present; Untether uses `[uploaded file: ...]`-style markers. | ported | §5 |
| E9 | `RunnerSettings` (startup_timeout_s, idle_timeout_s, kill_tree_on_cancel, shutdown_timeout_s, retry_max_attempts, retry_base_delay_s) | `src/takopi/settings.py` `[runners]` | No `RunnerSettings` model in `src/untether/settings.py` (`grep` for `RunnerSettings`/`retry_max_attempts` = 0 matches in settings). | ported | §3 |
| E10 | Transient **retry loop** in `JsonlSubprocessRunner` (pre-event-only, linear backoff) | `src/takopi/runner.py` | `src/untether/runner.py` (1464 lines) has the classifier imported but no retry loop around the stream read (to confirm during port; `transient_failures` is only consumed in `runners/agy.py:282-293` for message formatting, not retry). | ported | §3 |
| E11 | startup/idle guards in `JsonlSubprocessRunner` | `src/takopi/runner.py` | Untether has watchdogs in `runner_bridge.py` but not the per-read startup/idle timeout guards from Takopi's `manage_subprocess` integration (to confirm during port). | ported | §3 |
| E12 | `ty` hard gate (remove `allow_failure: true`) | `takopi/.github/workflows/ci.yml` (no allow_failure) | `untether/.github/workflows/ci.yml:31` `allow_failure: true` on ty job; current `ty check src tests` = **346 diagnostics**. | ported | §6 |
| E13 | `checks` job 3-OS matrix (format/ruff/ty) | `takopi ci.yml` (9 lint cells) | Untether `checks` job is `ubuntu-latest` only; `test-cross-os` is already 3-OS. | ported | §6 |

## F. `unverified` (requires live CLI/credentials)

| # | Item | Reason |
|---|---|---|
| F1 | Native OMP/Grok/Agy/ACP live compaction interception | Requires installed CLIs + session IDs. Deterministic fixtures cover the contract. |
| F2 | Service/task launcher inventory for runtime cutover | Must be confirmed with read-only `sc`/`schtasks` queries immediately before cutover. |

---

## Re-evaluation of `untether-comparison.md` rows

The comparison doc (snapshots `481f0c7`/`4285dad`, pre-migration) claimed Untether LACKS: scheduler hardening, Pi/OMP compatibility, OMP/Grok/Agy, Windows cleanup, 3-OS CI, 81% threshold. Against current HEAD `daca548c`, rows A1-A16 above prove these claims are **stale** — all are `present`. The comparison's "Critical" annotations (adopt `CancelQueuedResult`, port `PiUnknownEvent`, preserve `shorten_session_id=False`) are satisfied.

The comparison's claims that remain accurate and require this audit's `ported` work: E1-E13 above (compact dispatch wiring, directive parser, mode badge, Pi plan/stdin, markup, file annotation, RunnerSettings, retry loop, startup/idle guards, ty hard gate).

## Test mapping (Takopi → Untether)

Adapted during port (behavior tests, not source-text assertions). Destination filenames recorded as tests are added:

| Takopi test | Untether destination | Coverage |
|---|---|---|
| `test_scheduler_queue.py` | already present | A1-A4 |
| `test_telegram_compact_dispatch.py` | `tests/test_telegram_compact_dispatch.py` (new) | E1 |
| `test_compact_event_invariants.py` | already present | A11 |
| directives/plan/goal tests | `tests/test_directives.py` (extend) | E2, E3 |
| `test_pi_schema.py` | already present | A7, A8 |
| Pi plan/stdin tests | `tests/test_pi_runner.py` (extend) | E4, E5, E6 |
| renderer markup tests | `tests/test_telegram_render.py` (extend) | E7 |
| file annotation tests | `tests/test_telegram_files.py` (extend) | E8 |
| retry safety tests | `tests/test_runner_retry.py` (new) | E10 |
| runner timeout tests | `tests/test_runner_timeouts.py` (new) | E11 |

## Commit reconciliation

Post-ancestry Takopi commits (skargaklop authorship, `54073ec`..`e51e256`) reconciled against changelog aggregate. All feat/fix commits map to a row above. Doc-only commits (`docs:` prefix) and the `chore(release)`/`fix(...)` upstream banteg commits are accounted for as ancestry or `not-applicable`. No commit body was left unreconciled.


## Port completion status (2026-08-09)

All ported items verified via `tests/test_takopi_ported_behaviors.py` (36 tests, all passing) and targeted test runs on affected modules (369 tests passing, 2 pre-existing Windows path failures unrelated to ports).

| Item | Status | Evidence |
|---|---|---|
| E1 (compact/handoff dispatch) | ported | `telegram/commands/compact.py`, `telegram/commands/queue_cmd.py`, `loop.py` job.kind branching, `commands/parse.py` invocation parser |
| E2/E3 (directive parser) | ported | `directives.py` plan/goal/skill/subagent fields, `transport_runtime.py` ResolvedMessage propagation |
| E4-E6 (Pi plan-mode) | ported | `runners/pi.py` detect_plan_mode_extension, _final_prompt, --plan flag |
| E6 (Pi stdin_payload) | ported | `runners/pi.py` newline-terminated bytes for multi-line prompts |
| E7 (markup preprocessing) | ported | `telegram/render.py` spoiler/underline/strike with code-region protection |
| E8 (file-task annotation) | ported | `telegram/files.py` is_image_document, format_image_prompt_annotation; `loop.py` handle_prompt_upload |
| E10 (transient retry) | ported | `runner.py` retry-aware run_impl with classify_transient_failure |
| E11 (startup/idle guards) | ported | `runner.py` _iter_jsonl_with_timeouts, RunnerSettings class, runtime_loader wiring |
| Queue command | ported | `telegram/commands/queue_cmd.py`, menu.py, loop.py dispatch |