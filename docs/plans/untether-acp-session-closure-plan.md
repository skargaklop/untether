# Untether ACP Discovery and Remaining Closure Plan

## Context

Substantial work from the previous plan is already done in the isolated worktree: startup/source baselines passed; full-session-ID regressions were added; compact/handoff regressions were added; prompt-batch integration regressions were added; focused behaviour gates and static/full gates were run; and one source poller startup/shutdown smoke succeeded. The remaining live Telegram interaction gate was blocked because Orca could not deliver keyboard input after its focus retry.

A new startup defect was then found: the official ACP registry publishes Cline under singular `distribution.npx`, while `src/untether/acp_registry.py` parses only legacy `distributions`, so installed `cline.cmd` does not become a runtime engine or Telegram command. This plan preserves and verifies completed work, fixes that new defect first, then closes only remaining verification.

Execute only in `C:/Users/DELL E5570/.config/superpowers/worktrees/untether/fix-session-compact-batch` on branch `fix/session-compact-batch`. Use strict TDD and `uv run ...`; preserve all existing modified tests and `EXPERIENCE.md`. `D:/Projects/takopi` remains read-only.

## Approach

### 1. Preserve completed previous-plan work

1. Re-read the current diff before editing. Existing modified files are `tests/test_runner_utils.py`, `tests/test_pi_runner.py`, `tests/test_omp_runner.py`, `tests/test_meta_line.py`, `tests/test_rendering.py`, `tests/test_telegram_compact_dispatch.py`, `tests/test_telegram_prompt_batch_integration.py`, and `EXPERIENCE.md`.
2. Treat those modifications as completed prior work, not tasks to recreate. Do not overwrite, revert, or duplicate their regressions.
3. Run the existing focused behaviour command once after the ACP change to ensure this completed work remains green. If it fails, repair only the observed regression; do not restart the old implementation plan.

### 2. Repair official ACP registry discovery

1. Add a failing regression to `tests/test_acp_registry.py` using the official Cline shape: `id = "cline"`, `version = "3.0.55"`, `distribution.npx.package = "cline@3.0.55"`, and `args = ["--acp"]`. Patch `shutil.which` so only `cline` resolves to an absolute temporary `cline.cmd`. Assert parsing yields a selectable distribution with `cmd == "cline"`, preserves `args == ("--acp",)`, and `discover_installation()` stores the full resolved executable path.
2. Run that single test before production edits and confirm RED because singular `distribution` currently produces no selectable distribution.
3. Extend the existing parser in `src/untether/acp_registry.py`; do not create a second parser:
   - Retain compatibility with existing `distributions: [...]` inputs.
   - Parse official `distribution.binary[target]` into `RegistryDistribution(target, "binary", cmd, args, env)`.
   - Parse `distribution.npx` only when `package` is a deterministic unscoped npm package optionally followed by `@version`. Remove only the final version suffix, so `cline@3.0.55` maps to direct executable `cline`.
   - Preserve registry `args` and `env`.
   - Skip scoped packages, malformed names, `uvx`, and ambiguous package-to-binary mappings rather than guessing.
4. Generalise `choose_binary_distribution()` only enough to prefer an exact-platform binary and otherwise select the safe package-derived candidate. Keep `discover_installation()` passive: cached absolute-path checks and `shutil.which(candidate.cmd)` are allowed; invoking `npx`, `uvx`, npm, registry commands, shells, downloaders, or installers is forbidden.
5. Add a runtime regression to `tests/test_runtime_loader.py` using its existing registry/cache fixtures. Feed one Cline-shaped agent and resolvable `cline.cmd`; assert `build_runtime_spec(...).dynamic_engine_ids == frozenset({"cline"})` and the router exposes `cline`.
6. Run existing Telegram command-menu coverage. Menu production code remains unchanged unless a failing test proves a separate defect, because the menu already enumerates runtime engine IDs.
7. Update `docs/reference/config.md` and `docs/reference/runners/acp.md`: safe unscoped package entries may bind an already-installed direct executable from PATH; scoped or ambiguous entries require explicit configuration; startup never executes package managers or downloads packages.

### 3. Verify completed session, compact, and batch work

1. Run the current focused behaviour suites without rewriting their tests:
   - exact full session IDs in runner, Pi, OMP, meta, and rendering paths;
   - compact/handoff dispatcher behaviour;
   - prompt-batch dispatcher integration;
   - scheduler invariants.
2. If any test fails due to the ACP parser change, fix only the shared affected source. If failures are unrelated and expose unfinished prior work, use the existing failing regression as RED and make the minimum production fix through current runner/dispatcher/scheduler/store abstractions.
3. Preserve complete `ResumeToken.value` in persisted and copyable output; compact/handoff continues through existing confirmation, scheduler, routing stores, cards, and `TelegramOutbox`; batching continues through the sole `PromptInputBatcher` and re-enters the dispatcher exactly once.

### 4. Complete remaining evidence

1. Append to the existing `EXPERIENCE.md` closure section, preserving prior text. Record the ACP schema mismatch, safe direct-executable mapping, failed subagent routes, Windows PATH/cache pitfalls, and the actual workaround.
2. Run static, full, and lock gates after focused tests.
3. Start exactly one source poller and verify sanitized startup logs contain `cline` in both `setup.summary` and `startup.command_menu.updated`; stop it cleanly.
4. Retry the four authorized live Telegram scenarios only if an interactive input path is available: one multi-message batch, compact approve/decline, successful handoff followed by routed message, and complete OMP footer resume. If Orca still cannot deliver input, retain this gate as explicitly blocked; do not run a second poller or claim it passed.

## Critical files & anchors

- `src/untether/acp_registry.py` — `_RegistryAgentModel`, `parse_registry_agents`, `choose_binary_distribution`, `discover_installation`, `registry_backend`; current Cline exclusion point.
- `src/untether/runtime_loader.py` — `build_runtime_spec`; converts discovered registry backends into runtime IDs/router entries.
- `tests/test_acp_registry.py` — parser and passive executable-discovery contract.
- `tests/test_runtime_loader.py` — end-to-end registry-to-`RuntimeSpec` proof.
- `src/untether/telegram/bridge.py` — `build_bot_commands`; verify, but do not modify unless independent filtering fails.

## Verification

Run from the isolated worktree.

1. ACP RED/GREEN gate:
   `uv run pytest tests/test_acp_registry.py tests/test_runtime_loader.py tests/test_telegram_command_menu.py -q --no-cov`
   Expected result: an official Cline-shaped entry plus PATH-resolvable `cline.cmd` yields runtime engine `cline` and a menu-eligible command without starting npm/npx.
2. Preserve completed behaviour:
   `uv run pytest tests/test_runner_utils.py tests/test_pi_runner.py tests/test_omp_runner.py tests/test_meta_line.py tests/test_rendering.py tests/test_telegram_compact_dispatch.py tests/test_compact.py tests/test_telegram_prompt_batch.py tests/test_telegram_prompt_batch_integration.py tests/test_scheduler_queue.py -q --no-cov`
3. Static/full/lock gates:
   - `uv run ruff format --check src tests`
   - `uv run ruff check src tests`
   - `uv run ty check src tests`
   - `uv run pytest`
   - `uv lock --check`
4. Runtime startup: with no existing Untether poller, run one `uv run untether`, wait for `setup.summary` and `startup.command_menu.updated`, confirm both include `cline` in sanitized output, then stop cleanly.
5. Live Telegram interaction, when input is available: verify one assembled multi-message run, compact approve and decline, successful handoff routing, and full-ID OMP resume. Never expose credentials, private IDs, prompts, answers, state contents, or endpoints.

## Assumptions & contingencies

- Current modified closure tests and `EXPERIENCE.md` are completed prior-plan work and must be preserved.
- Current workstation evidence confirms `C:/Users/DELL E5570/AppData/Roaming/npm/cline.cmd` exists and `cline --help` works. If package-to-executable mapping is not deterministic, require explicit ACP configuration instead of probing npm metadata.
- `uv run untether doctor` may exit 1 solely because optional local/OpenAI voice providers are unavailable; use Telegram and engine-discovery diagnostics as startup evidence.
- If another poller cannot be stopped safely, skip runtime/live interaction rather than create concurrent Telegram consumers.
- If Orca cannot deliver keyboard input again, report the live Telegram interaction gate as blocked with the already verified clean source-poller startup/shutdown evidence; do not downgrade or fabricate the result.
