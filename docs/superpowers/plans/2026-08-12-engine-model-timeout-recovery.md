# Engine Model Display and Timeout Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Display the active model state for every engine and recover boundedly from explicit provider request timeouts.

**Architecture:** Runner `StartedEvent` metadata is the sole model-display interface. The existing engine-neutral bridge retry path receives narrowly classified failed-request timeouts from the terminal error and answer, nudges authentic sessions, and retries fresh only when no real session exists.

**Tech Stack:** Python, Pydantic, AnyIO, pytest, structlog, TOML.

---

### Task 1: Specify auto-routed engine model metadata

**Files:**
- Modify: `tests/test_agy_runner.py`
- Modify: `tests/test_grok_runner.py`
- Modify: `tests/test_omp_runner.py`
- Modify: `tests/test_meta_line.py`
- Modify: `src/untether/runners/agy.py`
- Modify: `src/untether/runners/grok.py`
- Modify: `src/untether/runners/omp.py`

- [ ] **Step 1: Write focused failing tests**

Add tests that invoke each runner with a `/model` run option plus a configured model and assert `StartedEvent.meta["model"]` picks the run option. Add no-model tests that assert `"auto"`; add one footer test asserting `format_meta_line` renders `auto`.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_agy_runner.py tests/test_grok_runner.py tests/test_omp_runner.py tests/test_meta_line.py -k 'started_meta or auto_model' -v`

Expected: tests fail because Agy ignores per-run metadata and all three omit unknown models.

- [ ] **Step 3: Implement only the required metadata resolution**

Resolve `run_options.model`, then config, then the literal UI fallback `"auto"`. Do not pass `"auto"` to engine CLIs.

- [ ] **Step 4: Verify GREEN**

Run the Step 2 command. Expected: PASS.

### Task 2: Specify authentic Agy resume behavior

**Files:**
- Modify: `tests/test_agy_runner.py`
- Modify: `src/untether/runners/agy.py`

- [ ] **Step 1: Write failing tests**

Add failed-run fixtures with and without a scraped conversation id. Assert a real id remains in `CompletedEvent.resume` and an unscripted provisional UUID results in `resume is None`.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_agy_runner.py -k 'resume and conversation' -v`

Expected: the no-conversation test fails because the provisional UUID leaks as a resume token.

- [ ] **Step 3: Implement token-authenticity tracking**

Keep the provisional UUID only for the internal early lock. Yield it as a completed resume only after promotion or when continuing an incoming real session.

- [ ] **Step 4: Verify GREEN**

Run the Step 2 command. Expected: PASS.

### Task 3: Specify shared timeout recovery

**Files:**
- Modify: `tests/test_exec_bridge.py`
- Modify: `tests/test_settings.py`
- Modify: `tests/test_transient_failures.py`
- Modify: `src/untether/runner_bridge.py`
- Modify: `src/untether/settings.py`

- [ ] **Step 1: Write failing integration tests**

Use `_StatefulRunner` to assert that `Error: timeout waiting for response` in a failed Agy answer with generic `agy failed (rc=1).` causes exactly one `continue` nudge for a real token. Add a no-token case that runs the original prompt fresh. Add disabled, exhausted, no-fresh, and signal-death cases. Add a classifier unit test and pin `classify_transient_failure()` as timeout-negative.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_exec_bridge.py -k 'timeout and transient' -v`

Expected: failure because the bridge examines only `CompletedEvent.error`, has no explicit timeout pattern, and requires a resume token.

- [ ] **Step 3: Implement minimum shared recovery**

Add `timeout_nudge` and `timeout_fresh_retry`, default true. Match only explicit request-timeout phrases across bounded error/answer text. Reuse the existing retry counter and all existing cancellation, signal, delivery, and budget guards. Nudge valid sessions; retry fresh only without a token when configured.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_exec_bridge.py -k 'timeout and transient' -v && uv run pytest tests/test_settings.py -k 'timeout' -v && uv run pytest tests/test_transient_failures.py -v`

Expected: PASS.

### Task 4: Document and verify

**Files:**
- Modify: `docs/reference/config.md`
- Modify: `docs/how-to/operations.md`
- Modify: `docs/how-to/troubleshooting.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Document settings**

Document the shared 0–3 retry cap, `timeout_nudge`, `timeout_fresh_retry`, and model `auto` state.

- [ ] **Step 2: Verify changed contracts**

Run focused runner/bridge tests, documentation build, Ruff, Ty, and the full 81% coverage gate before committing/pushing master.
