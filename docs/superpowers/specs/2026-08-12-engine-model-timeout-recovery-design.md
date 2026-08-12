# Engine Model Display and Timeout Recovery Design

## Goal

Show the active model for every engine and recover boundedly from explicit provider request timeouts without making Agy-specific retry behavior.

## Scope

- Display a selected model in runner metadata for every registered engine.
- Use `auto` when an engine has no trustworthy concrete active-model identifier.
- Recover failed provider timeouts through the existing engine-agnostic bridge retry path.
- Preserve subprocess-level retry safety: generic timeouts must not become runner-level transient failures.

## Model display

The renderer already displays `StartedEvent.meta["model"]` in the progress/final footer. Headers intentionally stay `label · engine · elapsed`; no renderer change is required.

Every runner must place its selected model in `StartedEvent.meta`:

1. A per-run `/model` override wins.
2. The configured engine model is next.
3. A native runtime-discovered model wins only where the runner already provides a reliable identifier.
4. Engines with no reliable concrete identifier report `auto`.

Agy currently resolves `/model` when constructing `--model`, but publishes only its configured model in metadata. Its emitted metadata will instead use the same precedence as `build_args`. With no configured/overridden model, it will report `auto`.

Grok and OMP have the same config-only metadata gap. They will also report `auto` without an explicit selection. This does not invent a vendor model name; it denotes account/provider routing.

## Timeout recovery

The shared transient-error retry path in `runner_bridge.handle_message()` remains the only post-failure recovery path. It has no engine filter, is already bounded by `[auto_continue] transient_error_max_retries`, and preserves cancellation, signal-death, delivery, and usage-budget guards.

The retry classifier will recognize narrowly scoped failed-request timeout phrases, including `timeout waiting for response`. It will inspect the sanitized error plus the answer text because Agy currently puts the specific stderr error in `CompletedEvent.answer` while its generic error is `agy failed (rc=N).`.

On a recognized timeout:

1. If a real resume token exists, invoke the existing shared session nudge: `continue` against that token.
2. If no real token exists and `timeout_fresh_retry` is true, rerun the original prompt as a fresh session.
3. If disabled, exhausted, cancelled, signal-killed, delivered, or budget-blocked, render the existing sanitized failure.

Agy creates a provisional UUID to hold the session lock. That value is not an upstream conversation id. If stderr/stdout did not provide a real conversation id, Agy must emit `CompletedEvent.resume=None`; callers must not persist or nudge the provisional UUID.

The fresh fallback is safe only in this no-real-session state. Once a real conversation id is scraped, the bridge nudges that session rather than reissuing the prompt.

## Configuration

Add these `[auto_continue]` settings:

```toml
[auto_continue]
transient_error_retry = true
transient_error_max_retries = 1
timeout_nudge = true
timeout_fresh_retry = true
```

`timeout_nudge` controls recognition and recovery of explicit failed-request timeout phrases. `timeout_fresh_retry` permits fresh fallback only where no authentic resume token exists. Both default to true. Retry count is shared with provider-transient recovery and remains bounded to 0–3.

## Non-goals

- Do not hardcode vendor model names for auto-routed engines.
- Do not scrape undocumented Agy settings or plain CLI output for a model.
- Do not add generic `timeout` to `utils.transient_failures`; that subsystem retries subprocesses only before visible output and must remain timeout-negative.
- Do not create engine-specific retry settings or duplicate the bridge recovery path.

## Tests

Tests are written and observed failing before production changes:

- Agy metadata uses the `/model` override over config and reports `auto` when unknown.
- Grok and OMP report explicit config/override models or `auto`.
- Explicit Agy timeout text is recognized through the answer text even when `CompletedEvent.error` is generic.
- Agy nudge retries use a real session token and the `continue` prompt.
- Agy without a scraped session retries the original prompt fresh only when configured.
- Disabled/exhausted recovery and signal deaths do not retry.
- Agy emits no resume token when no conversation id was scraped.
- Generic timeout stays non-transient in the runner-level classifier.

Update the configuration reference, operations/troubleshooting guidance, and changelog with the engine-wide semantics and retry bounds.
