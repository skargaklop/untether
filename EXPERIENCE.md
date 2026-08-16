# ACP Phase E Experience

## Difficulties

The ACP peer API is intentionally duplex: prompt responses and session updates can arrive through separate paths, while the existing runner contract requires one ordered event stream. The main difficulty was keeping reducer state independent from transport details and preserving the exact Started/Action/Completed lifecycle.

## Solutions

I used an in-memory peer factory seam for runner tests and a small ordered `AcpSessionState` projection. Namespaced action IDs (`tool:`, `plan:`, `terminal:`, and `note:`) make updates idempotent for progress tracking, while `EventFactory` remains the runner event boundary. Cleanup is kept in the async generator `finally` block so early consumer closure still closes the peer.

## Inconveniences

The implementation was available as a partially integrated Phase E slice rather than a single public runner reference: registry loading, settings, peer/protocol adapters, reducer state, and runner behavior live in separate seams, while the legacy compact ACP client remains for compatibility. The live ACP agent and credentials were not available for verification, so documentation distinguishes fixture-tested behavior from opt-in live probing and avoids claiming third-party compatibility. The registry/install cache format and the shared TTL also needed explicit documentation because fresh-cache startup intentionally performs neither network access nor `PATH` scanning.

## Difficulties

The plan specifies a broad protocol matrix, but the implemented public configuration surface is intentionally smaller than the full design: settings currently expose protocol, command, args, environment, registry enablement, and the shared TTL, while optional auth and Client capability controls are documented as capability-gated behavior rather than invented TOML keys. It was also easy for ACP-specific wording to imply an aggregate command, even though routing is deliberately ordinary per-engine directive dispatch.

## Solutions

I added a concise generic ACP runner reference, linked it from the runner documentation and site navigation, and extended the configuration reference with the two cache paths, shared three-day default, explicit executable rules, v1/v2 negotiation, safety restrictions, and normalized per-agent commands. The live-probe section is opt-in and records the required lifecycle evidence without making CI or startup depend on external agents, credentials, or network access.

Phase D integration exposed a useful seam but also a limitation: registry discovery helpers existed without a complete runtime-facing cache/backend assembly API. I kept the normal `EngineBackend`/router/menu/directive path and moved synchronous discovery behind `anyio.to_thread.run_sync` so an expired registry refresh cannot stall Telegram. Explicit ACP configurations remain authoritative, while unavailable or malformed registry data is ignored so static engines keep working. The remaining inconvenience is that the current implementation still needs broader cache-state coverage and platform-specific fixtures before it can claim the full plan gate.

- Windows AnyIO cancellation can invalidate an active task-group cancel scope when cleanup exits it from a timed request. The peer now uses a separately scheduled reader task and cancels it without cross-scope task-group exit; EOF is converted to a peer protocol error, while tests must pass `-o addopts=''` to avoid the repository coverage gate masking targeted results.

## ACP Phase C interaction experience

The interaction slice is deliberately bounded: reverse requests use opaque nonces, owner checks, duplicate rejection, and timeout cancellation rather than exposing wire payloads to callbacks. ACP turn control is cancellation-only; it sends the protocol `session/cancel` notification and explicitly does not advertise steering. Authentication remains a small login/logout/retry seam, while filesystem and terminal facilities remain capability-gated until their complete root and process lifecycle adapters are enabled.


## ACP adapter gap-fix experience

Inline `python -c` peer fixtures were brittle under Windows quoting; the file-based `tests/fixtures/acp_agent.py` subprocess fixture removed that failure class. `select_backends` had masked the untested production registry path — behavior-asserting tests now target `build_runtime_spec` directly. The `dynamic_engine_ids` guard in `/config` showed how getattr-defaulted guards silently rot; the field is now asserted in a transport-runtime test. Compaction did not need the old client's transport at all: resuming via the new peer, waiting for `available_commands_update`, and gating on an advertised `compact` command reproduces the legacy contract in ~100 lines, and `runners/_acp.py` is deleted.

## Session identifier closure experience

The identifier implementation worker stalled without yielding and made no changes; the requested code-review agent could not start because its runtime had no selected model. Inline execution used the approved source-only worktree and an explicit temporary `[:8]` restoration to prove the strengthened runner-label test was genuinely RED before restoring the full-ID implementation. Telegram rendering deliberately strips Markdown backticks into code entities, so the copyable-footer regression must assert rendered text plus entity span, not the pre-render Markdown source.

## Session, compact, and prompt-batch closure experience

Three scoped implementation workers did not execute: two exited without tool calls or a yield, and the Codex-rescue route lacked its required Bash capability. The controller retained the approved isolated-worktree TDD plan, reviewed the resulting test-only diff inline, and used the source checkout's `uv run` gates instead of stale installed code. The strengthened regressions exposed no current production truncation or dispatcher defect, so the minimal outcome is durable contract coverage rather than speculative production rewrites. The graph query route could not be used because this worktree has no `graphify-out/graph.json`; its sidecar warning was non-blocking.

## ACP registry discovery closure experience

The official registry uses a singular `distribution` object, which the parser already accepts. The remaining production gap was that `build_runtime_spec()` did not supply the passive installation inventory to registry discovery, leaving package-backed agents unavailable. The runtime now carries one local npm/Bun/uv/pipx/Cargo metadata inventory into discovery; package matching accepts the installed package despite a registry version update, but still requires exactly one proven launcher. The existing Telegram command menu already enumerates and deduplicates available runtime engine IDs, so no menu production change was required. Focused ACP/runtime/menu coverage proves Cline becomes a dynamic engine and an ordinary slash command.

The direct Cline probe showed the local package version can lag the pinned registry version. Exact version matching therefore rejected a verified installed launcher even though its package and sole executable were unambiguous; matching now keys on ecosystem and normalised package identity, retaining the one-launcher ambiguity guard. Scoped packages are not guessed: all package-derived commands originate from inventory metadata. Worker routes remained unavailable, so the controller used the approved isolated-worktree TDD sequence and recorded every RED/GREEN result inline.
