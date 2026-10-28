# ACP Phase E Experience

## OMP production lookup versus collector-only evidence

The read-only compatibility scout found that production lookup parsed a title slot as the session header, while the SDK runtime test bypassed lookup through the bundle collector. The repair worker reproduced SESSION_NOT_FOUND, then reused collector validation to accept only the fixed 256-byte title slot followed immediately by a bounded native header. Controller rerun passed 45 checks with five Windows symlink privilege skips. Runtime now routes export/import through OmpPersistenceAdapter with independent SDK cold loading; neither this result nor SDK append establishes model continuation or full asset transfer.

## OpenCode replay verification boundary

The runtime worker found that replay verification in the importing writer process did not establish independent persistence. Import now stops that owned child, launches another, and compares full canonical events again before certifying restart. Controller rerun passed 14 checks with one Windows symlink privilege skip, including original completed shell output, unrelated destination-session preservation, and a follow-up shell command surviving another restart. Fresh non-repository working directories timed out during worker diagnostics; repository-cwd evidence must not be generalised to arbitrary project authorization. Remaining source-scoping, duplicate-collision, port ownership and HTTP size limits need separate fixes. Native shell follow-up is not provider/model continuation.

## Codex installed-engine resume diagnostic

The bounded diagnostic worker isolated RPC -32600 to a synthetic rollout named without the native timestamp component. Use `rollout-YYYY-MM-DDTHH-MM-SS-<uuid>.jsonl`; do not weaken identity or exact-prefix assertions to accommodate a malformed fixture. The production probe must send `initialized` as a notification, not wait for a response. A fresh app-server can verify a second resume without duplicating an RPC client in the test. Controller rerun: Codex runtime and adapter suite passed (25 passed, one platform privilege skip). This proves disposable synthetic-journal restoration and independent resume, not conversational/model follow-up fidelity.

## All-engine completion boundary

The all-engine contract was incorrectly claimed satisfied while production OMP/Codex/OpenCode registrations and real follow-up turns were still missing. The operator's statement that Codex/OpenCode could be tested locally was not permission to defer them. A coordinator probe using a constant-true health command and `pi --version` demonstrates framing/import only, not native continuation. Read-only adapter research also suggested copying OpenCode's whole database; that would violate per-session migration and preservation of unrelated destination sessions, so session-scoped persistence must be investigated instead. OMP collector/restore routines are reusable but do not become a production adapter merely by existing; registration, gating, durable identity metadata, reclaim and a genuine scoped restore probe require verification.

## Session-transfer controller checkpoint

Repeated flash worker packets consumed their turn budgets during inspection without writing the assigned patch, including mandatory gate configuration and runner integration. Their reports are not implementation evidence. The controller implemented those narrow changes and reran checks. Test-only workers also left a broken placeholder and a tautological `closed or True` assertion; both were corrected, and teardown is now observed through the fake generator's `finally`. Keep packet scope to one explicit behavioural invariant and inspect the actual diff before accepting output.

The migration worktree lacked the corrected product contract and existing discovery modules, although they existed uncommitted in the main checkout. Contract text and the bounded catalog/scanner modules were copied without altering the originals; discovery tests use disposable roots. Never infer branch contents from another checkout.

Argon was consulted personally through the existing authenticated Untether thread. Its source-fencing and cold-restart suggestions are useful review input, not engine verification. Its generic path-rewrite suggestion conflicts with the fidelity contract and was not adopted. See [[docs/reference/session-migration-advisor-review]].

## Native-transfer verification pitfalls

A restored Pi v3 record can load correctly yet fail byte fidelity: cold startup appended a missing `thinking_level_change` record. The disposable runtime fixture now declares that state before export; installed-Pi history/identity checks and post-restart byte equality pass together. Do not remove the byte check to accommodate a synthetic fixture.

Operation-bound receiver exports now require an explicit gate and a held snapshot context. Older CLI/subprocess fixture adapters must implement that contract rather than bypass production guards. When logical and native IDs differ, both identities need admission and durable fences; fencing only the logical ID leaves the actual native session runnable. Controller verification passed 16 receiver/protocol tests after that repair.

A coverage investigation returned stale claims about absent executable registration and Pi runtime evidence while workers were modifying those areas. Re-read current files and rerun checks before adopting research summaries. Fixture subprocesses remain plumbing evidence, not installed-engine round-trip evidence.

## Shared runner cancel-scope cleanup

A successful `CompletedEvent` is emitted while the per-attempt async generator may still own subprocess and AnyIO task-group cancel scopes. Returning or breaking from the retry wrapper at that yield abandons the nested generator, so Python may finalize it in a different task and raise `Attempted to exit a cancel scope that isn't the current task's current cancel scope` after the final Telegram answer was already delivered. Explicit `aclose()` is also unsafe here because injecting `GeneratorExit` through an active AnyIO task group becomes a `BaseExceptionGroup`; the minimal safe fix is to mark the terminal event, drain the attempt generator to natural completion in the same task, ignore any post-terminal events, and only then return or sleep before retrying. The deterministic regression asserts cleanup has run before `run_impl()` returns and in the same task that consumed the generator. Investigation required explicit Windows `HOME`/`USERPROFILE`, focused pytest needed `-o addopts=''`, and two attempted cheap-model routes were unavailable (one unknown alias and one retired upstream), so the default general-purpose route completed the read-only diagnosis.

## Explicit ACP launcher support

The existing `[acp.engines.<id>]` seam already modeled a fixed command plus argv, but Windows cannot execute npm-generated `.cmd` launchers directly through `asyncio`/AnyIO. The narrow fix is to retain absolute-path validation and translate only Windows `.cmd`/`.bat` entries to fixed `ComSpec /d /c <launcher> <args>` argv; native binaries and executable npm/uv/bun launchers remain direct. A live probe confirmed local `dsh --profile acp` negotiates ACP v1. Investigation was briefly interrupted because the background Explore agent disappeared before its result could be retrieved; direct targeted reads of the known ACP settings, registry, backend, tests, and docs supplied the required evidence.

## Compact/handoff lifecycle feedback

`ThreadScheduler.enqueue()` can start a claimable job before returning, so editing an operation card after enqueue may overwrite `claimed`, running, or terminal states. The confirmation callback now writes `creating handoff summary…` before enqueue and leaves all later progress, success, failure, and cancellation edits to the scheduler lifecycle. The scheduler's generic `claimed` observer must not overwrite compact/handoff operation text before the worker starts, and native compact now begins with the direct `compacting <engine> session…` status. Cross-engine wording stays neutral because the source engine creates the summary and the destination only receives it. Focused pytest needs `-o addopts=''` to avoid the repository-wide coverage gate; Windows Python commands still require explicit `HOME` and `USERPROFILE`. The first read-only subagent route was unavailable because its selected model had reached end of life; the replacement verifier completed without modifying files.

## Untether Pi compact/handoff investigation

The existing Pi extractor tests covered a bare marker but not the actual Telegram-rendered footer with its leading `↩️` prefix. `AutoRouter.resolve_resume()` already normalized that prefix, but `/compact` and `/handoff` bypassed the router and called runner extractors directly. The fix reuses the router normalization through `TransportRuntime.resolve_resume_from_reply()`, and the compact regression now uses a realistic rendered footer. The failure reproduced entirely in memory; no session files or local state inspection was required.

## Telegram direct shell command experience

### Difficulties

The foreground process must keep draining its combined pipe after reaching the configured retention limit or a verbose child can deadlock. Timeout and caller cancellation also need to leave the existing process-tree manager in control of cleanup. Focused pytest initially failed because the system Python had neither the editable package metadata nor repository import path.

### Solutions

A single direct-shell module now owns parsing, fixed argv construction, discovery, cwd selection, bounded draining, detached flags, execution, and plain-text formatting. Focused RED/GREEN runs use the repository's existing `uv run` environment. The ordinary Telegram route remains the authorization boundary, and the dispatcher starts the handler only after that check. Final adversarial checks caught that AnyIO's public `open_process` API does not accept `close_fds` and that disabling orphan reaping could leave a descendant alive after its shell exited; detached launching now uses `subprocess.Popen`, foreground teardown keeps process-group orphan reaping enabled, and unexpected launch failures become a generic safe reply.

### Inconveniences

`USERPROFILE` and `HOME` must be set explicitly for every Python command. Existing untracked `.local/` and `.trash/` directories were preserved. No live PowerShell executable is available on non-Windows CI, so platform behavior is covered through discovery and process-launch seams rather than an actual PowerShell invocation.


## Inactivity/stall policy change experience

### Difficulties

The warning cap remained a class-level default while other watchdog values were copied from live per-run settings, so testing had to exercise the `handle_message` wiring rather than only model validation. The repository's default pytest options also include a coverage gate that is inappropriate for focused RED/GREEN runs.

### Solutions

Added one settings contract test for the five-minute repeat default, 15-warning default, and 1–100 bounds, plus one bridge test proving a live `WatchdogSettings` value reaches `ProgressEdits`. Focused tests use `-o addopts=''`, and the implementation is limited to the Pydantic field, per-run assignment, aligned fallback default, and config reference.

### Inconveniences

Python commands require explicit Windows `USERPROFILE` and `HOME`; `.local/` and `.trash/` already contained untracked state and were deliberately preserved.

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

The official registry uses a singular `distribution` object, which the parser already accepts. The remaining production gap was that `build_runtime_spec()` did not supply the passive installation inventory to registry discovery, leaving package-backed agents unavailable. The runtime now carries one local npm/Bun/uv/pipx/Cargo metadata inventory into discovery; package matching accepts the installed package despite a registry version update, but still requires exactly one proven launcher. The existing Telegram command menu already enumerates and deduplicates available runtime engine IDs, so no menu production change was required. The focused ACP/runtime/menu gate and preserved session, compact/handoff, and batch gate remained green.

## Launcher inventory matching experience

The direct Cline probe showed the local package version can lag the pinned registry version. Exact version matching therefore rejected a verified installed launcher even though its package and sole executable were unambiguous; matching now keys on ecosystem and normalised package identity, retaining the one-launcher ambiguity guard. Scoped packages are not guessed: all package-derived commands originate from inventory metadata.

## Telegram inline menu experience

The restored quick-action menu replays fixed slash commands from a closed callback mapping, so callback payloads cannot inject arbitrary command text. The updated panel adds Agent, Queue, and Health shortcuts to the original eight rows. Disabled-topic replay, unknown-action handling, and dynamic Engines filtering are covered by focused loop and command tests.

## ACP negative-cache invalidation experience

The rebuilt global package matched source exactly and direct no-cache discovery resolved Cline, but startup reused a fresh negative record keyed before npm metadata supplied a command: `cline:3.0.55:windows-x86_64:`. The registry parser and Windows npm layout were therefore not the remaining defect. Package-backed cache identity now uses the registry package spec, invalidating that legacy empty-command key without deleting user state or disabling caching. A regression recreates the fresh legacy negative record and proves runtime discovery rescans it. Three read-only diagnostic subagents exited without yielding after repeated reminders; inline comparison, a direct discovery probe, and the narrow cache-state reproduction supplied the actionable evidence. Global rebuild initially failed because the user's active `untether.exe` held the uv tool directory open; source-runtime verification confirmed Cline while that poller remained running.


## Resume-directive fix experience

Four scoped subagent spawns (three investigation plus one implementation) exited without a single tool call or yield, so the controller executed the resume fix inline. The edit tool mangled two same-file edits issued close together (duplicated import blocks, a dropped test body, a truncated docstring); every boundary-echo warning required an immediate re-read of the touched region before continuing. The handoff's urgent `settings.py` duplicate-class repair was stale: verifying by importing the module before editing saved a pointless revert. Cross-engine verification was cheap once real runner constructors were enumerated: all eight engines construct with trivial kwargs, so one loop over real runners replaced per-engine mocks. The bridge prepends its Telegram preamble to prompts, so end-to-end assertions must use suffix containment (`endswith`), not equality.

## Telegram fork implementation experience

### Difficulties

The first implementation pass exposed that runner-level native fork support is not present in the current contract, and a command-only wrapper cannot honestly claim provider support. Codex also has two distinct modes, but only app-server exposes the native `thread/fork` operation; exec mode must remain explicitly unsupported. Focused pytest must run through `uv run` with Windows `USERPROFILE` and `HOME` set.

### Solutions

Added a runtime-checkable `ForkableRunner` capability, implemented Codex app-server `thread/fork`, and left every unverified runner unsupported. Reused the established effective-engine resolver rather than selecting the first configured engine. Parser, source precedence, failure-state preservation, provider protocol, and practical loop dispatch are covered; the full suite and repository-wide Ruff gate pass.

### Inconveniences

Pre-existing `.local/` and `.trash/` remain untracked and untouched.

- Reclaim lifecycle pitfall: durable inbound intent blocks ordinary replay; only exact pending fence owner/digest with persisted prior-operation linkage can reconcile.
- In-process receiver tests must isolate remote gate roots from local migration locks.
- File-bundle export_exact acquires runner admission internally; it cannot reload under a held migration lock. A public lock-only adapter read contract is needed (adapter files left untouched).

## Bounded file-bundle reclaim follow-up

Added caller-context/live-descriptor validated export_locked; coordinator reads reuse that seam while executable adapters retain export_exact. File overwrite validates incoming header and metadata before replacement, persists incoming metadata, and reruns scoped restart proof on reconciliation retries. Changed-byte file roundtrip and failed-probe retry pass. Record and metadata replacements remain separate: a crash between them leaves pending ownership blocking admission; matching incoming bytes with prior metadata can retry. Power-loss directory durability and arbitrary external store edits are not proven. Legacy reclaim_contract has two incompatible tests omitting lineage and declaring an unscoped nonexistent restart command; these fail closed, and were not edited outside the allowed scope. Final focused file/coordinator/gate/Pi check: 65 passed, 4 skipped; scoped Ruff passes. No live infrastructure or commits.

## OMP public-SDK cold-load verification

OMP 18.1.6 exports SessionManager through ./session/*; use that narrow public surface rather than importing agent orchestration. Its v3 title slot, sibling artifacts and global blob references are not Pi-compatible. The disposable Bun probe uses inMemory, appendMessage, persistCopy with suppressBreadcrumb, flush/close, then a separate process opens the exact file with breadcrumbs suppressed. An environment allowlist isolates HOME, USERPROFILE, XDG, Windows app/temp directories and PI_CODING_AGENT_DIR; empty OMP_PROFILE/PI_PROFILE prevents inherited named-profile routing. No agent/model/provider calls or installed-library edits.

Verified: focused pytest 1 passed in 2.64s; Ruff check, Ruff format --check and py_compile passed. Assertions cover native UUIDv7/header/file identity, ordered user/toolResult/user payloads and parent links, 256-byte native title slot, unchanged file bytes after cold load, and no terminal breadcrumb files. Each child has a 25-second timeout and 64-KiB cap per output pipe; only the owned child is killed/reaped. Initial format check flagged the new Python test; formatting was applied and rechecked. This proves SDK persistence/cold-load only, not migration adapter integration, roundtrip, asset/blob transfer, fsync durability or provider-signed replay.

## Store-local identity lookup

Extracted the existing bounded header scan for export reuse; lookup_exact returns only the requested native ID after a unique valid header match and declared-store validation. This is store-local identity, not project authorization. Lookup does not read transcript bodies or metadata, acquire gates, run probes, or write fences/cache/configuration. Boundary tests simulate 10,000/10,001 candidates without creating thousands of files. Verification: file pytest --no-cov: 43 passed, 3 skipped; scoped Ruff and ty: all checks passed. Pitfall: Windows symlink privileges were unavailable, so three real-symlink tests skipped; symlink refusal is not runtime-proven on this host.


## In-process session discovery boundary

Added a bounded integration test connecting real remote.inspect_session frame/reply validation through an injected subprocess transport to real handle_receiver and FileBundlePersistenceAdapter.lookup_for_project. Two distinct sessions reuse the same peer; foreign session cwd and mismatched receiver project root refuse. An unrelated registered adapter must never be invoked. Full temporary-tree snapshots prove discovery creates no files or directories (including fences/cache). Preflight, identity paths, and transport are simulated; no SSH, keys, model, or deployment was exercised. The SessionCommand/perform boundary remains untested by this file.

Verification: uv run pytest tests/test_session_migration_discovery_end_to_end.py --no-cov -q: 3 passed (0.48s); Ruff check and format check passed; ty check passed. Pitfalls: transport refusal translation intentionally asserts the receiver redaction code inside except (targeted PT017 suppression); anchored regex uses a raw string (RUF043); initial format check required Ruff formatting before the final successful checks.

## 2026-10-27 OMP restore + runtime probes (parent-implemented)

- `omp_restore.py`: the collector's record names lose the journal stem (sibling
  artifact root), so `journal_name` MUST be passed explicitly — the header does
  not encode it. Guessing `native_id.jsonl` breaks artifact placement.
- OMP SDK `SessionManager.open` pins `getHeader().cwd` and `sessionDir` to the
  probe root; cross-tree verification needs `OMP_SESSION_DIR`/`OMP_EXPECT_CWD`
  env overrides (added to `omp_persistence_probe.ts`).
- `_environment()` in the runtime test mkdirs each env dir: pre-create the
  probe root before calling it, or it raises FileNotFoundError.
- Delegated restore worker burned its whole budget on re-reading context and
  wrote zero files; the five verified findings it did return were still useful.

## OMP restore preflight security boundary

Restore now validates portable artifact names, existing target ancestors via
`omp_bundle._checked`, referenced blob closure in root and child journals, and
all planned file collisions/prefix conflicts before creating the journal.
Exclusive creation alone did not prevent parent-link traversal or partial trees
from predictable collisions. PurePosixPath normalizes `.` to empty parts, so
explicitly reject that spelling as well. Concurrent hostile replacement remains
outside this quiescent disposable-tree contract; interrupted writes may leave a
partial tree that must be discarded, with no production activation.

Verification: focused restore and collector pytest: 30 passed, 5 skipped;
real parent-symlink tests could not run on this Windows host (privileges).
Scoped Ruff and ty passed. No dependencies, live stores, agents, or commits
were used. Historical recall retrieval timed out waiting for its existing index
builder; implementation used freshly read local files.

## Receiver reclaim boundary verification

Added real protocol/NativeReceiver checks for absent configured authorized_peer,
foreign peer/project, divergent lineage, and receipt replay after an injected
activation interruption. Build outbound authority using real export/completion;
restore original destination bytes before submitting the evolved return package.
Refusals preserved exact destination bytes and the completed outbound fence.
Replay preserved the exact receipt file and restored bytes, prohibited both import
methods, and reopened session admission. No production bug reproduced in these
boundaries. The existing test module already exceeded the 1000-line guideline;
kept changes in the sole allowed test file rather than moving unrelated tests.
History retrieval timed out; fresh source inspection supplied the contracts.
Verification: `uv run pytest tests/test_session_migration_transfer_end_to_end.py
--no-cov -q` (32 passed); `uv run ruff check
tests/test_session_migration_transfer_end_to_end.py` and `uv run ruff format
--check tests/test_session_migration_transfer_end_to_end.py` passed. No live
stores, agents, dependency installations, production edits, or commits used.

## 2026-10-27 PR CI stabilization (parent-driven)

- CI's `ty check` covers tests too — local `ty check src` missed 47 test-side
  diagnostics. Always run the full command before pushing.
- GitHub-hosted Windows runners can collide `st_mtime_ns` for rapid writes, but
  `os.replace` always yields a new `st_ino`: the (mtime, ino, size) tuple in
  `state_store.py` is collision-free. A test that copies the REPLACEMENT's
  identity into the cached signature artificially defeats it — simulate only
  the timestamp collision and keep the old inode.
- New Windows/CI-only failures with green local runs: fetch `--log-failed`
  before theorizing; three separate fixture issues were all POSX chmod(0o700).
- OMP collection must carry the real journal filename: record names lose the
  stem, and the filename defines the sibling artifact root.
- Repeated-move receiver collisions need operator-bound `authorized_peer`
  evidence, never a wire-supplied identity; status must stay hidden until
  fence activation completes.
