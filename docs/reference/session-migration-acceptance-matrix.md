# Session transfer acceptance matrix

Authority: [[../plans/session-migration-product-correction]] and [[../plans/session-migration-acp-wide-goals]]. This is a work tracker, not evidence of delivery.

## ACP-wide delivery gate

The operator clarified on 2026-10-29 that a fixed Pi/OMP/Codex/OpenCode roster is not the product scope. Engine-specific rows below are subordinate evidence, not a substitute for a general ACP-facing system. Current adapter and installed-engine results do not satisfy this gate.

| Goal | Required evidence | Current status |
|---|---|---|
| G1 | Pinned ACP/acpx/hydra-acp/alternative capability comparison and empty-store probes | Pinned source findings plus one EXECUTED empty-destination probe: acpx 0.21.0 `sessions export`→`import` against a disposable ACP agent leaves the destination native store empty and the follow-up turn fails (`native restore did not happen`) — client-record transfer, not native restoration ([[session-migration-acp-foundation-research]] §Executed probe). Hydra takeover semantics and acpx client-store interface confirmed from pinned sources. Remaining: hydra-acp runtime probe and additional alternative review |
| G2 | Evidence-backed reuse/wrapper/extension foundation decision | Decision recorded: extend Untether's persistence seam ([[session-migration-foundation-decision]]), three-approach comparison against executed evidence — acpx fails native restoration by probe, hydra admits text-level handover, Untether seam already proves byte-exact native transfer for four engines. Remaining protocol gaps listed (model follow-up, real-SSH transport) |
| G3 | Negotiated provider-neutral path plus independent additional extension without central engine dispatch edits | Closed: declared `executable-v1` profiles register generic adapters through `register_from_settings` with zero core edits; `tests/test_session_migration_independent_extension.py` proves a novel engine (`acme-agent-v1`, named nowhere in the codebase) joins through a declared profile + external adapter binary alone, and the coordinator moves two distinct sessions push A→B and reclaim round trip B→A with byte-exact records and generation evolution, running the real receiver protocol handler with unmodified `protocol: 1` replies. Root-cause fix `e502c4c` aligns `_complete_remote_export` with the real wire shape (the previous exact-equality-without-`protocol` check made every real-SSH completion fail after successful import) | 
| G4 | Complete records/tools/assets and identity fidelity | Partial adapter evidence only; omissions remain open |
| G5 | Both directions, two sessions, round trip, restart/follow-up, binding and recovery | Existing bounded tests retained; final architecture verification open |
| G6 | Reproducible discovery-derived breadth matrix, not a fixed installed roster | Open; installed-engine command is supplemental evidence |
| G7 | Reconciled documentation, scoped reviewed PR and final quality gates | Open; no merge/deployment/live restart authorized |

## Existing implementation evidence

| Required behaviour | Current implementation/evidence | Outstanding verification |
|---|---|---|
| One-action discovery-driven move in both directions | Telegram outbound catalog resolution; omitted-engine inbound uses targeted pinned inspection, configured receiver project root and exact header cwd membership; strict bounded replies | Cross-boundary command/receiver integration, CLI public syntax and discovery for non-file adapters |
| All same-engine ACP pairings | ACP registry is open-ended; executable-v1 profiles register generic adapters; dedicated production adapters registered via profile routing for `omp-v1`, `codex-rollout-v1`, `opencode-sync-v1`, and `pi-jsonl-v3` file bundles (`profile_registration.py`; verified `omp`/`codex`/`opencode` all register through `register_from_settings` with a gate root configured). Installed-profile matrix: `scripts/check-engine-matrix.sh` 15 passed (Pi/OMP/Codex/OpenCode runtime coldloads + coordinator roundtrips incl. `test_codex_coordinator_push_and_native_followup_roundtrip`: two-session push, real app-server resume, native shell follow-up journalled, independent cold restart, evolved re-push) | ACP-wide breadth beyond installed profiles still unproven (G6); matrix is supplemental installed-roster evidence, not discovery-derived coverage |
| Original records, tools, metadata and assets | Native archive hashes; Pi JSONL v3 file adapter | Complete referenced asset/index persistence and remaining real-engine round trips. Pi parent-reference chains are now exported/imported byte-exact, with import preflight refusing foreign parent bytes, destination directories, symlinks and late/root collisions with zero writes (write recorder asserts no calls, `test_session_migration_pi_parent_import.py`); reclaim re-reads existing shared parents before and after the probe, accepts unchanged parents without rewriting them (`st_mtime_ns` unchanged) and refuses mutated/added/missing/non-regular parents before mutation (`test_session_migration_pi_parent_reclaim.py`, `test_session_migration_pi_parent_reclaim_integration.py`, commit `23975e7`). Referenced Pi assets and all other engines' assets stay open. |
| Native identity or explicit stable binding | RestoreProof logical/native IDs; receiver receipts. Codex coordinator follow-up keeps native id, cwd and literal history across independent app-server restart (`test_codex_coordinator_push_and_native_followup_roundtrip`); Pi identity mapping retained through coordinator roundtrip incl. destination chat/topic binding | OMP/OpenCode coordinator-integrated restart mapping retained via roundtrip tests; LLM/model follow-up turns remain unclaimed (shell/SDK restart probes only) |
| Quiescent source snapshot | Shared runner gates; mandatory native snapshot admission; cached operation-bound exports; explicit active ownership after verified return with prior fence linkage | External-writer boundary and real-engine quiescence evidence; power-loss durability |
| Two distinct sessions per endpoint and round trip | CLI fixture simulations; changed-record executable/file-bundle return tests check metadata, repeat pulls, runnable ownership, interrupted metadata publication and restart-probe retry | Installed Pi now cold-loads two sessions through A→B→fresh return-store with exact archives/history/bytes (`test_session_migration_pi_runtime.py`); the real Pi adapter now runs inside `transfer.perform` for two sessions — push A→B, pull B→A-return into a third store declaring real destination chat/topic bindings verified in the durable `TopicStateStore`/`ChatSessionStore`, a genuine installed-Pi RPC shell follow-up whose `bashExecution` survives an independent cold-load restart, and an evolved push into a fourth store (`test_session_migration_pi_coordinator.py`). Coordinator roundtrips now exist for all four engines (`scripts/check-engine-matrix.sh` runs `test_omp_coordinator_push_pull_roundtrip`, `test_opencode_coordinator_push_pull_roundtrip`, `test_codex_coordinator_push_and_native_followup_roundtrip` incl. two-session push, real app-server resume, native shell follow-up journalled, independent cold restart, evolved re-push). The receiver boundary in coordinator tests is an injected in-process call (real protocol codec + `NativeReceiver` + `ReceiverEndpoint`), not a real SSH byte transport. LLM/model-generated follow-up turns remain unclaimed (native shell/SDK follow-ups only) |
| Destination chat/topic registration | `transfer.perform` binds a destination chat/topic only after import verification and durable receipt; public `TopicStateStore`/`ChatSessionStore` seams and enrollment records are exercised end-to-end by `test_pull_binds_destination_chat_only_after_verified_restore` and retry/multi-session variants in `tests/test_session_migration_transfer_end_to_end.py` (state `bound`, persisted resume token, confirmed enrollment, fresh-process export equality) | Final end-to-end verification |
| Lost acknowledgement recovery and duplicate prevention | Operation/intents/receipt records; native receiver reserves intent before import and refuses uncertain replay without reimport; `test_pull_completion_retry_without_duplicate_import` (13 parameterisations) replays mutated/absent status replies and receipt loss without reimport; `test_receiver_reclaim_authority_and_receipt_replay` covers interrupted-activation receipt replay; `test_file_bundle_changed_roundtrip_metadata_and_retry` covers interrupted metadata publication and restart-probe retry | Automatic interrupted-import reconciliation; full failure-injection verification |
| Malformed/foreign-owner/foreign-engine rejection | Protocol and native receiver tests | Re-run final safety suite |
| Bounded output, archive sizes and deadlines | Receiver and executable adapters reuse bounded process runner; configured timeout no longer gains five seconds; 20 focused transport/adapter tests pass | Final-revision malicious-process checks |
| Optional Coolify and outbound pull | SSH peer configuration and pull path | Final portable setup documentation |
| Reviewed local PR, no live changes | Existing working branch preserved; live process untouched; focused migration suite 464 passed/10 skipped; final full suite 4,260 passed/47 skipped/9 warnings; lint/format/migration types, whitespace and lockfile checks pass | Draft PR #1 updated; bounded review findings fixed and regression-verified; remote CI pending |

## Asset-fidelity evidence boundary

[[session-migration-asset-evidence]] records pinned OpenCode spill-file references,
installed Pi truncated-shell spill references, and the observed Codex attachment
add/list/restart probe. Original event/journal equality alone does not cover these
external files or attachment membership rows. Current implementation state:

- **OpenCode**: export-side byte collection of referenced tool-output spill files
  is implemented (`opencode_assets.py`): every reference is prevalidated
  fail-closed before any read (pinned store path, regular non-symlink, hash/size
  manifest, bounded total) and carried as archive records; asset-carrying import
  refuses before any write (`TOOL_OUTPUT_IMPORT_NOT_IMPLEMENTED`) rather than
  falsely certifying a silent drop (`test_session_migration_opencode_sync_adapter.py`).
- **Pi**: export-side collection of `bashExecution`/`fullOutputPath` OS-tmp spill
  logs is implemented (`pi_output_assets.py`, commit `c6aaf33`): direct-child-of-tmpdir
  + `pi-bash-<16 hex>.log` name validation, fstat/lstat replacement guard, 64 MiB
  total bound, canonical manifest; journals are never modified. Import of an
  asset-carrying archive is implemented in `import_exact`: validated manifest,
  strict record set, tmpdir preflight, O_EXCL restore with rollback, post-probe
  asset reread, and materialization of absent spill files on a disjoint
  destination verified by `test_import_materializes_absent_spill_before_probe`
  (`test_session_migration_pi_asset_import.py`). Asset-bearing reclaim
  (`import_overwrite`) is implemented via `reclaim_asset_plan`
  (`pi_output_assets.py`): per-side strict record validation with orphan
  refusal, preflight before any write, byte-reuse of existing spill files,
  atomic journal replace, restart probe and post-probe asset re-preflight;
  A→B evolved reclaim, back-reclaim leaving foreign prior-only assets
  untouched, collision refusal with zero store writes, retry after interrupted
  metadata publication and orphan/legacy refusal are covered by
  `test_session_migration_pi_asset_reclaim.py` (6 tests). Only the legacy
  manifest-less-prior combination still refuses (`PI_OUTPUT_IMPORT_NOT_IMPLEMENTED`)
  by contract.
- **Codex attachments**: the public attachment add API preserves payload and logical
  identity keys but mints new row IDs and timestamps across stores; exact-preservation
  integration remains unresolved open work.

None of these gaps is a scope amendment or successful unsupported transfer.

## Current resynchronization check (2026-10-29)

The prior full-matrix result below is historical, not a fresh passing gate.
Current reruns exceeded outer bounds of 180 seconds and 100 seconds; the verbose
second run reached the installed OpenCode runtime after six native-runtime
checks passed. A separate Pi/OMP/Codex coordinator subset passed seven checks.
The transfer/reclaim suite passed 36 checks, and file-bundle/registration/profile
regression passed 143 with four skips after the bounded responsibility extraction.
Follow-up diagnosis found cumulative lifecycle cost exceeded those outer bounds;
no unbounded wait was established. The isolated OpenCode runtime passed in 59.46
seconds. A subsequent full supplemental installed-engine run, bounded at 420
seconds, passed all 15 checks without skips in 256.62 seconds
(`bash scripts/check-engine-matrix.sh -vv --tb=short`). This verifies the named
installed profiles only, not ACP ecosystem breadth, model continuation or complete
asset portability. The earlier timeout is not by itself proof of a runtime bug.

## Earlier bounded controller evidence

Controller rerun on the current working revision: `bash scripts/check-engine-matrix.sh`
passed all 15 runtime/coordinator checks with no skips (248.27 seconds). A separate
`uv run --no-sync pytest -q -o addopts='' --tb=short` passed 4,599 tests with
54 skips and nine warnings; this disables the default coverage gate and is a
regression execution result, not a coverage claim. Ruff check, formatting,
source-and-test Ty, configured Bandit (`-c pyproject.toml`) and lockfile checks
passed. Subsequent manifest-boundary tests also accept Windows and POSIX source
path metadata without destination filesystem access and reject traversal and
basename/record mismatches (43 passed). These results do not close asset-import,
model-continuation or real-SSH acceptance gaps. Older measurements below are
historical, not the current verification baseline.

The expanded `bash scripts/check-engine-matrix.sh` gate includes runtime and coordinator suites for Pi, OMP, Codex and OpenCode; its latest full run passed 15 checks in 266.05 seconds (all four installed engines, OpenCode enabled via `UNTETHER_TEST_OPENCODE_EXE`). Codex inbound binding is required unconditionally, not counted as success on refusal. The gate exercises native shell follow-ups and independent restart, not provider-generated model turns; coordinator tests use injected receiver transport and do not establish real SSH coverage. After the Codex interrupted-metadata recovery fix (`test_codex_coordinator_reclaim_crash_after_journal_before_metadata`, commit `c75e856`) and the Pi spill-carry work, the full regression passed 4,553 tests, 53 skips, nine warnings (runtime suites verified separately: 6 passed plus installed-OpenCode runtime 1 passed). Source/test Ruff, format and migration Ty passed.

## Engine coverage truth boundary

Deployment reality: all engines are installed locally; the remote VPS has only Pi installed. Pi is therefore the only currently installed same-engine local↔VPS pairing; no live VPS transfer has been verified by this work. Codex/OpenCode/OMP native transfers require both a supported registered adapter and the engine on both endpoints; local engine installation alone does not establish transfer support. Unsupported refusal is a limitation, not success. The draft PR contains Pi + OMP persistence evidence, not completed all-engine migration. Production profile routing now selects the OMP (`omp-v1`), Codex (`codex-rollout-v1`) and OpenCode (`opencode-sync-v1`) adapters. `tests/test_session_migration_executable_registration.py` exercises construction through the shared registration path for Codex/OpenCode. This registration evidence does not satisfy native restoration, project authorization, asset fidelity or follow-up acceptance; those mandatory checks remain open. The operator explicitly permitted local testing, not a scope deferral. Codex's earlier append-only resume probe is not evidence that the production adapter's real-engine verification passes.

Neither the plugin interface nor the Pi-format codec demonstrates all-engine restoration. The ACP registry is extensible, and every engine needs a supported persistence implementation and independent behavioural evidence. Unsupported capability refusal protects data but is not successful transfer and does not satisfy an engine coverage row. Installed-Pi cold-load evidence checks native ID, historical messages and unchanged bytes in disposable stores. OMP has bundle collect→restore→separate-process SDK cold-load evidence including the title slot and ordered messages and a registered production adapter; coordinator-integrated real-engine follow-up proof remains required. OpenCode's registered adapter uses documented sync/history and sync/replay rather than the lossy CLI export/import; full canonical event verification now rejects altered payloads even when event IDs and ordering remain unchanged (`test_import_refuses_divergent_replay`). Installed OpenCode production-adapter verification now passes independent writer shutdown/restart, complete event and projected-message equality, unrelated destination-session preservation, and a native follow-up shell command persisting across another restart (`test_session_migration_opencode_runtime.py`). Exact native `Session.directory` authorization accepts the configured source project and refuses foreign/missing/malformed bindings (`lookup_for_project`); destination storage roots do not grant source authorization. Controller rerun of runtime plus adapter checks passed 27 tests with one Windows symlink privilege skip. This is not provider/model continuation or coordinator-integrated round-trip evidence.

## Current bounded verification additions

OMP's SDK-created journal now passes production `OmpPersistenceAdapter.export_exact` and `import_exact`, including independent SDK cold loading. Native lookup validates the fixed title slot and immediately following bounded header; foreign cwd, duplicate identities and malformed headers refuse. Controller adapter/runtime/collector checks passed 45 tests with five Windows symlink privilege skips. Full assets and meaningful continuation remain mandatory work.

Codex production restore and a second independent app-server resume pass on canonical timestamped rollout filenames with exact original-byte prefixes and identity/settings checks. Unsupported extra archive records now refuse before writes rather than receiving misleading restored hashes. `test_session_migration_codex_runtime.py` now also restores synthetic native user, assistant and dynamic-tool history and checks exact item identities, text, arguments, output and completion after independent app-server restart. Production verification requests turns and refuses missing recognised history/native completed-item IDs. Actual follow-up execution remains required; historical fixture replay is not tool execution.

Pi's installed RPC runtime now executes a direct shell follow-up for each of two restored sessions, verifies native `bashExecution` output after an independent restart, and carries the evolved bytes/messages through a fresh return-store restore (`test_session_migration_pi_runtime.py`). The same adapter now also runs through the production coordinator: `test_session_migration_pi_coordinator.py` pushes two sessions via `transfer.perform`, pulls both into a third store with durable chat/topic bindings, executes a genuine installed-Pi RPC shell follow-up whose `bashExecution` survives an independent restart, and pushes the evolved bytes onward. The SSH boundary is replaced by an injected in-process receiver call with a swapped adapter registry, so this is coordinator-integrated native shell persistence, not an LLM-generated follow-up and not evidence over the real remote transport.

Codex production restore now also executes a native `thread/shellCommand` echo on the restored thread, verifies completion notifications, persisted user-shell result with exit 0 and exact marker output, journal prefix retention, independent app-server restart reconstruction, and evolved archive return to a third store (`test_session_migration_codex_runtime.py`). OMP likewise executes native RPC `bash` on the exact restored path, verifies persisted `bashExecution`, independent recovery and evolved archive equality (`test_session_migration_omp_runtime.py`). These are native shell persistence proofs, not LLM/model continuation or coordinator round trips.

OMP restore proof now independently rereads and certifies every restored record from the destination after the restart probe—journal, artifacts, referenced and imported unreferenced blobs, and exact metadata bytes—instead of hashing the input archive; overwrite path artifact mapping was corrected and mutate/delete regressions added (`src/untether/session_migration/omp_adapter.py`).

Current controller baseline: full suite 4,379 passed, 51 skipped, 9 warnings; migration-focused subset 580 passed, 14 skips; separately enabling `UNTETHER_TEST_OPENCODE_EXE` passed its installed-runtime test. Migration Ruff/format and Ty passed. The reproducible all-engine compatibility command is documented in [[session-migration-verification]] and its last full controller run passed all four installed engines. Remaining mandatory rows stay open and are not satisfied by these bounded results: Pi referenced assets/index persistence, LLM-generated model follow-up, coordinator round trips for the non-Pi engines (Pi's coordinator round trip is native-shell only and uses an injected in-process transport), and full failure-injection.

## Executed verification refresh (2026-11-02)

- Production adapter registration for all three dedicated profiles re-verified end-to-end on this host: minimal valid settings with an `omp-v1`/`codex-rollout-v1`/`opencode-sync-v1` engine map and a configured session-gate root register exactly `['codex', 'omp', 'opencode']` through `register_from_settings` (the CLI receiver path).
- `scripts/check-engine-matrix.sh` rerun on the committed revision: 15 passed, 0 failed, 0 skipped (Pi/OMP/Codex/OpenCode runtime coldloads and coordinator roundtrips, including `test_codex_coordinator_push_and_native_followup_roundtrip` — two-session push, real app-server resume, native shell follow-up journalled, independent cold restart, evolved re-push).
- Full migration suite `tests/test_session_migration_*.py + test_acp_migration_gate.py`: 828 passed, 19 skipped (Windows symlink-privilege and absent-optional-binary skips, reported with reasons).
- Executed acpx 0.21.0 empty-destination probe (see [[session-migration-acp-foundation-research]]): acpx `sessions export`→`import` transfers client records only; the destination native store stays empty and the follow-up turn fails. Client-record transfer is not native restoration.
