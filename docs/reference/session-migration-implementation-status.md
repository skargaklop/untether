# Session migration implementation — corrected native fidelity contract

**Status:** draft PR [#1](https://github.com/skargaklop/untether/pull/1), not deployed or approved as all-engine completion. This document supersedes the previous transcript-copy status.

## Current local verification checkpoint (27 October)

Before the final restore-safety additions, the focused migration suite passed **443 tests, 9 skipped** using `PYTHONPATH=src PYTHONUTF8=1 python -m pytest tests/test_session_migration*.py tests/test_acp_migration_gate.py -q --no-cov`. Migration-package Ruff and type checks passed; repository Ruff passed and the final formatting check reports **425 files already formatted**. Final repository lint, migration-package types, `git diff --check` and `uv lock --check` pass.

Inbound discovery is now implemented and integration-verified: an omitted-engine `/session move … --from` resolves through targeted pinned inspection, the configured receiver project root, exact recorded-cwd membership and a real store lookup; foreign working directories refuse, and unrelated local adapters cannot influence resolution. The full command-to-receiver boundary runs through injected transport only — no live SSH has been exercised. Discovery integration is covered by `tests/test_session_migration_discovery_end_to_end.py` (3 tests); the Telegram boundary is covered by separate command tests, not by that integration file.

OMP 18.1.6 has offline persistence evidence (`tests/test_session_migration_omp_runtime.py`) and an exact-bundle collector (`omp_bundle.py` + `tests/test_session_migration_omp_bundle.py`) covering journal bytes, the title slot, recursive artifacts and SHA-256-verified blobs. A disposable-store restore helper now verifies referenced blobs and preflights unsafe paths and collisions; the real SDK write→collect→restore→separate-process cold-load regression passes. There is still **no registered production OMP adapter**; coordinator integration, ownership fencing and follow-up turns for OMP remain unverified. Real symlink tests skip on this Windows host due to missing privileges. Trusted quiescent roots are required; interrupted writes may leave a partial disposable tree.

Reclaim now retains prior-operation fence evidence and publishes explicit verified local ownership. Changed-record round trips exercise runner admission, repeat pulls, interrupted metadata publication and restart-probe failure. Installed-Pi offline cold-load verification passed; it does not prove a model follow-up. Record and metadata publication remain separate atomic replacements; simulated interruption recovery is tested, not filesystem power-loss durability.

The final full-repository regression run passed: **4,260 passed, 47 skipped, 9 warnings** in 389.99 seconds with `PYTHONPATH=src PYTHONUTF8=1 python -m pytest tests -q --no-cov --tb=short`, including the discovery integration and OMP additions. Coverage was not measured; warnings include unawaited AsyncMock coroutines and remain visible in the report.

An earlier full regression run passed: **4,081 passed, 41 skipped, 9 warnings** in 380.35 seconds with `PYTHONPATH=src PYTHONUTF8=1 python -m pytest tests -q --no-cov --tb=short`. Per-test home/config isolation clears inherited global overrides and preserves explicit test path patches; isolated homes live outside directories under test. Earlier shared-override and directory-contamination failures were corrected, not waived. No live configuration was changed to accommodate tests. Coverage was not measured; warnings include unawaited AsyncMock coroutines and remain visible in the regression report.

## Earlier controller verification checkpoint (historical, not final-revision proof)

The controller independently ran the full regression suite: **3,914 passed, 36 skipped** (no coverage measurement in that run). Subsequent endpoint engine-isolation checks passed: **10 protocol tests**. Ruff passed across `src` and `tests`. These observations do not establish the complete product contract below.

Local implementation now includes a native transfer coordinator, CLI transfer/serve routing, a registered Telegram backend, exact archive validation, chat/topic binding after verified restore, receiver engine scoping, and a configurable Pi JSONL v3 file adapter. Two-session receive/export CLI simulations preserve record bytes through fixture adapters. File-adapter tests use a fresh Python storage reader; this is NOT evidence that an actual installed Pi/OMP agent resumed successfully.

Still open: production-grade source admission/snapshot consistency, complete asset/index persistence across supported engines, actual provider restart/resume proof, the documented `/session move` discovery-driven interface rather than explicit engine/project plumbing, bounded export transport review, final documentation, merge and cleanup. The file adapter supports one specific persistence format, not every ACP engine. No automatic transcript fallback is allowed.

DevOps deployment follows reviewed local delivery and is not a prerequisite for local code verification. No live process was restarted or reinstalled.

## Required product experience

After configuring named SSH peers once, CLI and Telegram should expose:
`/session move <session-id> --to <peer>` and
`/session move <session-id> --from <peer>`.
Each command must discover authorized sessions, verify a quiescent generation,
transfer exact records/assets, import via an actual engine persistence adapter,
restart and resume independently, verify all record hashes, publish a durable
destination binding, and reconcile lost acknowledgements.

The command must **refuse** if any engine adapter cannot prove native state
restoration. Reseeding a prompt is not a substitute.

## Upstream evidence

- [acpx export](https://github.com/openclaw/acpx/blob/main/src/session/export.ts)
  exports its local session record and ACP event log.
- [acpx import](https://github.com/openclaw/acpx/blob/main/src/session/import.ts)
  allocates a new *acpx local record ID* and retains the provider session
  handle; it does **not** populate a fresh provider-native store. Import may
  become discoverable even though `session/load` on another host cannot work.
- [hydra-acp bundles](https://github.com/smagnuso/hydra-acp/blob/main/src/core/bundle.ts)
  carry lineage, metadata, raw history, and optional referenced tool blobs.
- [hydra-acp resurrection](https://github.com/smagnuso/hydra-acp/blob/main/src/core/session-manager.ts)
  may rebuild a fresh upstream agent and run `seedFromImport`; this is not
  byte-identical provider-native restoration despite stable Hydra-level lineage.

ACP specifies session management and optional load/resume, not a portable
provider-state import/export API. Support is therefore **per actual engine
persistence adapter capability**, not an allowlist of agent names.

## Current branch changes

Removed incorrect automatic transcript reseeding modules, fixtures and
coordinator. Added `native_archive.py` for a bounded, lossless, hash-checked
collection of records/assets and explicit stable logical ID, native ID,
generation and lineage. Added `native_adapter.py` with an adapter registry
and a strict restart/resume and record hash proof gate. Configured Pi JSONL
profiles and executable-v1 profiles now register persistence adapters. Installed-Pi
cold-load verification passes with retained native identity, historical messages
and unchanged record bytes (`tests/test_session_migration_pi_runtime.py`). This
is not a full round trip, follow-up-turn proof, or all-engine coverage; undeclared
engine persistence still fails closed.

The pre-existing SSH pinned-host identity, fixed verbs, durable receipt,
operation locks, runtime-config schema and explicit destination enrollment
remain reusable. Old context-only `package.py` and receiver scaffolding are
legacy and MUST NOT be activated as the public migration route; their
replacement by the native envelope remains incomplete.

## Exact verified support matrix

| Engine | Lossless codec | Real engine import | Independent restart and follow-up | End-to-end move |
| --- | --- | --- | --- | --- |
| Test fixture | Unit-tested record codec | Fixture subprocess adapter with restart probe | Fixture-only | Fixture level |
| Pi | `pi-jsonl-v3` file adapter with lineage metadata | Installed-Pi cold-load verified on disposable stores | Cold-load verified; live follow-up turn not yet shown | Pull path verified against fixture receiver |
| OMP | Exact bundle collector (`omp_bundle.py`) + restore (`omp_restore.py`): journal incl 256-byte title slot, recursive sibling artifacts, SHA-256-verified blobs; real-SDK write→collect→restore→separate-process read verified (`test_omp_bundle_collect_restore_sdk_coldload`) | Restored into disposable tree verified by separate SDK process; no production adapter registration/coordinator wiring | Cold-load verified on synthetic sessions; no model follow-up | No |
| Codex CLI 0.163 | Not registered | Disposable-store probe: native resume with sessions-only copy, no credentials | Metadata-only identity/settings retained; original bytes exact prefix plus settings event | No |
| OpenCode 1.17.13 | Native JSON export/import tested locally | Synthetic offline admitted input visible in A's durable history is absent from exported JSON and B's history | Native ID retained, but admitted-event fidelity fails; messages/tool history not proven | No |

Codex probe evidence (2026-10-27, `D:/Temp/untether-codex-persistence-probe/probe_followup2.py`): `thread/resume` with the original cwd and declared model/provider returns identical id/model/provider/cwd; the original 22,388-byte journal remains an exact byte prefix and exactly one 791-byte `thread_settings_applied` event is appended (resolved defaults: approval policy, permission profile, workspace roots). Resume is strictly append-only — identical declared settings do not suppress the event. This is metadata-only persistence evidence on a disposable store, not historical-message/tool fidelity or a transfer adapter. A fresh asserted probe (`D:/Temp/untether-codex-local-verification/probe_local.py`) independently passed with 22,388 original bytes, 791 appended bytes, unchanged identity/settings and no copied credentials/configuration. Its source contains only one `session_meta` record.

OpenCode local evidence (`D:/Temp/untether-opencode-probe/native-eb9f8bd799-output.json`, adjacent `-export.json`): offline `/api/session/{id}/prompt` with `resume:false` creates one `session.next.prompt.admitted` event in A history. Native export/import both exit zero and retain the session ID, but export has only `info` and `messages: []`, and B history is empty. This proves loss of that admitted input, not that all completed-message exports are broken. Empty projected message/context equality is not fidelity. No model/tool turn or production adapter is claimed.

Final bounded review reproduced and fixed a repeated-move defect: a second A→B push after A→B→A formerly failed `SESSION_ID_COLLISION`. Receiver-side reclaim now requires operator-bound `authorized_peer` context and durable prior export evidence. Real-receiver tests cover the second push, foreign peer/project/lineage refusal without overwriting prior bytes, and receipt replay after interrupted activation without another import. After the recovery-ordering fix, targeted receiver/transfer checks passed 43 tests; full regression passed 4,260 tests, 47 skipped, 9 warnings. Status withholds receipts while reclaim activation is pending; rejected completion validates both fences before creating export evidence. Repository lint, formatting (425 files), migration types, lockfile and whitespace checks pass. No live VPS transfer or real model follow-up is claimed.

The branch is not ready to merge. No live SSH, deployment, restart, secret
access, or modification of local unpublished work has occurred.

## Reproducible checks

Run from the review checkout with the Untether venv interpreter, `PYTHONPATH=src`, `PYTHONUTF8=1`. Final local checkpoints (not live-deployment or full product approval):

```sh
python -m pytest tests/test_session_migration_*.py tests/test_acp_migration_gate.py -q --no-cov   # 463 passed, 10 skipped
python -m pytest tests -q --no-cov --tb=short                                                    # 4,260 passed, 47 skipped, 9 warnings
python -m ruff check src tests                                                                  # All checks passed!
python -m ruff format --check src tests                                                         # 425 files already formatted
python -m ty check src/untether/session_migration                                                # All checks passed!
```

Skips are environment-bound (Windows symlink privilege, absent optional binaries) and reported with reasons via `-rs`.
