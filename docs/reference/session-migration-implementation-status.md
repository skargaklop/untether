# Session migration implementation — corrected native fidelity contract

**Status:** draft, not delivered or deployed. This document supersedes the previous transcript-copy status.

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
and a strict restart/resume and record hash proof gate. No actual engine
adapter has yet been registered, so native migration fails closed.

The pre-existing SSH pinned-host identity, fixed verbs, durable receipt,
operation locks, runtime-config schema and explicit destination enrollment
remain reusable. Old context-only `package.py` and receiver scaffolding are
legacy and MUST NOT be activated as the public migration route; their
replacement by the native envelope remains incomplete.

## Exact verified support matrix

| Engine | Lossless codec | Real engine import | Independent restart and follow-up | End-to-end move |
| --- | --- | --- | --- | --- |
| Test fixture | Unit-tested record codec | Not implemented | Not implemented | No |
| Pi | Available generic format | No proven persistence adapter | No | No |
| OMP | Available generic format | No proven persistence adapter | No | No |
| Any other ACP agent | Available generic format | Capability-dependent, not registered | No | No |

The branch is not ready to merge. No live SSH, deployment, restart, secret
access, or modification of local unpublished work has occurred.

## Reproducible checks

```sh
uv run pytest tests/test_session_migration_native_archive.py -q --no-cov
uv run pytest tests/test_session_migration_*.py -q --no-cov
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run ty check src tests
```
