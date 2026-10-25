# SSH session migration implementation status (draft PR)

## What is committed

- Typed disabled-by-default migration configuration with portable `native` (default) and `container-wrapper` profiles (Docker/Podman with or without Coolify).
- Pinned known-host SSH identity and fixed `inspect` receiver command with redacted refusal codes and strict health attestations.
- Bounded opaque ACP context archive (not a native agent archive), exact allowlist and hash validation.
- Private on-disk staging, write-once verified receipts, idempotent operation lookup, and a durable ambiguous-import intent that refuses retries until reconciliation.
- An ACP **context-copy** importer capable of seeding a fresh native ID and independently loading it through an ACP protocol subprocess; exercised only with a disposable persistent fake agent. Tool-isolation attestation is mandatory.
- A guarded operator CLI command `untether migrate inspect <peer> --project <alias>`, which is read-only and refuses disabled/unconfigured peers.
- Isolated subprocess, archive and Windows-safe fixture tests. No SSH host was contacted and no service or provider was restarted.

## Exact support matrix

| Engine or mode | Profile/preflight | Real context copy (both hosts) | Native migration |
| --- | --- | --- | --- |
| Generic ACP fixture | Yes, simulated endpoints | Local separate-process importer tested, remote integration missing | No |
| Pi | Generic profile only | Not supported end-to-end | Not supported |
| OMP | Generic profile only | Not supported end-to-end | Not supported |
| Claude/Codex/other ACP | Generic profile only | Not supported end-to-end | Not supported |

**NO engine/mode pair is ready for a live cross-host transfer.** In particular the ACP context importer is not proof of native migration or provider-specific persistence.

## Critical blockers to full completion

1. Published baseline lacks the planned durable ACP source journal, archive/transfer path and session catalog. Its Telegram `/export` backend stores only a bounded in-memory event history, not a complete conversation. Treating that as a lossless snapshot would drop prompts/tool state.
2. No installed, fixed least-privilege receiver endpoint or complete RPC/wrapper orchestration has been implemented for SSH. Current code only probes `inspect`; it does not upload/pull a package. Native and container modes must share that future RPC without requiring Coolify.
3. There is no scheduler-integrated quiescence/generation gate or atomic Telegram binding/ownership fence. A remote success cannot safely switch a private chat or source owner without these.
4. The tracked Pi/OMP runner resume paths are provider-specific, but their persisted state, collision semantics, restart recovery, permission-grant cleaning and cross-host proof have not been validated against real agent versions. Native support must remain disabled.
5. Actual provider authentication, connectivity, destination Telegram enrollment, distinct bot IDs, UID/mount integrity, and project parity cannot be inferred from static config or test mocks. Fresh live validation must occur only after separate SSH/deployment approval.

## Safe operating rule

Do not deploy or merge as functional migration. No user session must be copied, moved, fenced or deleted through these modules until a trusted receiver, complete adapter and ownership protocol are available. Historical transcripts must not be globally rewritten, and source remains intact on all errors.

## Local test commands

```sh
uv run pytest tests/test_session_migration_*.py -q --no-cov
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run ty check src tests
```

Refer to GitHub Actions for observed outcomes, rather than treating listed commands as passed.
