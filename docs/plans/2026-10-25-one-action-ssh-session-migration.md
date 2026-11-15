---
title: One-action SSH session migration implementation plan
status: proposed; no runtime or VPS changes made
date: 2026-10-25
related:
  - coddy-agent-migration-research.md
  - 2026-09-29-acp-session-migration-revised.md
  - ../reference/acp-session-archive.md
---
# One-action SSH session migration — implementation plan/spec

> **Superseded for fidelity and product acceptance:** The transcript-seeding recommendation below is rejected. The binding contract is full native session restoration only through a verified engine persistence adapter, with the public `/session move <id> --to|--from <peer>` single-action interface. ACP does not define portable native storage; missing adapter capabilities must refuse without reseeding. See [implementation status](../reference/session-migration-implementation-status.md).


> **Decision:** Do not copy Coddy's remote-control mode as migration. Implement an explicit, resumable SSH transfer in Untether, with a single command or Telegram action after one-time configuration. This is a plan, **not a deployed feature**. No live service or VPS changes are authorised by this document.

**Goal:** Configure a named VPS, its SSH identity and destination projects once; move one selected session from local → VPS or VPS → local securely by one action, with a verified usable destination and an unambiguous source/destination ownership result.

**Architecture:** One migration coordinator invokes the same transfer operation from either machine. An engine-specific adapter exports/imports only state it can honestly restore. SSH carries a versioned, bounded package to a private staging directory; a compatible Untether CLI on the receiving side validates and publishes it. After verification, switch only that chat/topic's selected session to the destination bot; do not automatically switch engines in an unrelated topic. No public HTTP endpoint or generic shell interpreter is required.

**Stack:** Python 3.12+, current Untether session catalog/state stores, stdlib `hashlib`, `json`, `tarfile`/`zipfile` with strict validation, `subprocess`/AnyIO; installed OpenSSH (`ssh` and `sftp`/`scp`) selected by configuration and checked in preflight. No new Python SSH dependency. Never run `uv sync`, restart, or contact VPS during plan authoring.

**ASCII implementation sketch:**

```text
Telegram: /sessions → session → Move to VPS   OR   /session move <id> --to vps
                                     |
                            validate owner + profile
                                     |
       source session adapter: quiesce → snapshot + hash manifest
                                     |
                          pinned-host SSH / staged upload
                                     |
       VPS Untether CLI: validate → map project → import → probe
                                     |
             receipt + ownership fence → destination selected
             failure: source stays usable, stage is retriable

Reverse: VPS bot /session move <id> --to local → same protocol, reversed roles.
```

## Evidence and decision

The *correct* upstream repository is [`coddy-project/coddy-agent` at da8fdeb](https://github.com/coddy-project/coddy-agent/tree/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d). Its [remote guide](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/operate/remote.md#L3-L13) says the client has no local session store and proxies to a remote server: selection is not migration. Its [session layout](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/features/sessions.md#L5-L31) contains assets, tasks, tool records and children beyond text; its [export docs](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/features/session-export.md#L42-L59) omit attachment bodies. Do not borrow its [changed-host-key auto-replacement](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/tools/ssh/known_hosts.go#L16-L61). Local investigation and links: [[coddy-agent-migration-research]].

Untether already has an ACP journal/validated `.utsession.zip` and a **local-path** `/session export|import` draft: [`session_move.py`](../../src/untether/telegram/commands/session_move.py), [`acp_session_transfer.py`](../../src/untether/acp_session_transfer.py), [[../reference/acp-session-archive]]. Reuse those contracts; do not reimplement a second archive codec. ACP `session/list` discovers sessions, not native-state transfer. The existing entry point in [`pyproject.toml`](../../pyproject.toml) may be absent from installed metadata: preflight must check *installed* command discovery on both machines, not just the source tree. Pi/OMP native sessions and Claude/Codex native sessions require different adapters; never label a transcript handoff a native resume.

### Options considered

1. **Coddy-style remote proxy:** least code for *new* remote sessions, but does not move an existing local session; reject for this request.
2. **ACP transcript seed plus SSH:** reuse existing codec, permits cross-machine conversation continuity; new native ID, no hidden state or project files. Ship as the first supported adapter with honest UI wording.
3. **Raw provider store copy:** can preserve more native state for the *same provider/version* but paths, SQLite indexes, grants and partial writes vary; admit only after provider-specific live-restart proof. No blanket recursive copy of `~/.pi`, `~/.claude`, or a whole home tree.
4. **Git-only handoff:** moves project files, not conversation state; useful as separate preflight, not the session transfer.
5. **Encrypted public API/cloud relay:** introduces tokens, hosting, and another exposure surface; unnecessary when SSH already exists.

## Operator experience and configuration contract

One-time configuration on **each** Untether installation (show examples with dummy values; never save a private key or passphrase in TOML):

```toml
[session_migration]
enabled = true

[session_migration.peers.vps]
host = "vps.example.org"
port = 22
user = "untether"
identity_dir = "~/.ssh"
identity_file = "id_ed25519" # explicitly selected file inside identity_dir
known_hosts_file = "~/.ssh/known_hosts"
host_key_fingerprint = "SHA256:REPLACE_WITH_VERIFIED_FINGERPRINT"
remote_untether = "/opt/untether/venv/bin/untether"
state_dir = "/home/untether/.untether"
# project maps are explicit, not blind Windows/Linux path substitution:
projects = { projects = "/srv/projects" }
```

Use an equivalent `local` peer profile on the VPS for reverse initiation, with SSH access to the Windows local machine (OpenSSH server or an explicitly configured reachable tunnel). If the local machine is not SSH-reachable from the VPS, **the local bot can initiate a pull** of a VPS session through its outbound SSH connection (`/session move <id> --from vps`); the VPS bot must not promise a one-click reverse push in that topology. Telegram user ID grants access separately to each bot; use distinct bot tokens and never let two active pollers share one token. Profile setup must require the operator to verify host fingerprint out of band, test authentication interactively once, select a key (do not blindly choose first in directory), and configure project mapping. Allow SSH agent for passphrase-protected keys; disable password fallback, agent forwarding, changed-key acceptance and arbitrary SSH options supplied by chat text. Configuration hot reload only where the existing loader proves it; do not claim TOML changes are instantly active without a test.

User-facing actions (names proposed; align with `/sessions` and `/session` rather than add another menu):

```text
/sessions → select → Move → VPS → Confirm
/session move <session-id> --to vps --project projects
/session move <session-id> --from vps --project projects
/session move status <operation-id>
```

Only the second confirmation is needed for a destructive source ownership switch; never expose private file paths or archive transcript in Telegram cards. Default remains non-destructive **copy/context handoff** if the adapter cannot enforce source fencing; label it `Copy context`, not `Move`.

## Transfer contract

- `operation_id`: unpredictable UUID; source engine/native id and chat/topic/owner; peer name; direction; destination engine/project; requested mode (`context` or verified `native`); archive schema; byte counts and SHA-256 per member; source generation. Never include identity key contents, credentials, full transcript, or arbitrary path arguments in a shell string.
- Package immutable session state only; project files are **separate**. Preflight compares project Git remote/commit/dirty status or explicitly selected file bundle; require the operator to sync dirty/untracked work separately or confirm a bounded allowlisted workspace bundle. Never silently claim source filesystem is available on VPS.
- Receiver writes to mode-0700 staging under its configured state directory, validates exact member allowlist/schema, size/decompression bounds/hash, safe paths and no symlink traversal before publication; receipt is bound to operation id, archive hash, importer version and newly allocated native ID. Private keys remain local and are never packaged.
- Import never restores source permissions, pending approvals, running jobs, environment variables or tokens. Never execute transcript/tool output as instructions during validation; ACP seed marks historical text as untrusted data. Source is not deleted after transfer.
- Idempotence: the same operation/hash returns the same receipt, changed hash with same id fails. A destination with a conflicting native ID must refuse or allocate a new ID, not overwrite. Writes to shared state use existing atomic/serialised store seams; use an operation lock on both endpoints.
- Failure semantics: do not flip the Telegram session pointer before successful receiver verification; preserve source session on SSH failure, destination import failure, or disconnect. A cross-host ownership switch cannot be atomic by rename: record a durable fencing epoch/receipt; if acknowledgment is lost, reconcile by operation id **before** allowing a second active owner. Do not delete staging/source during ambiguous outcomes. Timeouts, archive limits and retries belong in settings (bounded defaults and visible status).

## Delivery tasks (TDD: each red test → minimal fix → green check)

### 0. Inventory and acceptance baseline (read-only)

Inspect installed vs source entry points on both endpoints; session store shapes, native Pi/OMP storage, ACP capability flags, actual bot/topic routing, and configured SSH topology. Files: [`pyproject.toml`](../../pyproject.toml), [`session_move.py`](../../src/untether/telegram/commands/session_move.py), [`session_catalog.py`](../../src/untether/session_catalog.py), [`telegram/commands/sessions.py`](../../src/untether/telegram/commands/sessions.py), [`telegram/chat_sessions.py`](../../src/untether/telegram/chat_sessions.py), [`telegram/topic_state.py`](../../src/untether/telegram/topic_state.py). Do not inspect private key material. Record baseline tests and whether runner-backed session snapshots are consistent during an active turn. **Gate:** choose which engine and mode can honestly be migrated; do not silently expand scope.

### 1. SSH profile validation and preflight

Files: extend [`settings.py`](../../src/untether/settings.py) with one typed migration profile section, create `src/untether/session_migration/ssh_transport.py`, tests `tests/test_session_migration_ssh.py`, update [[../reference/config]]. Validate selected identity is a regular file inside `identity_dir`, host/port/user and mapped paths are strictly typed, known_hosts exists and the pinned fingerprint matches; call platform OpenSSH using argv and `BatchMode=yes`, strict host key checking and disabled forwarding. No `shell=True` or untrusted command concatenation. A remote endpoint must expose a **fixed allowlisted command** `untether migrate receive|inspect|status` with structured stdin/stdout; prohibit user-supplied remote shell commands. Fail fast on incompatible version/installed entry points/disk/project mismatch. Test changed key, missing/encrypted key, rejected mapping, remote protocol mismatch, no secret leak in error logs. Key unlock belongs to local SSH agent or explicit secure setup; no unattended passphrase prompt in Telegram.

### 2. Bounded source snapshot and receiver

Files: `src/untether/session_migration/package.py`, `src/untether/session_migration/receiver.py`, `tests/test_session_migration_package.py`; reuse [`acp_session_archive.py`](../../src/untether/acp_session_archive.py), [`acp_session_transfer.py`](../../src/untether/acp_session_transfer.py). First supported path: a **complete observed ACP journal** becomes validated context archive; do not claim migration of arbitrary native Pi/OMP sessions. Quiesce at the scheduler/session boundary; refuse active tools/background tasks and test no new turn can race snapshot. Receiver stages and validates before import; only after seed and separate load/resume probe publish receipt. Characterise duplicate, interrupted upload, stale generation, malformed path/ZIP, and destination crash before/after publication. No raw source session ID as destination resume token.

### 3. Source/receiver state machine and rollback

Files: `src/untether/session_migration/operation.py`, `tests/test_session_migration_operation.py`; integrate existing journal, chat/topic stores at a narrow edge. States: `prepared → uploaded → imported → verified → activated`; failures `refused`/`needs_reconcile`. Implement idempotent receipt lookup and fencing, source admission block during transfer, explicit ownership result. Only update the relevant chat/topic after receiver has proved resumability. Re-running after lost SSH response must not reseed twice. Verify no old/new concurrent turns while ownership is ambiguous. A context *copy* does not fence the old source; display both independently and never call it a move.

### 4. Same command for CLI, Telegram and both directions

Files: extend [`telegram/commands/session_move.py`](../../src/untether/telegram/commands/session_move.py) and [`telegram/commands/sessions.py`](../../src/untether/telegram/commands/sessions.py), add narrowly scoped CLI subcommand under `src/untether/cli/` (inspect existing CLI wiring first), tests `tests/test_telegram_session_migration.py` and `tests/test_cli_session_migration.py`. CLI and bot call the same coordinator, not duplicated copy logic. Bind callbacks to chat/topic/sender/card, short TTL and explicit confirmation; respond to stale token with a message, never silently drop. Status displays transfer bytes, preflight issue or recovery action. Reverse direction uses the same package/receiver protocol, with outbound local pull when VPS cannot dial local. Verify both CLI and callback dispatch (installed plugin discovery, not only backend unit tests).

### 5. Native adapters only where proven

Candidate files: `src/untether/session_migration/adapters/pi.py`, `adapters/omp.py`, `adapters/claude.py`, `adapters/codex.py` **only as required**, with per-engine fixtures under `tests/`. Inspect provider public APIs and existing scanner first; prefer supported provider export/import APIs over copying undocumented storage. For each adapter demonstrate: snapshot consistency, imported session survives agent restart on disjoint Windows/Linux stores, real follow-up uses migrated context, no stale absolute-path trust grants, collisions handled, no provider core edits. If provider refuses/does not support native import, show `Copy context (new session)` instead. Native adapter delivery is not a prerequisite for the ACP context route but **is** prerequisite before advertising `Move native Pi/OMP session`.

### 6. Real two-endpoint acceptance and rollout

Use disposable local + VPS test identities and a disposable project **only after explicit permission for SSH commands and remote changes**. Exercise local→VPS→local, session fact recall after agent process restart, two bots with same allowed Telegram user ID and distinct tokens, dirty/untracked file warning, occupied session, drop mid-upload, lost acknowledgement, changed host key, archive bomb, symlink/path escape, disk exhaustion, and retry. Check no duplicate owner, no unexpected source deletion, no secret in logs. Run focused pytest with explicit scanner roots (never real home scans), complete regression baseline, Ruff, `python -m py_compile`, and installed-command discovery. The user's current live Untether process is not restarted/reinstalled without separate approval. Record exact supported engines/modes and operator commands in [[../how-to/operations]] and [[../reference/config]].

## Linux VPS portability — mandatory product requirement

The feature MUST support any SSH-reachable Linux VPS, **with or without Coolify**. Woody's Coolify deployment is one local configuration example, never the default or a product dependency. Support two explicit execution modes through the same receiver protocol: `native` (Untether installed directly, including a venv, standalone executable, or user service) and `container-wrapper` (Docker/Podman, with or without Coolify). Resolve the executable and state/project roots from the peer profile and validate them at runtime. Do not require a particular distribution, package manager, systemd, root access, Docker, Coolify, fixed host paths or a public control port. Unsupported prerequisites must produce actionable refusal, not an implicit installation.

Preflight negotiates Linux OS/architecture, protocol capabilities and execution mode. For native mode verify executable, user permissions and actual agent environment; for container mode additionally verify container selector, in-container executable/UID and mount mappings. Use the same snapshot/receipt/rollback contracts regardless of mode. Include isolated acceptance cases for native Linux, ordinary container deployment without Coolify, and Coolify-managed container; both transfer directions must pass. SSH/auth/project/agent compatibility prerequisites still apply: 'any VPS' is portability, not a guarantee for an offline or unconfigured server.

## Availability fuses and production deployment correction (review update)

Gemini/Argon reviewed the sanitised design in https://gemini.google.com/app/80a6f229328ab915 (2026-10-25). Accepted: explicit bot handoff, exact protocol handshake, workspace parity, quiescence deadlines, link rejection and lost-receipt recovery. Rejected: blindly rewriting transcript strings/line endings (preserve evidence; remap structured paths and add destination environment notice), Git-storing sensitive session state, mandatory VPN, and unbounded in-memory streaming. Native SSH config aliases are an optional simplification, not a replacement for host verification.

**Telegram correction:** different bots cannot transparently transfer a private conversation. The destination must bind its own configured chat/topic after the user has opened/authorised that bot, or explicitly mapped a shared group/topic. Return a destination bot link and success card; never claim a local pointer changes the other bot. Source stays fenced until destination receipt is reconciled. First implementation should default to context copy unless cross-endpoint fencing is proved.

**Newest local VPS evidence:** Woody `VPS_OPERATIONS_RUNBOOK.md` and `docs/audit/maintenance-20261025.md` supersede September pre-install audits. They describe Coolify-managed **container** Untether 0.35.6 + Pi 0.84.4, persistent host mounts `/srv/data/untether` and `/srv/data/pi`, projects under `/srv`. Container-running evidence is not agent/Telegram health. Host SSH alone does not prove a host-level Untether CLI exists. Do not assume `/opt/untether/venv/bin/untether`. Implement a narrowly allowed host-side receiver wrapper with a fixed container selector and verified in-container executable/UID/mount mappings; Docker group/root privileges must not be silently granted. Wrapper deployment needs separate operator approval. Do not copy Woody private addresses/identity paths into public Git or advisor prompts. Keep actual endpoint details in local ignored inventory. The current VPS Pi installation does not prove ACP availability: native Pi adapter or explicit compatible ACP installation is required before this user's real session is supported.

Every transfer must run a fresh bounded check chain in **both** directions and recheck critical conditions before activation:

| Fuse | Required evidence / refusal |
|---|---|
| Availability | TCP/SSH connection within deadline; distinguish offline/DNS/timeout/auth/host-key failure, no ping-only health |
| Endpoint identity | pinned SSH key and expected endpoint identity, least-privilege account; no auto-enrolment |
| Runtime | compatible receiver protocol and installed command registry; actual container ready, correct UID/mounts and writable state |
| Agent | executable and version, adapter capabilities, provider authentication readiness without leaking tokens, bounded harmless persistence probe |
| Provider/network | required provider/DNS reachable and authentication usable; do not mark ready merely because binary exists |
| Workspace | explicit destination map, existing root, Git remote+commit and dirty/untracked comparison; refuse incompatible state, never auto-stash/push |
| Storage | free bytes/inodes and quota for compressed+expanded archive plus rollback headroom; write/read/delete probe in staging |
| Sessions | no active turn, children, tools or pending permission; bounded drain then abort (no forced kill) |
| Telegram | destination bot configured, distinct token, intended user/chat/topic authorised and reachable; no bot-token values in receipt |
| Concurrency | session and project operation locks, generation/fencing epoch, durable idempotent receipt reconciliation |
| Completion | independent process restart/load, destination context check and durable target binding before success |

Expose settings for connect/probe/drain/transfer/import/reconcile timeouts, max retries, retry backoff, max archive/expanded bytes, staging TTL and disk headroom. Finite retries only; permanent auth/trust/schema failures never retried. Health results carry timestamp and TTL; green setup yesterday is not proof today. Emit redacted structured events with operation/trace id, phase, duration, failure code, recovery action. No silent callback returns. Interrupted transfer never overwrites native stores or deletes the source. Reconciliation is an explicit state, not a false success or automatic source unfreeze.

### Implementation handoff scope

ChatGPT implementation must branch from the published plan commit, inspect all existing archive/native/session interfaces and build the profile schema before enabling runtime config. No speculative claim that this source-only draft is installed. Use the versioned example config in `docs/reference/session-migration.example.toml`. Actual local inventory is deliberately outside Git; no VPS/SSH/private key details may be requested through ChatGPT. No live remote commands, installations or restarts are authorised. Implement in a pull request, with test evidence and exact supported engines. Preserve all unrelated local working-tree edits; they are not part of this documentation commit.

## Acceptance / stop conditions

- After one-time setup, one action starts and tracks a transfer; no manual archive/SCP/path typing per session.
- Both directions work within **documented network topology**. If VPS cannot SSH into local, local bot initiates `--from vps`; do not promise VPS-initiated push.
- Destination restart and follow-up prove retained context; native-state claims require native adapter evidence, not ACP fixture success.
- Failed/interrupted transfer leaves source usable or explicitly fenced for reconciliation; never reports `moved` before a receipt and owner check.
- Changed/unknown host key, key outside selected folder, malformed archive, or untrusted destination refuses without executing content.
- Project files, secrets, in-flight tools, hidden reasoning and native provider state are excluded unless a separately tested adapter/bundle explicitly includes them. Missing prerequisites show an actionable message rather than silently switching bot/session.

**Approval gate:** implementing local code is separate from authorising remote SSH probes, installations, process restarts, and copying sensitive user conversations. Ask before any such live action.
