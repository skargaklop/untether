# Coddy Agent: evidence for Untether local/VPS migration

## Scope and verdict

Inspected **https://github.com/coddy-project/coddy-agent**, not codbash, at commit **da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d**, obtained with `git rev-parse HEAD` after a shallow clone into `D:/Temp/coddy-agent-research`. No VPS connection, SSH command execution, service change, or upstream modification was performed.

**Coddy supplies remote control and resumable disk bundles; the inspected paths do not supply a transactional, bidirectional machine-migration feature.** Switching environments is not transferring a session. This is a bounded finding, not a whole-repository proof of absence. Untether's own implementation was not inspected here.

## Verified evidence

1. **Remote execution, not migration.** The guide states the client “keeps no session store” and proxies calls to the server. Code implements an HTTP/SSE-backed handler, not a bundle transporter: [remote guide, lines 3–13](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/operate/remote.md#L3-L13), [handler, lines 1–35](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/remote/handler.go#L1-L35). Environment selection changes which backend is consulted; it should not be labelled “move session.”

2. **State exceeds conversation text.** Bundles include metadata/cwd, original messages, assets, todos, plans, tool records, stats, nested child sessions, permission grants and pending state: [session layout, lines 5–31](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/features/sessions.md#L5-L31). Code confirms bundle constants and nested storage: [filesystem, lines 22–44](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/session/filesystem.go#L22-L44). Workspace source files are separate from this state.

3. **Export is not a restore archive.** `/export` renders conversation documents; attachment bodies are omitted, with paths/names retained. Do not use its JSON as a lossless migration format: [export contents, lines 42–59](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/features/session-export.md#L42-L59), [renderer contract, lines 3–7](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/session/transcript_export.go#L3-L7).

4. **Concurrency matters.** Bundle writers take process/file locks, but copying files externally does not acquire a consistent snapshot automatically: [filesystem locking, lines 178–195](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/session/filesystem.go#L178-L195). Turns use bundle locks; cancel acknowledgement is not proof of completion: [sessions, lines 69–73](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/features/sessions.md#L69-L73).

5. **Do not copy SSH security behaviour.** Default host verification automatically replaces changed keys; insecure mode ignores them entirely: [known_hosts, lines 16–61](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/tools/ssh/known_hosts.go#L16-L61). Key discovery tries only four conventional filenames and silently skips passphrase-protected keys: [keys, lines 11–38](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/tools/ssh/keys.go#L11-L38). SSH tool execution merely runs commands: [exec, lines 20–57](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/internal/tools/ssh/exec.go#L20-L57).

6. **API credentials are powerful.** Authentication is optional, HTTP has no native TLS, configured remote tokens may reach configuration readers: [remote security, lines 44–67](https://github.com/coddy-project/coddy-agent/blob/da8fdeb0882a10f5ae8c8c3920d65d73ce5ba63d/docs/operate/remote.md#L44-L67).

## Recommended implementation sequence

Considered three approaches: remote attachment only; transcript export/import; transactional bundle plus workspace transfer. **Choose the third for actual migration**, retaining attachment as a separately named convenience.

1. Inspect Untether's existing session adapter first. Define its minimum resume contract; Coddy's storage is evidence, not necessarily Untether's format. Configure VPS host/user/port, SSH key folder with explicit selected identity, pinned host fingerprint, and local/remote workspace roots. Keep private keys local; use SSH-agent unlock for encrypted keys. Never auto-accept changed host keys.
2. Implement one deterministic command, proposed `untether migrate <session> --to vps|local`, with one UI button invoking the same operation. First-use trust/encrypted-key prompts remain explicit; subsequent runs are one action. Use argument arrays, not shell interpolation; no agent forwarding or public control port. Prefer existing SSH/SFTP tooling.
3. Preflight both endpoints: compatible schema/runtime, provider availability, disk space, valid destination paths. Freeze source admission and drain active turns, children and background jobs; refuse migration when quiescence cannot be established. Snapshot persisted state under a migration-aware lock. Process memory, terminals and running jobs are **not** portable.
4. Transfer session and workspace separately into staging. Include dirty/untracked project work through an explicit manifest; exclude credentials, caches and private keys. Validate sizes/hashes, reject traversal/symlink escapes, enforce limits and private permissions. Preserve binary bytes and line endings. Reconstruct worktrees rather than copying machine-bound `.git` pointers.
5. Validate/import through the session adapter: remap cwd and structured asset paths, never globally replace transcript text. Reset host-specific grants, pending permissions and executable trust. Handle session-ID conflicts without overwriting. Atomically publish staged data, verify resume readiness without executing tools, then transfer ownership and switch UI. Retain source rollback data but prevent concurrent execution; use fencing, not merely an “archived” label.
6. Exercise both directions on disposable endpoints: dirty files, attachments, nested sessions, Windows/Linux paths, encrypted keys, changed host key, interrupted upload, disk exhaustion, duplicate retry and concurrent prompts. Require idempotent retries, no lost bytes, no double owner, and recoverable failure before calling migration complete.

```text
quiesce -> snapshot -> encrypted staging -> verify -> publish -> ownership switch
                  failure -> preserve source; resume only after fencing check
```

## Pitfalls and limits

SSH encryption does not make unsafe archive extraction safe. Migrated histories may contain secrets or hostile tool payloads. Never import approvals as authority. Hooks can execute again on resume; destination trust must be explicit. Cross-host ownership needs a durable migration receipt and recovery protocol, because atomic rename on one host is not a two-host transaction. History recall was attempted but unavailable because the deja CLI could not resolve its home directory.
