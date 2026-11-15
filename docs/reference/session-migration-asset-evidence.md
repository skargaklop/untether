# Session-migration asset evidence (extracted sources)

Scope: source evidence for the asset-fidelity row of [[session-migration-acceptance-matrix]]
("Original records, tools, metadata and assets"). Read-only inspection of extracted
snapshots; no live sessions, stores or user data touched. Adapter coverage below is a
**current gap**, not a proof of impossibility, until a probe/design concludes.

## 1. OpenCode v1.17.13 — engine-owned tool-output spills

Snapshot: extracted at commit `10c894bdeef3618f5666fb506ef7f9491bb964d8`
(`package.json` `repository.url = https://github.com/anomalyco/opencode`, package
`packages/opencode/package.json` version `1.17.13`).

Primary URL (tagged commit path):
`https://github.com/anomalyco/opencode/blob/10c894bdeef3618f5666fb506ef7f9491bb964d8/packages/core/src/tool-output-store.ts`

Excerpts (relative paths under the snapshot):

- `packages/core/src/tool-output-store.ts`
  - `export const MAX_LINES = 2_000`, `export const MAX_BYTES = 50 * 1024`, `export const RETENTION = Duration.days(7)`
  - `export const MANAGED_DIRECTORY = "tool-output"`
  - `const directory = path.join(global.data, MANAGED_DIRECTORY)`
  - `const file = path.join(directory, \`tool_${Identifier.ascending()}\`)` — spill name carries no session/tool-call identity and no content digest
  - under limits: `return { output: input.output, outputPaths: [] }`
  - over limits: `outputPaths: [outputPath]` plus marker `` `... output truncated; full content saved to ${outputPath} ...` ``
  - `cleanup` deletes `tool_*` files older than `RETENTION` (hourly `cleanupLayer`)

- Spill **references** do ride the event stream, the **bytes** do not:
  `packages/core/src/session/runner/publish-llm-event.ts` (`outputPaths` param, `outputPaths,` in the `SessionEvent.Tool.Success` payload), `packages/core/src/session/runner/llm.ts`, `packages/core/src/tool/registry.ts`
  (`Settlement.outputPaths`, `bounded.outputPaths.length > 0 ? { result, output: bounded.output, outputPaths: bounded.outputPaths } : ...`), `packages/core/src/session/message-updater.ts:313` (`outputPaths: event.data.outputPaths ? [...event.data.outputPaths] : []`).
  So replay re-carries paths; the referenced files live in the engine-owned
  `Global.data/tool-output` directory with 7-day retention, outside the record stream.

- Inline base64 is a **separate** mechanism, not an alternative to the spill path:
  `packages/core/src/config/attachments.ts` (`max_base64_bytes: PositiveInt`), `packages/core/src/image.ts`
  (`maxBase64Bytes: image.max_base64_bytes ?? 5 * 1024 * 1024`, `Image.normalize` takes and returns
  `FileSystem.Content & { encoding: "base64" }`).

## 2. Codex `rust-v0.163.0` — two distinct attachment APIs

Snapshot: `openai/codex` tag `rust-v0.163.0`, ref SHA `129b8ec7ee6f4511557a4856279f529025c25af4`
(`https://api.github.com/repos/openai/codex/git/refs/tags/rust-v0.163.0`).

**State thread attachments** (durable, metadata-only, no bytes):
`codex-rs/state/src/model/thread_attachment.rs` and `codex-rs/state/src/runtime/thread_attachments.rs`
(`//! Transactional thread attachment storage using the attachment SQL schema.`), params in
`codex-rs/thread-store/src/thread_attachments.rs`, local adapter
`codex-rs/thread-store/src/local/thread_attachments.rs`, RPC
`codex-rs/app-server/src/request_processors/thread_attachments.rs`, wire contract
`codex-rs/app-server-protocol/src/protocol/v2/thread_attachment.rs`, schema
`codex-rs/app-server-protocol/schema/json/v2/ThreadAttachment*.json`.

- `/// Stable, server-assigned UUIDv7 attachment identity.` → `pub id: String`
- `/// Integer Unix timestamp in seconds when the attachment was attached.` → `pub created_at: i64`
- `pub attachment_type: String`, `pub identity_key: String`, `pub payload: Value` (bounded, client-defined; module doc: `membership changes independently of resource contents`)
- `AddThreadAttachmentParams` / `ListThreadAttachmentsParams` / `RemoveThreadAttachmentParams` / `ListThreadAttachmentThreadsParams`

**External attachment store** (bytes, store-assigned `file_id`):
`codex-rs/attachment-store/src/lib.rs` (`//! Storage-neutral attachment persistence interfaces.`).

- `fn upload(&self, request: UploadRequest) -> UploadFuture<'_>` / `fn resolve<'a>(&'a self, request: ResolveRequest<'a>) -> ResolveFuture<'a>`
- `pub file_id: &'a str` and `pub download_url_ttl: Option<Duration>` ("Minimum remaining lifetime required for the returned file URL")
- `pub enum UploadResult { Inline { bytes: Vec<u8> }, File { file_id: String } }`; doc: "Implementations choose whether uploaded attachments remain inline or become file references."
- `AttachmentMetadata { file_name, digest (MD5), size_bytes, mime_type, format_specific, file_url }`
- Earlier probe schema cached at `D:/Temp/codex163-attachments/probe-schema/` (`ClientRequest.json`, `codex_app_server_protocol.v2.schemas.json`).

## 3. Reconstruction and current adapter gap

```text
OpenCode:  tool result ──bound()──> [ spill bytes: Global.data/tool-output/tool_<ascending> (7d, engine-owned) ]
                                  └─> [ event stream: outputPaths: string[] ]  ──> replay re-carries paths only
           image/file parts ──> inline base64 content (config max_base64_bytes) — separate mechanism

Codex:     state thread attachment: id (server UUIDv7) + attachment_type + identity_key + payload + created_at (unix s)
                                  -> membership/metadata only; no bytes in this record
           attachment store:        upload(data, ephemeral, file_name) -> Inline{bytes} | File{file_id}
                                    resolve(file_id, download_url_ttl) -> AttachmentMetadata{..., file_url}

Untether:  adapters (src/untether/session_migration/codex_rollout_adapter.py, opencode_*.py)
           currently carry no reference to these asset APIs (grep for
           attachment/outputPaths/tool-output under src/untether/session_migration/ returned nothing)
           => acceptance-matrix asset row stays open: neither spill paths nor thread
              attachments / file_id references are exported, remapped or restored yet.
```

## 4. OpenCode relocation boundary

The source marker embeds the same absolute path as `outputPaths`; copying bytes to a
new root alone does not make that original reference resolve. The adapter must not
rewrite the original events to conceal this difference. In particular, a Windows
absolute path is not a usable native absolute path on Linux. The pinned
[`database/path.ts`](https://github.com/anomalyco/opencode/blob/10c894bdeef3618f5666fb506ef7f9491bb964d8/packages/core/src/database/path.ts#L14-L19)
accepts drive-letter storage paths only when `process.platform === "win32"`, otherwise
throwing `Path is not absolute`. This is source evidence of a path compatibility
boundary, not an executed cross-platform restore test or proof that every possible
external integration is impossible.

A bounded follow-up must verify byte collection and safe destination placement,
then independently test whether the unchanged references remain usable. Merely
archiving orphaned bytes does not close the fidelity row. Cross-platform path
compatibility remains unresolved; unsupported refusal does not count as delivery.

Codex open checks: whether `file_id` references survive destination-side `resolve`;
whether thread-attachment `payload` needs identity rebinding on import. The live
attachment RPC probe is still required. No user data, live store or credential was
read for this note.

## 5. Observed Codex thread-attachment add/list RPC probe (disposable homes)

Disposable probe: `trash/codex-attachments-probe/probe.py` — a standalone JSON-RPC
app-server client reusing the transport and env-isolation shape of
`tests/test_session_migration_codex_runtime.py::_shell_probe` (read-only reuse; no repo
import). Exact invocation, hard 45 s budget (`DEADLINE = time.monotonic() + 45.0`,
enforced per `_receive`):

```powershell
cd D:/Temp/untether-native-migration-review/trash/codex-attachments-probe
python probe.py
```

The script resolves its own root (`ROOT = Path(__file__).parent`), so it must run from
its folder (`diag.py` does `from probe import AppServer, ROOT`). It writes
`report.json` next to itself, never runs model turns, uses no credentials or network,
and two disposable `CODEX_HOME`s. Default `CODEX_CMD` is the npm launcher
(`C:/Users/DELL E5570/AppData/Roaming/npm/codex.cmd`, overridable via
`UNTETHER_TEST_CODEX_CMD`), wrapped as `COMSPEC /c ...`. Execution trace
(`__pycache__/probe.cpython-313.pyc`) shows Python 3.13 ran it. Observed run:
`report.json` `ok: true`, `elapsed_seconds: 9.31`.

Observed results (`trash/codex-attachments-probe/report.json`):

- `thread/attachment/add` outcome `created` returns the full attachment: server-minted
  `id` in UUIDv7 form (home-a `01a18428-cdf0-78e2-a64f-67c2677206f2`, home-b
  `01a18428-e756-7563-8b05-f37e87591422`), caller-supplied `attachmentType`
  (`probe.artifact`), `identityKey` (`probe-identity-1`) and `payload` echoed verbatim
  (nested list/dict plus a Cyrillic string), and server-minted `createdAt` unix seconds
  (`1793218629` / `1793218635`).
- Original id/time survive restart: home-a `persistence_after_restart: true` — a fresh
  app-server against the same home listed data identical to `list_after_add` (`id` and
  `createdAt` unchanged).
- Idempotency: repeat add with the same `identityKey` but a mutated payload returned
  `outcome: "existing"` and kept the original `id`, `createdAt` and `payload`
  (`repeat_kept_original_id/payload/created_at` all `true`).
- Cross-home minted fields: identical caller inputs across two disposable homes gave
  `equal_fields: [attachmentType, identityKey, payload]` and
  `differing_fields: [createdAt, id]` — only the server-minted fields differ.
- Prerequisite: `thread/start` alone left no persisted row; one offline
  `thread/shellCommand` turn (`echo probe`, `prep_shell: ok`) flushed the rollout and
  satisfied the attachment store's threads FK without any model call.

Boundary: this demonstrates the state thread-attachment **add/copy contract only** —
persistence, idempotency and restart durability, caller content echoed verbatim while
the server mints `id`/`createdAt`. It does not demonstrate `file_id` resolve of the
external attachment store, byte transfer, payload identity rebinding on import, or any
non-Codex engine; no broader impossibility is claimed. The thread-store DB local
adapter remains assessed as not a supported public extension point yet. This section
supersedes the "live attachment RPC probe is still required" note in sections 2 and 4
for the add/list contract only. No user data, live store or credential was touched.

## 6. Installed pi 1.4.0 — bash full-output spill to OS tmpdir (corrects prior gap conclusion)

Evidence: installed pi-coding-agent release 1.4.0 at
`C:/Users/DELL E5570/.pi/agent/install/releases/1.4.0/node_modules/@earendil-works/pi-coding-agent/dist/`
(`package.json` version verified `1.4.0`). Prior research (session-manager-only review)
incorrectly concluded there is no asset gap in pi's session records; this section
corrects that.

- `utils/output-files.js:16` — `return join(tmpdir(), \`${prefix}-${randomBytes(8).toString("hex")}${extension}\`)`
  with `OUTPUT_FILE_MODE = 0o600` (line 13): spill files live in the OS temp directory,
  outside the session record stream.
- `core/bash-executor.js:30` — `{ path: tempFilePath, stream: tempFileStream } = createOutputFileStream("pi-bash", ".log")`
  inside `ensureTempFile`; the stream is opened once output exceeds the cap (line 46)
  and again when `truncationResult.truncated` (line 89); the returned result carries
  `fullOutputPath: tempFilePath` (line 98, contract in `core/bash-executor.d.ts:25`).
- `core/agent-session.js:3184-3203` — `recordBashResult(command, result, options)`
  builds `bashMessage` including `fullOutputPath: result.fullOutputPath` (line 3192)
  and persists it via `this.sessionManager.appendMessage(bashMessage)` (line 3202),
  or defers via `_pendingBashMessages` flushed at lines 3229-3230. So the original
  session record text path embeds an absolute reference to the spill file.
- `core/messages.js:21-37` — `bashExecutionToText(msg)` ends truncated messages with
  ``text += `\n\n[Output truncated. Full output: ${msg.fullOutputPath}]`;`` (line 36,
  guard `msg.truncated && msg.fullOutputPath` line 35); the LLM transformer
  (`content: [{ type: "text", text: bashExecutionToText(m) }]`, line 86) shows the
  model only this text.

Distinction: the persisted record and the LLM-visible text carry only the absolute
path string plus the truncated tail; the full bytes remain in the tmpdir file and are
never reread by these files. A migration consumer that sees the path has not been
proven to reread the file — no caller-side reread evidence was inspected, so no
file-reread behavior is asserted here. Migration implication, mirroring section 4:
an unchanged tmpdir absolute path is an engine-owned, non-durable (OS temp cleanup),
platform-specific reference; the asset-fidelity gap therefore applies to pi 1.4.0 as
well, and byte collection must be verified by a bounded follow-up probe before the
row closes.
