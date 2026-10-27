# Session transfer acceptance matrix

Authority: [[../plans/session-migration-product-correction]]. This is a work tracker, not evidence of delivery.

| Required behaviour | Current implementation/evidence | Outstanding verification |
|---|---|---|
| One-action discovery-driven move in both directions | Telegram outbound catalog resolution; omitted-engine inbound uses targeted pinned inspection, configured receiver project root and exact header cwd membership; strict bounded replies | Cross-boundary command/receiver integration, CLI public syntax and discovery for non-file adapters |
| All same-engine ACP pairings | ACP registry is open-ended; validated executable-v1 profiles now register generic adapters | Verify actual engine integrations; registry/plugin compatibility alone is insufficient |
| Original records, tools, metadata and assets | Native archive hashes; Pi JSONL v3 file adapter | Complete referenced asset/index persistence and real engine round trips |
| Native identity or explicit stable binding | RestoreProof logical/native IDs; receiver receipts | Independent engine restart and follow-up with mapping retained |
| Quiescent source snapshot | Shared runner gates; mandatory native snapshot admission; cached operation-bound exports; explicit active ownership after verified return with prior fence linkage | External-writer boundary and real-engine quiescence evidence; power-loss durability |
| Two distinct sessions per endpoint and round trip | CLI fixture simulations; changed-record executable/file-bundle return tests check metadata, repeat pulls, runnable ownership, interrupted metadata publication and restart-probe retry | Real engine subprocess round trips and follow-up on disposable stores |
| Destination chat/topic registration | Transfer binding integration and command scoping tests | Final end-to-end registration verification |
| Lost acknowledgement recovery and duplicate prevention | Operation/intents/receipt records; native receiver reserves intent before import and refuses uncertain replay without reimport | Automatic interrupted-import reconciliation; full failure-injection verification |
| Malformed/foreign-owner/foreign-engine rejection | Protocol and native receiver tests | Re-run final safety suite |
| Bounded output, archive sizes and deadlines | Receiver and executable adapters reuse bounded process runner; configured timeout no longer gains five seconds; 20 focused transport/adapter tests pass | Final-revision malicious-process checks |
| Optional Coolify and outbound pull | SSH peer configuration and pull path | Final portable setup documentation |
| Reviewed local PR, no live changes | Existing working branch preserved; live process untouched; focused migration suite 443 passed/9 skipped; final full suite 4,253 passed/47 skipped/9 warnings; lint/format/migration types, whitespace and lockfile checks pass | Final bounded review and scoped PR update |

## Engine coverage truth boundary

Deployment reality: all engines are installed locally; the remote VPS has only Pi installed. Pi is therefore the only currently installed same-engine local↔VPS pairing; no live VPS transfer has been verified by this work. Codex/OpenCode/OMP native transfers require both a supported registered adapter and the engine on both endpoints; local engine installation alone does not establish transfer support. Unsupported refusal is a limitation, not success. The PR ships with Pi + OMP persistence evidence; Codex/OpenCode adapter registration is deferred by the operator's decision (2026-10-27), with Codex's append-only resume evidence already recorded in the implementation status.

Neither the plugin interface nor the Pi-format codec demonstrates all-engine restoration. The ACP registry is extensible, and every engine needs a supported persistence implementation and independent behavioural evidence. Unsupported capability refusal protects data but is not successful transfer and does not satisfy an engine coverage row. Installed-Pi cold-load evidence checks native ID, historical messages and unchanged bytes in disposable stores. OMP has bundle collect→restore→separate-process SDK cold-load evidence including the title slot and ordered messages; it still lacks a registered production adapter, coordinator wiring and follow-up-turn proof.
