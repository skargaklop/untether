# Session migration — corrected public product contract

Status: binding correction to the transcript-copy implementation; not delivered or deployed.

This contract supersedes conflicting acceptance wording in the September ACP reseeding plan and October SSH handoff plan. Untether is an open-source product for all users, not a bespoke local/VPS script.

## ACP-wide scope and delivery goals

Operator clarification (2026-10-29): the product is a general ACP session-transfer system, NOT a migration product for a fixed roster of four or five harnesses. Pi, OMP, Codex and OpenCode are compatibility examples, not the scope boundary. Their adapters and passing checks cannot establish ACP-wide delivery.

The ordered, checkable goals and research gates are defined in [[session-migration-acp-wide-goals]]. This clarification does not waive original-record fidelity, assets, recovery, independent restoration or any other acceptance requirement below. A protocol limitation requires primary-source evidence and an explicit operator decision; it is neither successful transfer nor permission to silently narrow scope.

## Required user workflow

After documented one-time endpoint configuration:

```text
/session move <session-id> --to <peer>
/session move <session-id> --from <peer>
```

One command performs discovery, compatibility checks, quiescence, snapshot, transfer, restoration, verification, destination registration and recovery tracking. No per-session descriptors, manual archive/SCP steps, repeated path entry, or single-session receiver restriction. Any authorised session may be selected, across any same-engine ACP pairing. Native Linux and container deployments are supported; Coolify is optional. Outbound pull must work where the remote cannot connect inbound to the initiating machine.

## Identity and exact content

Preserve the native session ID when the engine supports restoring it. Otherwise preserve a stable Untether logical identity and explicit native binding, while retaining ALL transferable session content identically. Do not replace original conversation records with one synthetic transcript prompt. Preserve ordered messages, raw ACP events/tool updates, metadata and available referenced session assets without silent summarisation, truncation or global text/path rewriting. Preserve archive record hashes and lineage/generation across round trips.

ACP controls agents; it does not by itself specify portable native persistence. Use the same engine's supported export/import or a verified external persistence adapter to restore the actual session. Never claim universal native portability solely because both agents implement ACP. Missing restoration capability must be exposed explicitly; prompt reseeding is not an automatic fallback or successful migration.

## Referenced tools — inspect before implementing

- https://github.com/openclaw/acpx — inspect actual export/import, session records and upstream provider ID handling. Existing research warns that importing the client record alone does not populate an empty native store.
- https://github.com/smagnuso/hydra-acp — inspect bundles, lineage and import/resurrection. Existing research identifies reseeding into a new upstream session, not identical native restoration.

Evaluate these tools and other maintained ACP implementations before selecting a foundation or extending the harness-specific adapter roster. Compare native-store restoration, client-only state, transcript reseeding, identity, tool records, assets, extension points and licensing in a reproducible capability report. No candidate is selected merely by being named here. These are design evidence, not permission to invent missing ACP methods or copy incompatible databases. Reconcile findings against actual supported engine persistence APIs. Explain any hard protocol limitation with source evidence rather than silently reducing product scope.

## Acceptance

Use disjoint disposable endpoint stores and real subprocesses locally: two distinct sessions through one endpoint configuration; local-to-remote-to-local; original content identity/hash comparison including tools/assets; native ID retention or explicit stable mapping; independent restart and follow-up; destination chat/topic registration; active-turn exclusion; lost acknowledgement reconciliation; no duplicate import; malformed archive and foreign-owner refusals. Passing infrastructure tests does not establish session fidelity.

DevOps deploys only AFTER reviewed local delivery. Live receiver deployment is not a coding prerequisite. No live SSH, service changes or running Untether restart are authorised here. Keep existing unrelated dirty work intact.

## Branch decision

Reuse `feature/ssh-session-migration-preflight` only for verified reusable transport/security components. Remove or replace transcript-only migration claims and unneeded duplicate scaffolding; do not delete unrelated branches or work. The local uncommitted implementation checkout is not silently part of the published branch. No merge until the corrected contract passes controller review.
