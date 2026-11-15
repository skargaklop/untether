---
title: ACP transfer foundation research
status: partial source review; runtime probes pending
---

# ACP transfer foundation: evidence, not a portability claim

Scope: [[../plans/session-migration-acp-wide-goals]]. No fixed engine roster defines delivery. Findings below do not amend acceptance or establish universal impossibility.

## Pinned sources

- [acpx import implementation](https://github.com/openclaw/acpx/blob/d75a50115070186f993a0a5c2f29ee8601089ccf/src/session/import.ts): archive schema contains session state and history; import parses the source record and copies it into a newly identified local client record. Controller fetched and inspected this revision. Retaining the provider session identifier is not independently proven native-store restoration.
- [acpx export implementation](https://github.com/openclaw/acpx/blob/d75a50115070186f993a0a5c2f29ee8601089ccf/src/session/export.ts): research worker reports serialized client state plus raw event history. Full export/store-contract review and empty-destination runtime probe remain open. Inline tool content may be present in events; do not claim that the archive retains no tools.
- [hydra-acp README, line 602](https://github.com/smagnuso/hydra-acp/blob/4a3f88a4c7470b3f82d8d44120f735c56c3a4703/README.md#L602): controller independently fetched the pinned source. Quote: "hydra spawns a fresh agent, runs `session/new`, and feeds the imported history back in as a synthesized takeover transcript". It explicitly calls this a "text-level handover" and states that original internal state is not preserved. This import path does not meet native-restoration acceptance as-is.
- [hydra-acp-archiver README lines 9–14](https://github.com/smagnuso/hydra-acp-archiver/blob/7111b19f870bbcb0920bb8735e2e1362e06eda22/README.md#L9-L14): controller fetched README and resolved master to this commit. The archiver delegates `GET /v1/sessions/:id/export` and `POST /v1/sessions/import` with `replace: true` to Hydra; it adds lineage/hash-based deduplication, monitoring and backend synchronization. This documented wrapper does not establish a separate native persistence implementation beyond Hydra's import. Repository metadata reports MIT. Implementation/runtime validation remains open; not selected. Initial README lookup on `main` returned 404 because this repository uses `master`.

## Public extension boundary verified

Controller fetched [acpx public contract lines 413–416](https://github.com/openclaw/acpx/blob/d75a50115070186f993a0a5c2f29ee8601089ccf/src/runtime/public/contract.ts#L413-L416): `AcpSessionStore` exposes `load(sessionId)` and `save(record)`. This is a reusable client-record seam, not an engine-native export/import API. Wrapping it would introduce acpx-record-schema coupling and does not alone close the persistence gap.

The controller cross-checked fetched ACP **v2** documents at `91f0265cf93fd88a3904c226f3e47593b8ec3fc0`: [session setup lines 134–165](https://github.com/agentclientprotocol/agent-client-protocol/blob/91f0265cf93fd88a3904c226f3e47593b8ec3fc0/docs/protocol/v2/session-setup.mdx#L134-L165) says Clients "MUST" call `session/resume` and the agent restores context; this is not an archive payload import. Lines 202 onward explicitly say agents are not required to persist every inserted user message. Cross-client resumption must not be generalized to empty independent agent stores. V1 uses `session/load`; do not interchange the method names. The [v2 extension rules lines 161–170 and 244–250](https://github.com/agentclientprotocol/agent-client-protocol/blob/91f0265cf93fd88a3904c226f3e47593b8ec3fc0/docs/protocol/v2/extensibility.mdx#L161-L250) permit underscore-prefixed custom requests and recommend namespaced `_meta` capability advertisement. This is a supported negotiation convention, not an existing portable native-state format or an implemented transfer service. Full v1 schema and runtime probes remain open; absence of a standard transfer method does not demonstrate universal impossibility.

Existing Untether `native_adapter.py` already has engine-keyed entry-point plugins and `ExecutablePersistenceProfile` allows an external implementation without a new engine branch. Controller reran executable registration (12 passed) and the combined Pi-import/executable-protocol checks (37 passed). These establish regression compatibility only. The scout's claim that no fixture protocol executable existed was incorrect: `tests/fixtures/native_persistence_engine.py` and `tests/test_session_migration_subprocess_adapter.py` already cover that boundary.

## Three approaches under evaluation

| Approach | Reusable value | Unresolved contract gap |
|---|---|---|
| Adopt an existing client archive directly | Existing client history and identity lifecycle | Must prove native restoration into a disjoint empty engine store and complete referenced assets; client-record import alone is insufficient |
| Wrap an existing ACP client/store | Potentially reusable discovery, sessions and protocol handling | Inspect public extension contract, licensing and version stability; demonstrate integration without duplicating Untether's existing runner |
| Extend Untether's existing external persistence seam | Existing provider-neutral coordinator, executable profiles and entry-point plugins | Capability negotiation, evidence-backed ecosystem coverage and real per-provider persistence remain to be established; an open registry alone is not universal support |

No new dependency or custom ACP method is selected by this report. Standard ACP session operations, tool archives and native storage transfer must be evaluated separately. Protocol extensions may be useful, but their necessity and upstream support have not yet been demonstrated.

```text
Client record/history imported
             !=
Native engine state restored and independently usable
```

## Reproduction boundary and confirmed CLI

Pinned `docs/sessions.md` lines 40–41 and `src/cli/command-registration.ts` lines 187–210 confirm **plural** `sessions export` / `sessions import`, not the previously proposed singular command. Example upstream syntax: `acpx codex sessions export NAME --output archive.json`, then `acpx codex sessions import archive.json --name restored`. Do not execute these examples against real user stores. A probe must override process home and native engine storage independently at both endpoints and disable implicit adapter downloads. `where.exe acpx` found no executable on the current PATH; this is not proof that no copy exists elsewhere.

Controller inspected pinned export source: archive fields include serialized session state and `history: AcpJsonRpcMessage[]`; native files/assets are not separately enumerated by that declared archive shape. This is source evidence, not an executed empty-store result. Pinned package metadata declares MIT licensing and Node >=22.13.0. No dependency has been installed for this probe. Release-age check at 2026-10-29T06:26Z: npm metadata reports 0.21.1 published 2026-10-27T03:25Z, below the configured three-day age; 0.21.0 was published 2026-10-24T00:42Z. Therefore a policy-compliant runtime probe must use an eligible release (and inspect that release's sources), not silently install the newer source-reviewed 0.21.1. `npm view ... --min-release-age=3` still reports latest metadata and is not evidence of install eligibility. The npm cache lookup found registry metadata, not a confirmed executable package.

## Eligible-release source cross-check

The controller fetched [`v0.21.0/src/session/import.ts`](https://github.com/openclaw/acpx/blob/v0.21.0/src/session/import.ts) and [`export.ts`](https://github.com/openclaw/acpx/blob/v0.21.0/src/session/export.ts). Both compare byte-for-byte equal to the previously pinned source snapshots (`git diff --no-index` returned zero for each pair). Import lines 299–346 parses an archive, creates a new client record, acquires local admission locks, writes event history and publishes that record. `buildImportedRecord` spreads the prior record, retaining its provider session identifier; it does not invoke an agent restore operation. This inspected import path is client-store restoration, not demonstrated native agent restoration. It is not a proof that no other supported persistence path exists.

## Next verification packet

1. Finish acpx public-store and export/import source review; pin the ACP specification revision and inspect advertised extension semantics.
2. Execute disposable empty-destination probes for candidate paths; retain structured failures as limitations, not success.
3. Inspect licenses before reuse. Record a foundation decision only after these checks; reuse existing Untether seams where they satisfy the same responsibility.

## Recovered extension-seam conclusion (2026-10-29)

A restart lost the extension-boundary worker; its partial transcript was
recovered (thinking/tool records only — no final assistant text) and
supplemented narrowly against the same pinned sources. Primary-source seam
(pinned `extensibility.mdx` at `91f0265…`): custom `_`-prefixed JSON-RPC
requests/notifications (unknown requests MUST return `-32601`; unknown
notifications SHOULD be ignored) and `capabilities._meta` reverse-DNS
advertisement; custom names MUST NOT sit directly on capability objects.
Gap in the current code: `src/untether/runners/acp/protocol.py` implements
initialize/authenticate/prompt/config/close/resume negotiation only — no
`_`-method dispatch, no `capabilities._meta` advertisement or parsing, and no
`_meta` propagation; `session_migration/native_adapter.py` keeps persistence
engine-adapter-local (`ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE`). So the
spec-sanctioned wire seam for advertising/consuming per-engine native
persistence is entirely unused. This is an identified seam, not evidence that
any engine implements `_`-prefixed export/import, and not an impossibility
proof; whether the ACP runner would reject unknown `_` methods
(`subprocess_adapter.py`, `runner.py`, `peer.py`) remains unverified. No new
implementation was selected by this recovery.

No live Untether process, remote infrastructure or user session store was changed by this research.

## Executed empty-destination probe: acpx 0.21.0 export/import (2026-11-02)

Runtime result on this machine (`D:/Temp/acpx-probe-0210`, acpx 0.21.0 from
the release-age-eligible tarball, `@agentclientprotocol/sdk` 1.9.0): a
controlled disposable ACP agent (real ACP stdio protocol via the SDK agent
API, per-session JSON files in an isolated `PROBE_NATIVE_STORE` directory)
drove the full `acpx sessions new → prompt → close → sessions export →
sessions import → follow-up prompt` lifecycle across two disposable host
homes (`HOME`/`USERPROFILE` redirected per host; global config
`<home>/.acpx/config.json` with an `agents.probe-agent.argv` array — raw
`--agent` strings are rejected on Windows).

Observed facts (probe log `D:/Temp/acpx-probe-0210/probe-last.log`):

1. Export/import completes with exit 0 and B's `sessions list --local`
   shows the imported record carrying the ORIGINAL agent session id
   (`acpSessionId: probe-…`, retained per `buildImportedRecord`).
2. B's native agent store remains EMPTY after import — acpx writes only its
   own client record + event log (`~/.acpx/sessions/…`), never agent-native
   state.
3. The follow-up prompt on B fails: acpx sends `session/load`, the agent's
   native store has no record for that session id, and the turn errors with
   `probe-agent: native store missing session …; native restore did not
   happen`. The imported session is NOT independently usable on the agent.
4. The imported client record also shows a duplicated user turn in
   `messages` (`User, User, Agent` for a single prompt), an incidental
   record-hygiene defect observed in 0.21.0, unrelated to restoration.

Conclusion: executed evidence confirms the pinned source review — acpx
`sessions export/import` transfers acpx client records (client history +
identity linkage), NOT agent-native state. An empty-destination import
yields a session that cannot survive a native follow-up turn. This is a
G1 limitation of acpx's built-in transfer, not native restoration, and not
a claim that acpx lacks other persistence mechanisms. Windows-specific
observation: per-agent argv config is required; queue-owner locks require
`sessions close` before export; agent-name subcommand must follow global
options.

Reusable probe artifacts: `probe-agent.mjs` (SDK-driven disposable agent),
`run-probe.sh` (isolated two-host lifecycle). No real user store was read
or written; no live process was touched.
