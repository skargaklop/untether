---
title: Session-migration foundation decision (G2)
status: decision recorded from G1 evidence; implementation of the general path continues
date: 2026-11-02
---

# Foundation decision: extend Untether's persistence seam

Authority: [[../plans/session-migration-acp-wide-goals]] (G2). Evidence base:
[[session-migration-acp-foundation-research]] (pinned sources + executed probes).

## Decision

**Extend Untether's existing external persistence seam** (approach 3). The
ACP-facing provider-neutral path stays Untether's coordinator + adapter
registry + profile routing; no adoption of acpx archives, no wrapping of an
external ACP client/store, no new ACP wire method is required for the
foundation.

## Why, against the acceptance criteria

G2 requires comparing at least three approaches against fidelity, generic
extensibility, assets, recovery and maintenance, with executed evidence
outweighing labels:

| Criterion | A. Adopt existing client archive (acpx) | B. Wrap existing ACP client/store | C. Extend Untether seam (selected) |
|---|---|---|---|
| Native-state fidelity | **Fails by executed probe**: acpx 0.21.0 export/import leaves destination native store empty; follow-up turn fails (`native restore did not happen`); imported record carries a duplicated user turn | Hydra-acp README (pinned 4a3f88a) admits "text-level handover… original internal state is not preserved" — takeover transcript, not native restore | Engine-native stores transferred byte-exact through dedicated adapters; already proven for Pi (JSONL v3 + RPC shell follow-up), OMP (journal+blobs+SDK coldload), Codex (rollout + app-server resume + shell follow-up), OpenCode (sync events + serve restart) |
| Generic extensibility | Tied to acpx record schema; every engine needs acpx-side support | Coupled to the wrapped client's extension contract; version-stability risk | `executable-v1` profiles + engine-keyed entry points admit any engine without central dispatch edits; profile routing is engine-data-driven |
| Assets | Archive carries client-visible content only; referenced spill files/artifacts not enumerated | Not addressed by takeover transcripts | Native archive records + hashed asset manifests already implemented (Pi spill logs, OpenCode tool-output assets, OMP blobs fail-closed) |
| Recovery | Single-shot import; no lost-ack reconciliation | Wrapper-dependent | Operation intents, durable receipts, reclaim fences, duplicate-prevention and retry semantics already implemented and tested |
| Maintenance | External schema churn (0.21.x series active); duplicated client identity vs native identity | Full daemon dependency (hydra is a Rust daemon + extension trust model) | In-tree, small modules, existing test surface (828 passing) |

Primary-source impossibility boundary (objective clause): none of this is a
protocol impossibility claim. ACP v2 `session/resume`/v1 `session/load`
restore context in the agent; they do not transport native stores. The
spec-sanctioned `_`-method/`capabilities._meta` seam (pinned extensibility
docs, revision 91f0265) remains available for future engines that advertise
native persistence — currently unused by any inspected engine.

## Which protocol gaps remain

1. No standard ACP method carries native engine state; portability is
   engine-specific (profile formats). Discovery of which engines expose
   native persistence is capability-negotiation work (G3), not archive work.
2. LLM/model follow-up turns are not proven anywhere (offline shell/SDK
   restart probes only); this remains a mandatory open row.
3. Real-SSH-transport coordinator round trips remain replaced by injected
   in-process receiver calls in tests.
4. Hydra-acp runtime behavior is classified from the vendor's own pinned
   README admission; no local runtime probe was executed (no local install,
   Rust build out of proportion to the classification question).

Related: [[session-migration-acceptance-matrix]].
