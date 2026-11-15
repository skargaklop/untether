---
title: ACP-wide session transfer goals
status: active clarification; not delivered
date: 2026-10-29
---

# ACP-wide session transfer: goals and gates

Authority: [[session-migration-product-correction]]. The operator explicitly rejected defining the product by four or five installed engines. This document clarifies implementation direction without reducing fidelity or acceptance requirements.

## Product goal

After one-time peer configuration, `/session move <session-id> --to|--from <peer>` transfers an authorized session between compatible instances of the same engine through a provider-neutral ACP-facing system. No per-session paths, descriptors, manual archives, or fixed engine allowlist are the public workflow. Cross-engine conversation conversion is not implied.

```text
/session move
      |
      v
Authorized ACP discovery -> capability/compatibility negotiation
      |
      v
Quiescence -> supported persistence extension -> verified transfer
      |
      v
Independent restoration -> chat/topic binding -> recoverable receipt
```

ACP compatibility alone is not proof of native persistence portability. The system must discover and explain that boundary honestly, while investigating supported extensions rather than stopping at refusal.

## Ordered delivery goals

| Goal | Deliverable | Observable acceptance |
|---|---|---|
| G1 — Establish actual protocol/tool capabilities | Pinned primary-source comparison of ACP, acpx, hydra-acp and other credible existing tools discovered during research | Record repository/revision, license, supported methods, persistence locations and runnable probes. Distinguish client history, transcript reseeding and real native restoration. Test with an empty destination store; no inference from method names. |
| G2 — Select the smallest reusable foundation | Evidence-backed architecture decision: reuse, wrap or extend an existing tool; custom code only for demonstrated gaps | Compare at least three approaches against fidelity, generic extensibility, assets, recovery and maintenance. State why the selected approach satisfies the contract and which protocol gaps remain. No speculative dependency installation. |
| G3 — Implement provider-neutral integration | Discovery and persistence-capability contracts wired into one orchestration path | An additional compatible engine/provider can participate through documented capabilities/extensions without modifying central dispatch or adding a hardcoded engine branch. Existing native adapters are optional implementations, not the product definition. Validate extension inputs/outputs and compatibility before mutation. |
| G4 — Preserve complete state | Original records, tools, metadata, assets and stable logical/native binding | Independently verify bytes/ordered records and referenced assets after restore and restart. No global path rewriting or transcript fallback. Demonstrate any unrepresentable state with primary-source and runtime evidence; obtain an explicit decision before changing acceptance. |
| G5 — Deliver the one-action lifecycle | Both directions, round trip, source exclusion, destination registration and durable retry | Two distinct sessions through one configuration; independent restart/follow-up; active-turn exclusion; interruption/lost-ack reconciliation; no duplicate import; foreign-owner/engine rejection; bounded subprocess/archive limits. Fixtures supplement, not replace, real engine evidence. |
| G6 — Demonstrate breadth, not a fixed roster | Reproducible capability-discovery and compatibility verification command | Coverage is derived from the declared ACP ecosystem/discovery surface, with named tested revisions and explicit evidence gaps. Include an independently added extension to test extensibility. A green installed-four matrix is insufficient. Neither an open registry nor a refusal counts as restored-engine coverage. |
| G7 — Prepare scoped delivery | Setup/limits documentation and reviewed scoped PR | Acceptance matrix maps all product requirements and G1–G7 to reproducible evidence. Preserve unrelated work. Do not merge, deploy, reinstall/restart live Untether or modify remote infrastructure. |

## Immediate work order

1. Preserve current native-adapter work and identify which pieces are reusable; do not discard it or expand the roster as a substitute for G1–G3.
2. Investigate acpx/hydra-acp and alternatives with pinned sources and disposable-store probes. Earlier claims about their limitations must be revalidated against the inspected revisions.
3. Write the foundation decision and capability contract from that evidence, then implement the general path in bounded increments.
4. Reconcile the existing engine matrix as subordinate compatibility evidence and rerun lifecycle/safety verification on the selected architecture.

## Evidence and decision rules

- Unknown capability is a research item, not a fabricated success or a demonstrated impossibility.
- No documentation edit can turn incomplete integration into delivered support.
- Shell/SDK persistence checks must be labeled accurately; they are not model-generated follow-up turns.
- Installed harness upgrades are maintenance, not proof of transfer support. Respect the existing npm minimum-release-age policy and preserve installation methods.
- Scope/acceptance changes require explicit operator confirmation. This document does not remove any engine or mandatory requirement from the active goal.

Related: [[../reference/session-migration-acceptance-matrix]], [[../reference/session-migration-implementation-status]], [[../reference/session-migration-verification]].
