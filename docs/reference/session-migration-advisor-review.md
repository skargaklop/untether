# Argon advisor checkpoint

Thread: https://gemini.google.com/app/80a6f229328ab915
Project: Untether; branch: feature/ssh-session-migration-preflight. Existing authenticated browser/thread reused personally; visible mode was Argon. Only a sanitised project review packet was submitted. Screenshot: local `argon-advisor-checkpoint.png` (not a release artifact).

## Advice, not implementation evidence

Argon concludes ACP alone cannot provide universal native persistence and recommends engine-specific persistence drivers, compatible codec preflight, cold-restart continuation verification, source fencing after snapshot and lost-receipt idempotency checks. It explicitly distinguishes inference from its protocol claims. Its response did not supply precise source-line citations and is not proof of any installed engine's behaviour.

## Independently checked

The public session-setup documentation at https://agentclientprotocol.com/protocol/session-setup explicitly says clients must not call `session/load` when the capability is absent, and describes loading an **existing** session. This supports capability gating; it is not an import API. Further engine source verification remains required.

## Accepted directions

- Separate ACP runtime control from faithful native persistence integration.
- Prove original records/assets and continuation linkage after an independent process restart.
- Keep source admission fenced across snapshot and recovery, not merely during the read.
- Retry matching operation IDs/hashes without duplicate imports after lost acknowledgements.

## Advice not adopted as stated

- Generic path rewriting conflicts with the corrected contract's prohibition on global rewriting. Any engine-required mapping must be explicit, scoped and verified without altering original archive records.
- A shared advisory file gate does not stop arbitrary external writers. Process teardown and supported engine quiescence must be established; killing the live Untether process is not authorised.
- A 'signed receipt' is a suggestion, not an implemented or previously approved requirement. Existing authenticated SSH plus validated operation receipts must be evaluated before adding another protocol.

## Remaining

Actual all-engine restoration coverage, supported codec negotiation and durable source fencing are still open. Capability refusal is not delivery of session transfer.
