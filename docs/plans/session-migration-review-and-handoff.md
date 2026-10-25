# Session migration review and implementation handoff

## Bindings

| Project / branch | Tool | Thread | Scope |
|---|---|---|---|
| skargaklop/untether / docs/ssh-session-migration-handoff | Gemini Argon | https://gemini.google.com/app/80a6f229328ab915 | Sanitised design review; no endpoint credentials |
| skargaklop/untether / implementation branch requested | ChatGPT | https://chatgpt.com/c/6ade1bb1-1774-83ea-8dca-0e88272093db | Assignment submitted and visible; GitHub write capability awaiting verification; no live infrastructure |

## Advisor review disposition

Gemini/Argon review obtained 2026-10-25. Accepted: bot-token conflict and no transparent private-chat handoff; workspace parity; NAT-aware pull; durable reconciliation; exact protocol version; timeout and archive link guards. Declined: replacing strings/line endings across historical transcripts; automatic Git stash/push; sensitive sessions in Git refs; mandatory VPN; unbounded in-memory receive. Existing bounded private staging is retained. Review was a sanitised summary of the design, not a full code audit.

## Deployment investigation

Read-only leaf worker inspected newest Woody `VPS_OPERATIONS_RUNBOOK.md` and `docs/audit/maintenance-20261025.md`. They supersede pre-install September notes. Actual deployment is Coolify container + host persistent volumes, not the old planned systemd/host Python service. Agent availability and Telegram health are not proved by Docker running status. Actual address/key path inventory saved only outside repository in local `.untether/session-migration.inventory.toml`; fingerprint/wrapper/bot binding remain unverified and disabled. No credentials were read or sent externally.

## Pitfalls

- Existing Playwriter 0.7 CLI warns extension expects 0.8; connection still works. No tool upgrade performed.
- Gemini onboarding tooltip overlay intercepted clicks. Review succeeded in existing browser; no replacement browser launched.
- Plan directory is Git-ignored: selected public docs require explicit force-add, never broad staging.
- Local working tree contains unrelated uncommitted code and tests; documentation commit deliberately excludes them. GitHub implementer must inspect committed baseline and not assume local drafts are published.
- Current Untether Pydantic config forbids unknown fields; proposed migration settings are a template, NOT inserted in live untether.toml before schema implementation.

## Implementation assignment

Read `docs/plans/2026-10-25-one-action-ssh-session-migration.md`, companion HTML, pinned Coddy evidence, and `docs/reference/session-migration.example.toml`. Implement on a dedicated feature branch and PR. Include full endpoint availability fuses and container-aware receiver contract. Prioritise user's native Pi/OMP support only where adapter proves real persistence and resume; never sell ACP seed as native migration. Inspect existing tracked baseline first. Build schema and safety tests before enabling any migration. Do not request/commit private keys, endpoint inventory, Telegram/provider tokens. No SSH execution, deployment, service changes or live restarts are authorised by the coding assignment. Return PR URL, supported mode/engine matrix, regression/E2E evidence and unresolved items.
