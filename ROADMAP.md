# Roadmap

This roadmap reflects the project's direction based on recent development and community feedback. It is not a commitment — priorities may shift as the ecosystem evolves.

## Near-term

- **Additional transport backends** — Discord and Slack transports via the plugin system
- **Improved onboarding diagnostics** — expand `untether doctor` with network, permission, and engine health checks
- **Rename Untether to Parkside** — execute a clean project-wide rename across package/module identifiers, CLI command, configuration and state paths, plugin entry-point groups, documentation, release metadata, repository branding, and migration tooling. Preserve existing users through an explicit, versioned configuration/state migration; avoid permanent aliases or dual-write paths after the transition.
- **Documentation refresh** — audit and update every maintained user, operator, contributor, architecture, configuration, runner, integration, and migration document; remove stale Untether behaviour and unsupported claims, align examples with released commands/configuration, and add verification that cross-references remain valid after the Parkside rename.

## Mid-term

- **Web dashboard** — browser-based UI for monitoring active runs and session history through a CodBash integration
- **Multi-user support** — per-user permissions and session isolation in group chats. Treat immutable Telegram user ids as principals and authorise every action before command resolution, project selection, queue admission, resume lookup, cancellation, compact/handoff, callback actions, and result delivery. Store state by `(chat_id, topic_id, telegram_user_id, engine)` so users in one topic never share resume tokens, overrides, queues, or compact/handoff state. Define viewer, runner, operator, and admin roles; restrict projects, engines/models, privileged actions, and concurrency/cost limits per principal. Shared sessions require an explicit audited handoff. Existing group-chat state must be assigned by an administrator or preserved as disabled legacy/shared state — never silently claimed by the next sender. Verify cross-user isolation, callback ownership, queued/batched/handoff propagation, and safe migration.
- **Agent orchestration** — integrate Archon workflows as Untether’s orchestration backend
- **Cost tracking enhancements** — per-project budgets, weekly summaries (historical reporting partially shipped via `/stats` in v0.30.0)

## Shipped

- **Gemini CLI engine** — full integration with Google's Gemini CLI via stream-json (shipped across v0.34.x–v0.35.x)
- **Amp engine** — full integration with Sourcegraph's Amp coding agent via stream-json (shipped across v0.34.x–v0.35.x)
- **Webhook-driven workflows** — trigger agent runs from CI/CD events, GitHub webhooks, or external services (shipped in v0.28.0 as the triggers system with cron and webhook support)
- **Session statistics** — `/stats` command for per-engine run counts, actions, and duration across today/week/all-time (shipped in v0.30.0)
- **Generic ACP/ACP v2 engine adapter** — functional plugin backend for any ACP-capable engine with registry discovery, explicit engine configuration, full session lifecycle, streaming updates, Telegram permission buttons via `acp_control`, client facilities, and strict registry validation (shipped in v0.35.x)
- **End-to-end model override guarantees** — explicit per-run > topic > chat > engine/runner-default precedence, persistent-scope isolation, and propagation through new/resumed/queued/batched/handoff runs (shipped)

## Future

- **Self-hosted relay mode** — deploy a public relay separately from local workers so the Telegram-facing service can run remotely without exposing agent hosts to the Internet. The relay receives Telegram updates, authenticates/rate-limits users, maintains bounded idempotent queues, relays progress/results, and tracks delivery acknowledgements. Each local worker initiates an outbound TLS tunnel, retains all engine configuration, repository paths, credentials, and authoritative session state, and rejects expired, replayed, unsigned, or incorrectly scoped requests. Pair workers with revocable per-worker credentials; require a stable request id and explicit acknowledgement state to avoid duplicate execution after reconnect. On worker loss, report offline state and never claim execution or blindly retry uncertain jobs. Relay restart must retain queued-but-unacknowledged work. Message content is visible to the relay by design; transport encryption does not make it private from the relay host. Multi-user authorisation is the prerequisite: the local worker remains the authority for the forwarded Telegram principal and local policy.
- **Additional transports** — Matrix, WhatsApp, or other messaging platforms via the plugin system
- **Additional coding-agent engines** — requested carryover of Takopi Task 4. Research and, only where a stable headless protocol exists, add Droid, Cline, Kilo, Warp, Open Interpreter, Mimo Code, ZCode, and Kimi Code through Untether’s plugin runner contract, with captured protocol evidence, resume/config/docs, and fixture/live-gated tests. This is speculative research, not a migration acceptance gate.
- **Cross-engine tool-action detail parity** — requested carryover of partial Takopi Task 20. Capture real tool input fields and normalize command/path/pattern titles and narration segmentation for Codex, OpenCode, Pi/OMP, and Agy while preserving shared generic helpers and existing Grok/Claude behavior.

## Contributing

Have a feature idea? [Open an issue](https://github.com/littlebearapps/untether/issues) — we'd love to hear what you'd find useful.
