# Roadmap

This roadmap reflects the project's direction based on recent development and community feedback. It is not a commitment — priorities may shift as the ecosystem evolves.

## Near-term

- **Additional transport backends** — Discord and Slack transports via the plugin system
- **Improved onboarding diagnostics** — expand `untether doctor` with network, permission, and engine health checks

## Mid-term

- **Web dashboard** — browser-based UI for monitoring active runs and session history
- **Multi-user support** — per-user permissions and session isolation in group chats
- **Agent orchestration** — chain multiple engines in a single workflow (e.g., Claude for planning, Codex for execution)
- **Cost tracking enhancements** — per-project budgets, weekly summaries (historical reporting partially shipped via `/stats` in v0.30.0)

## Shipped

- **Gemini CLI engine** — full integration with Google's Gemini CLI via stream-json (shipped across v0.34.x–v0.35.x)
- **Amp engine** — full integration with Sourcegraph's Amp coding agent via stream-json (shipped across v0.34.x–v0.35.x)
- **Webhook-driven workflows** — trigger agent runs from CI/CD events, GitHub webhooks, or external services (shipped in v0.28.0 as the triggers system with cron and webhook support)
- **Session statistics** — `/stats` command for per-engine run counts, actions, and duration across today/week/all-time (shipped in v0.30.0)
- **Device re-authentication** — `/auth` command for headless Codex re-auth via Telegram (shipped in v0.30.0)

## Future

- **Self-hosted relay mode** — run the Telegram bridge on a remote server with secure tunnelling to local agents
- **Additional transports** — Matrix, WhatsApp, or other messaging platforms via the plugin system
- **Additional coding-agent engines** — requested carryover of Takopi Task 4. Research and, only where a stable headless protocol exists, add Droid, Cline, Kilo, Warp, Open Interpreter, Mimo Code, ZCode, and Kimi Code through Untether’s plugin runner contract, with captured protocol evidence, resume/config/docs, and fixture/live-gated tests. This is speculative research, not a migration acceptance gate.
- **Cross-engine tool-action detail parity** — requested carryover of partial Takopi Task 20. Capture real tool input fields and normalize command/path/pattern titles and narration segmentation for Codex, OpenCode, Pi/OMP, and Agy while preserving shared generic helpers and existing Grok/Claude behavior.
- **End-to-end model override guarantees** — requested carryover of substantially implemented Takopi Task 23. Document precedence and harness limitations; prove explicit per-run > topic > chat > engine/runner default behavior, persistent-scope isolation, and new/resumed/queued/batched/handoff propagation through `EngineRunOptions` and each native runner/ACP request without cross-scope bleed.

## Contributing

Have a feature idea? [Open an issue](https://github.com/littlebearapps/untether/issues) — we'd love to hear what you'd find useful.
