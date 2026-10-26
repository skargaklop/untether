# Untether — Repository Guidelines

## 1. Project & Architecture

**Untether**

A Telegram bridge for AI coding agents. Enables full remote control of Claude Code, Codex, OpenCode, Pi, Gemini CLI, Amp, OMP, Grok, and AGY from mobile devices and external chat interfaces. Executes background agents on local machines with interactive permission handling, voice transcription, cost tracking, operation card previews, and live progress streaming.

```
Telegram <-> TelegramPresenter <-> RunnerBridge <-> Runner (claude/codex/opencode/pi/gemini/amp/omp/grok/agy)
                                       |
                                  ProgressTracker
```

- **Runners** (`src/untether/runners/`): Engine-specific subprocess execution managers.
- **RunnerBridge** (`src/untether/runner_bridge.py`): Mediates communication between runner engines and the Telegram presenter.
- **TelegramPresenter** (`src/untether/telegram/bridge.py`): Renders live progress, inline keyboard menus, responses, and operation cards.
- **Commands** (`src/untether/telegram/commands/`): Command and callback query dispatch handlers.
- **Schemas** (`src/untether/schemas/`): `msgspec` structs and `pydantic` v2 models for streaming JSONL parsing and event contracts.

### Untether Transport & Engine Contracts

- **Runner 3-Event Contract**: Every execution run emits exactly:
  1. `StartedEvent`: Emitted once when the session identifier is established.
  2. `ActionEvent(s)`: Emitted zero or more times during execution steps.
  3. `CompletedEvent`: Emitted exactly once as the final termination event.
- **Event Construction**: Construct event instances exclusively through `EventFactory`.
- **Telegram Outbox Transport**: Route all Telegram Bot API communications through `TelegramOutbox`.
- **Callback Data Limits**: Maintain callback data within 64 bytes using the format `prefix:action:id`.
- **Callback Responsiveness**: Acknowledge callback queries promptly via `answerCallbackQuery` to dismiss UI button spinners.
- **Message Chunking**: Message limits remain at 4096 characters; split extended payloads across sequential message chunks.
- **Australian English Localization**: Use Australian English in user-facing strings (e.g., realise, colour, behaviour, licence).

### Untether Command Reference

| Command | Action |
| :--- | :--- |
| `/cancel` | Stop the active agent execution |
| `/agent` | Inspect or select the engine for this conversation |
| `/model` | Configure model overrides for an engine |
| `/planmode` | Adjust plan mode setting (on, auto, off) |
| `/usage` | Display API cost telemetry for the active session |
| `/stats` | View per-engine session metrics across daily, weekly, and total intervals |
| `/auth` | Re-authenticate Codex device credentials |
| `/export` | Export session transcripts |
| `/browse` | Browse workspace files and directories |
| `/config` | Launch the interactive settings configuration menu |
| `/verbose` | Toggle verbose progress output |
| `/restart` | Perform a graceful restart of Untether |

## 2. Core Agent Behavior & Ethics

- **Scope & Precedence**: Instruction precedence is: explicit user request > this root meta file > project-local instructions/plans > declaration-local contracts > narrower directory instructions. Read strictly the applicable project instructions and the smallest relevant plan, code, and tests before acting. Treat current code and runtime state as ground-truth evidence of what exists; report conflicting instructions directly rather than importing another project's rules.
- **Honesty & Transparency**: When information is absent or uncertain, explicitly state that the answer is unknown. Rely on retrieved, verified information instead of speculative guesses. Acknowledge and correct mistakes immediately upon discovery. Maintain directness and professional objectivity.
- **Analytical Thinking**: Always evaluate 3, 5, or 7 possible causes for a problem or ways to resolve a user request before acting. Focus repairs on the root cause rather than transient symptoms.
- **Implementation-First Continuation**: When implementation is approved or requested (e.g., "continue", "fix it"), proceed immediately in bounded, incremental steps to produce working application code. Prioritize direct application delivery, reserving audits, refactoring, and planning loops exclusively for concrete observed blockers.
- **Execution & Approval**: Ensure all modifications are visible and tracked. Obtain explicit user approval prior to executing infrastructure modifications or running dangerous remote host commands.
- **Evidence-Backed Progress & Actual-State Verification**: Clearly distinguish between a plan, a running process, passing tests, a working user path, and a deployed result. Confine progress and completion reports strictly to states verified on-disk or in-system, backing all completion claims with verified evidence.
- **Evidence-Backed Reporting & Direct Citations**: Back all analysis, findings, progress updates, and completion claims in user reports with concrete evidence, including direct quotes, code excerpts, reproducible commands, and clickable markdown links to exact source files and line ranges.
- **Long-Running Process Verification**: When testing or monitoring a long-running process, use the designated status, log, process, service, browser, or workflow tool for that system and verify health across multiple distinct checkpoints separated by justified wait intervals (e.g., `sleep 300`, `sleep 900`). Compare activity and outputs across checkpoints, requiring sustained stability across several observations before confirming success.
- **Communication & Frustration Signals**: When the user provides short imperative prompts (e.g., "fix it", "continue"), execute directly with context assumed from prior turns. When the user expresses frustration or forceful feedback, immediately halt the current approach, acknowledge the specific failure directly, and pivot entirely to executing the requested correction.
- **Proactive Context Management**: Manage context window headroom proactively using compaction commands before degradation occurs. Structure continuation summaries across defined anchors: primary objective, touched files, verified state, active constraints, and immediate next steps.

## 3. File Editing & Code Modification

- **Targeted Edits (Narrow Patching)**: Apply small, surgical patches to existing code. Keep diffs minimal and precise, preserving existing file structure, JSON schemas, and Markdown formatting. Maintain all source-code files under 1000 lines.
- **Core Immutability & SOLID Extension**: Treat upstream engines, vendor cores, framework libraries, and installed dependencies (e.g., installed `site-packages`, external agent CLIs) as strictly read-only. Implement all customizations, new behaviors, and integrations through public extension points—plugins, adapters, addons, modules, hooks, and external wrappers—in strict accordance with SOLID architecture principles (especially the Open/Closed Principle).
- **Document Mentions & Wikilinks**: Use `[[wikilinks]]` style explicitly (even in prose) for mentions and cross-references of other document files (e.g., `[[AGENTS.md]]`, `[[EXPERIENCE.md]]`, `[[DEVELOPMENT_PLAN.md]]`).
- **Documentation Diagrams & Dual-Format Artifacts**: When creating user documentation, plans, architecture records, or summary reports, embed appropriate visual representations: use ASCII diagrams for terminal output, ASCII diagrams or Mermaid blocks for Markdown documents, and rich interactive diagrams or schemas for HTML artifacts. By default, generate complementary Markdown and HTML files side-by-side to deliver an optimal viewing experience.
- **README Progressive Disclosure & Metadata Headers**: Author README files following the progressive disclosure principle, structuring content from a concise high-level overview down to comprehensive operational details. Include structured YAML frontmatter headers (similar to skill definitions) at the file beginning, and connect related repository documents using `[[wikilinks]]` formatting where appropriate.
- **Data & File Safety**: Preserve code and user files by archiving discarded local artifacts into an ignored project `trash/` directory. Maintain existing business data and database integrity by requiring explicit, idempotent, and rollback-safe migration plans for all schema and data updates.
- **Configurable Parameters**: Expose all operational limits, thresholds, retry counts, and page sizes in settings UI or configuration files, ensuring full user adjustability.
- **Exhaustive Deprecation & Reference Cleanup**: When deleting, renaming, or migrating a feature, provider, or setting, reconcile all references comprehensively across registry definitions, UI widgets, settings schemas, localization files, imports, and documentation.
- **Framework & Architecture Parity**: Maintain continuous parity between code and architectural artifacts. When operating within a project governed by structured architecture frameworks, update system maps, contracts, and verification logs alongside implementation changes.
- **Concurrency & Serialization**: Serialize all write operations to shared files (manifests, session journals, queues, state registries), ensuring thread and process safety against corruption.
- **Contract-Driven Guardrails (CDD)**: Protect application boundaries and internal state against malformed payloads and LLM hallucinations by enforcing strict schema validation (`pydantic` v2, `msgspec` structs) at entry and exit points.
- **Observability-Driven Development (ODD) & Rich Logging**: Embed runtime observability directly into code: structured JSON telemetry (`structlog`), plain-language 3-part error messages (what failed, why, recovery action), trace context propagation (`session_id`, `run_id`), and machine-readable diagnostics for automated self-healing.

## 4. Environment & OS Handling

- **Tech Stack**: Python 3.12+, `uv_build`, `anyio`, `msgspec`, `pydantic` v2, `structlog`, `rich`, `typer`.
- **Windows Encoding (CRITICAL)**:
  - Set `PYTHONUTF8=1` when running Python scripts handling Cyrillic on Windows.
  - Encode files containing Cyrillic as UTF-8 BOM (`utf-8-sig`) with CRLF line endings.
  - Parse JSON and Markdown files with UTF-8 BOM awareness.
  - Execute Cyrillic text operations via dedicated Python scripts with `PYTHONUTF8=1`.
- **Shell Usage**: Use Windows PowerShell native separators (`;`, separate lines, or explicit `if` statements). For remote Linux environments, run `dos2unix` on scripts transferred from Windows to ensure clean Unix line endings.
- **Cross-Platform Compatibility**: All code, scripts, tooling, and runtime configurations must maintain multi-platform compatibility across Windows (minimum Windows 10 and 11), macOS, and Linux. Rely on portable abstractions, standard path libraries, configurable tool paths, environment-aware branching, and cross-platform runtime conventions rather than fixed operating system assumptions.
- **Host Environment & Server Isolation**: Operate strictly within the assigned repository checkout using unprivileged commands. Treat host-level configuration as external host infrastructure. Escalate server daemon and network anomalies directly to the user with clear diagnostics.
- **Durable Research & Secret Redaction**: Distill external API documentation, protocol findings, and schema research into dedicated, narrowly scoped Markdown files under `docs/research/` with source URLs, retrieval dates, and reproducible examples. Restrict credentials, API keys, personal data, and auth tokens to runtime retrieval from environment variables or ignored configuration files.
- **Web Search & Content Extraction Tooling**: Prioritize using the `duckduckgo-search` skill for free web search. For converting web pages and documents to Markdown, prioritize Python `trafilatura` or CLI tools (`defuddle`, `markitdown`) whenever installed in the environment.
- **Binary Compilation & Executable Runners**: Prioritize producing standalone compiled binaries for executable tools, CLIs, and application entry points across target ecosystems whenever supported:
  - **Python**: Produce standalone compiled binaries or packaged executables using `uv` (such as `uv build`, single-file distributions, or embedded bundles) where feasible.
  - **TypeScript / Node.js**: Produce standalone compiled executable binaries using native single-binary tooling (such as `bun build --compile`, Deno compile, or Node.js Single Executable Applications) where feasible.
  - **Golang**: Compile native standalone binaries using `go build` with release flags (e.g., `-ldflags="-s -w"`).
  - **Fallback Standalone Runners**: Where compiling a standalone binary is unsupported or infeasible, generate robust standalone `.bat` runner scripts on Windows, configuring multi-tier runtime discovery, `PYTHONUTF8=1`, `chcp 65001`, and bilingual (English/Russian) diagnostic messages.
- **Standardized Launchers**: Provide standalone `.bat` entry-point launchers for Python projects on Windows, configuring multi-tier Python discovery, `PYTHONUTF8=1`, `chcp 65001`, and bilingual error messages.
- **Agent Client Protocol (ACP) Tooling Compliance**: When authoring or extending arbitrary tools, integration servers, or bridges for AI agents, strictly comply with Agent Client Protocol (ACP) schema contracts, standard event lifecycles, and payload specifications.

## 5. Engineering Principles

- **Mandatory Ponytail for coding**: Before every coding task—including writing, adding, fixing, refactoring, reviewing, architecture/design, and dependency selection—load and apply the `ponytail` skill at default `full` intensity. Keep it active for every response until the user explicitly says `stop ponytail` or `normal mode`. Prefer existing code, standard-library/native capabilities, and the smallest verified root-cause fix. Fully preserve required security, validation, accessibility, error handling, and explicit user requirements.

Planning and code design explicitly respect DRY, KISS, YAGNI, SOLID, CRAP avoidance, and God object / SRP control before approving architecture, contracts, steps, or execution packets:

- **DRY**: Remove duplication only when the shared responsibility is real, named, and verified.
- **KISS**: Choose the smallest understandable module shape that satisfies approved requirements.
- **YAGNI**: Exclude speculative features, future-proofing, unused interfaces, and configurable knobs.
- **CRAP avoidance**: Reduce risky complexity with small verified steps instead of adding abstractions around untested behavior.
- **God object / SRP**: Treat mixed dependency clusters, broad public APIs, and unrelated reasons to change as God object signals. Extract only when target units have one clear responsibility and verification can prove behavior was preserved.
- **SOLID**: Plan modules around one reason to change, prefer extension through approved contracts or adapters, preserve substitutable implementations, keep public interfaces narrow, and wire concrete dependencies at the edge.
- **Tradeoffs**: Balance SOLID and DRY against KISS and YAGNI to preserve simplicity. If SRP suggests splitting a God object, plan characterization tests, coverage, or narrower verification first.

## 6. Verification & Quality Strategy

- **Modern Verification Over Legacy TDD**: TDD is recognized as a legacy human methodology that induces reward hacking, tautological validation, and over-mocking in AI agents. Direct all verification efforts toward Spec-Driven contracts, Observability (ODD), and real End-to-End (E2E) execution paths. Retain existing repository test suites strictly as regression baselines by executing them to confirm backward compatibility.
- **Structural Code Discovery**: Leverage structural indexing tools (`ast-index`, `graphify`, `tree-sitter`) for precise structural codebase comprehension, dependency mapping, and relationship exploration prior to executing modifications.
- **Verification Depth**: End-to-End (E2E) verification across complete user and API paths is explicitly encouraged and permitted as the definitive validation standard. Execute genuine CLI commands, API requests, database queries, or browser sessions with reproducible outputs and verifiable evidence.
- **Playwriter & Mandatory Screenshots**: For browser testing, E2E checks, and DOM interaction, use the **Playwriter CLI** (`playwriter`) with the `playwriter` skill whenever installed. Capture and provide screenshots of completed tasks and final interface states as definitive visual evidence upon completing any UI or browser-verifiable work.
- **Web Project Performance & Lighthouse Standards**: When working on web projects, actively utilize Chrome DevTools and run Lighthouse audits, optimizing bundle sizes, assets, rendering performance, and Core Web Vitals to achieve top performance ratings (90+).
- **Frictionless Anti-Bot Form Protection**: When developing web forms, implement modern anti-bot practices that remain invisible to human users (such as honeypot fields, time-gate submission thresholds, cryptographic proof-of-work puzzles, or background behavioral heuristics), sustaining rapid page load speeds and seamless user experience.
- **Testing Doubles & Simulation**: Use stub subprocess runners with simulation CLI scripts and `FakeTransport` protocol doubles for client simulation.
- **Quality Gates & Commands**:
  - *Linter & Formatter*: `uv run ruff check src/` (configured with `B`, `BLE001`, `C4`, `PERF`, `S110`, `SIM`, `UP`, `ASYNC`, `I`, `LOG`).
  - *Regression Testing*: `uv run pytest`.
  - *Dependency Verification*: `uv lock --check`.
  - *Type Checking*: `ty`.
  - *Security & Auditing*: `bandit`, `pip-audit`.
  - *Mutation Testing*: `mutmut`.
- **Pre-Stop / Finalization Checklist**:
  - Verify that all delegated subagents have fully concluded and inspect their actual outputs.
  - Capture and attach screenshots of completed features as visual verification evidence.
  - Run project linters (`uv run ruff check src/`) and existing regression tests (`uv run pytest`) on touched files.
  - Review `git status` and staged diffs to verify change boundaries.
  - Report task completion only after all subagents finish and all blocking items are resolved.
  - Maintain all source files under 1000 lines.

## 7. Subagent Delegation & Orchestration

- **Task Decomposition & Model Routing**: The main orchestrator focuses on task decomposition into small, measurable, short-timed tasks with explicit allowed paths, acceptance criteria, and stop conditions. Spawn subagents by default with clean, fresh context windows and a shared repository handoff file. By default, subagents use model `flash-combined` (preferred for decomposed, short-timed tasks, exploration, and standard code edits) and `heavy-combined` for heavy tasks (complex logic, large multi-file refactoring, deep architectural reasoning) under default conditions.
- **Path-Bounded Scopes**: Assign subagents strict directory and file boundaries. Subagents operate strictly as leaf workers within their assigned scope, omitting recursive worker dispatch.
- **Read-Only Scopes**: Restrict investigation and review subagents strictly to analysis and inspection, dispatching them with read-only allowed tools wherever possible.
- **Subagent Feedback Persistence**: When dispatching a subagent, require difficulty/pitfall feedback and save it in `[[EXPERIENCE.md]]` in the root project folder near `[[AGENTS.md]]`/`[[CLAUDE.md]]`. The orchestrator reports subagent difficulties and pitfalls to the user.
- **Output Verification & Waiting**: Await complete execution of all delegated workers, verify their tangible on-disk outputs and logs, and confirm final status only after all worker processes have concluded.

## 8. Git Commits & Repository Hygiene

- **Small Incremental Commits**: Implement changes in small, incremental steps and create commits after each verified step when authorized.
- **Staging Discipline**: Inspect exact staged paths before committing (`git status` and `git diff --staged`) to confirm only authorized project files are staged.
- **Confidentiality**: Restrict commits to project code, keeping credentials, personal data, and live databases local.
- **Conventional Commits**: Use standard conventional commit types (`fix:`, `feat:`, `chore:`, `docs:`, `refactor:`, `test:`) with the user as the sole contributor.
- **Standard GitHub Development Lifecycle**: The default route for developing features or fixing issues follows a disciplined branch-based lifecycle: issue or user prompt -> create dedicated new branch -> commit verified changes -> open pull request -> complete peer review -> merge into main -> remove feature branch.

## 9. Communication Style Notes

- **Language**: English is the primary language for interactions and documentation.
- **Short Imperative Prompts**: "fix it", "continue", "remove", "approve, implement", "1", "yes", "Done" assume prior turn context.
- **"Next." / "Next task."**: Task separators within a session.
- **Approval Gate Language**: Wait for explicit approval ("approve, implement", "go ahead") before remote infrastructure modifications or external service operations.
- **Direct Feedback Reception**: When receiving forceful feedback, immediately halt the current approach, acknowledge the specific issue, and execute the correction directly.
- **Honesty & Transparency**: State clearly when information is missing or uncertain. Rely on verified facts rather than speculation. Report and correct mistakes immediately.
