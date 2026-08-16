# Untether ACP Launcher Discovery Closure Plan

## Goal

Make locally installed official-registry ACP agents appear as runtime engines and Telegram slash commands without executing package managers, guessing an executable from a package name, or weakening existing session, compact/handoff, and prompt-batch coverage.

## Completed baseline

`fix/session-compact-batch` has been merged into `master` as `c300785`. It preserves full session identifiers and adds loop-level compact/handoff and prompt-batch regressions. The ACP runner guide changes were committed on that branch before the merge. Those completed changes are retained and are not reimplemented here.

## Root cause

The ACP registry parser already accepts the official singular `distribution` object, but it derives a launcher directly from `distribution.npx.package` when package metadata is unavailable. That heuristic can bind an unrelated executable of the same name on `PATH`. It also makes the documentation overclaim `uvx` and scoped-package auto-discovery.

## Design

1. Parse legacy `distributions` and official singular `distribution` in the existing ACP registry parser; do not add a second parser.
2. Keep exact-platform `binary` distributions preferred.
3. Represent `npx` and `uvx` package distributions without a guessed command. During passive discovery, derive an npm launcher only from that installed package's local `package.json` `bin` metadata beside the local `npx` executable. Then resolve exactly that declared launcher through `PATH`.
4. If metadata is absent, malformed, or ambiguous, omit the registry agent. Never infer the package name as a launcher, invoke `npx`, `uvx`, npm, a shell, a downloader, or an installer.
5. `uvx` entries remain configuration-only because a passive, deterministic local metadata location is not available. Scoped npm packages are permitted only when their installed local `bin` metadata supplies exactly one launcher.
6. The Telegram command menu remains unchanged unless its existing `seen` set fails a regression: it already lowercases and deduplicates `runtime.available_engine_ids()`.

## TDD sequence

1. Add and run a RED regression in `tests/test_acp_registry.py` for the official Cline singular shape. Arrange an installed `node_modules/cline/package.json` with one `bin` launcher and a PATH-resolvable `cline.cmd`; assert discovery records the resolved executable.
2. Add and run a RED regression with the same registry entry but no installed package metadata. Even if `cline.cmd` is on `PATH`, assert the record is unavailable and has an empty command.
3. Make the minimum parser change: package distributions carry no package-name-derived command; discovery calls the existing metadata reader for `npx` only.
4. Add or retain runtime-loader proof that a metadata-resolved Cline record reaches `RuntimeSpec.dynamic_engine_ids` and router availability.
5. Run the existing Telegram command-menu regression. Assert one lower-cased command for duplicate runtime engine IDs; do not alter the menu if it remains green.

## Documentation

Update `docs/reference/config.md` and `docs/reference/runners/acp.md` to say:

- current-platform binaries and metadata-resolved installed `npx` packages can be autodiscovered;
- `uvx`, missing metadata, and ambiguous packages need explicit absolute-path configuration;
- startup performs only cache reads, local package-metadata reads, and `PATH` resolution.

## Verification

Run from `D:/Projects/untether`:

```text
uv run pytest tests/test_acp_registry.py tests/test_runtime_loader.py tests/test_telegram_bridge.py -q --no-cov
uv run pytest tests/test_runner_utils.py tests/test_pi_runner.py tests/test_omp_runner.py tests/test_meta_line.py tests/test_rendering.py tests/test_telegram_compact_dispatch.py tests/test_compact.py tests/test_telegram_prompt_batch.py tests/test_telegram_prompt_batch_integration.py tests/test_scheduler_queue.py -q --no-cov
uv run ruff format --check src tests
uv run ruff check src tests
uv run ty check src tests
uv run pytest
uv lock --check
```

For startup evidence, start only one authorised source poller, confirm sanitized `setup.summary` and `startup.command_menu.updated` list `cline`, then stop it cleanly. If interactive Telegram keyboard input is unavailable, retain live message scenarios as blocked; do not create another poller or claim the scenarios passed.

## Constraints

- Use `uv run ...` only.
- Preserve complete native session IDs everywhere persisted or copyable.
- Keep all Telegram writes routed through `TelegramOutbox`.
- Never reveal tokens, private identifiers, prompts, replies, state files, or endpoints.
- Do not commit or push without an explicit request.
