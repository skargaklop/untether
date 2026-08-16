# Generic ACP Installation Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` or `executing-plans` task-by-task. Strict TDD: each behavioural change starts with an observed failing test. Do not modify files outside `D:/Projects/untether`. Do not run installers, package downloads, package-manager commands, or unknown executables during discovery.

**Goal:** Automatically expose every locally installed, officially registered ACP agent that can be identified from trusted package metadata, regardless of whether it was installed through npm, Bun, uv, pipx, a platform binary, or Cargo; expose the resulting engines through the normal runtime and Telegram slash-command paths without agent-specific rules.

**Architecture:** The official ACP registry remains the authority for ACP capability, engine ID, package identity, launch arguments, and environment. A new installation-inventory module passively reads local package-manager receipts/manifests and returns verified launcher candidates through one small interface. Registry matching joins registry package/distribution identity to those candidates; it never guesses from an arbitrary PATH executable and never executes installers or agents. Existing `EngineBackend`, `RuntimeSpec.dynamic_engine_ids`, router, and Telegram command-menu paths remain unchanged.

**Tech Stack:** Python 3.10+, Pydantic, pathlib, stdlib JSON/TOML metadata readers, pytest, Ruff, Ty, uv.

---

## Scope and safety contract

Supported automatic evidence, in priority order:

1. Current-platform official `binary` distribution: resolve the registry-declared command basename on `PATH`.
2. Official `npx` distribution installed by npm or Bun: match the exact registry package name against an installed `package.json`, then use only its declared `bin` entry.
3. Official `uvx` distribution installed by uv or pipx: match the normalized registry package name against a tool receipt or installed Python distribution metadata, then use only a declared `console_scripts` entry.
4. Cargo: accept a launcher only when an official current-platform binary distribution declares the same command and Cargo's `.crates2.json` records that exact installed binary. Cargo is installation evidence, not ACP capability evidence.
5. Agents absent from the official registry remain explicit configuration only. Untether cannot safely infer ACP support, required arguments, or stable engine IDs from arbitrary executables. No PATH-wide guessing.

The implementation MUST NOT:

- contain Cline-, Minion-, or other agent-name conditionals;
- invoke `npm`, `npx`, `bun`, `uv`, `uvx`, `pipx`, `cargo`, an agent launcher, a shell, or a downloader during startup discovery;
- select one launcher from ambiguous package metadata;
- trust a cached executable after its metadata source or file disappears;
- add a second runtime/menu path for ACP engines.

Environment-standard roots are used instead of new configuration knobs: `APPDATA`/npm layout, `BUN_INSTALL` or `~/.bun`, `UV_TOOL_DIR` or uv's platform default, `PIPX_HOME` or pipx's platform default, and `CARGO_HOME` or `~/.cargo`. Missing roots are skipped.

## File map

- Create `src/untether/acp_installations.py`: package-spec normalization, immutable installation candidates, passive npm/Bun/uv/pipx/Cargo inventory readers, deterministic candidate matching.
- Modify `src/untether/acp_registry.py`: keep registry parsing and backend construction; delegate installation discovery to the inventory module; remove npm-only private lookup.
- Modify `src/untether/runtime_loader.py`: build one inventory per startup, use source-aware cache keys, preserve existing backend/dynamic-ID flow, emit bounded omission diagnostics.
- Create `tests/test_acp_installations.py`: pure filesystem fixtures for every supported inventory and ambiguity/safety case.
- Modify `tests/test_acp_registry.py`: registry-to-inventory matching and cache revalidation contracts.
- Modify `tests/test_runtime_loader.py`: end-to-end RuntimeSpec proofs for multiple arbitrary registry agents and negative-cache invalidation.
- Modify the existing Telegram command-menu test file located by the current `dynamic_engine_ids`/command-menu tests: prove all discovered IDs appear once; do not add menu production code unless the regression fails.
- Modify `docs/reference/config.md` and `docs/reference/runners/acp.md`: supported installers, evidence rules, cache semantics, omissions, and explicit fallback.
- Append execution difficulties and pitfalls to `EXPERIENCE.md`.
- Copy this approved plan to `docs/plans/untether-generic-acp-installation-discovery-plan.md` before implementation, as requested.

---

### Task 1: Consolidate repository state before feature work

**Files:** No source edits.

- [ ] Enumerate all Untether worktrees and local branches. Confirm no worktree has uncommitted user changes and no unmerged commit is omitted.
- [ ] Merge each completed feature branch into canonical `master` in dependency order. Resolve conflicts by preserving the already verified session-ID, compact/handoff, prompt-batch, and ACP cache fixes.
- [ ] Run `uv run pytest -q --no-cov` on the consolidated tree before deleting any merged worktree.
- [ ] Commit the consolidated state with an allowed conventional type, without co-author metadata. Do not push unless the user explicitly requests it.
- [ ] Remove only worktrees whose commits are reachable from `master`; retain any worktree with unique or dirty state.
- [ ] Copy this approved plan to `docs/plans/untether-generic-acp-installation-discovery-plan.md` and commit it separately as `docs: add generic ACP discovery plan`.

Expected result: one canonical clean implementation base containing all prior verified work and the approved plan.

### Task 2: Define the deep installation-inventory interface

**Files:**
- Create: `src/untether/acp_installations.py`
- Test: `tests/test_acp_installations.py`

- [ ] Write failing tests for these value objects and pure functions:

```python
InstalledLauncher(
    ecosystem="npm",
    package="cline",
    version="3.0.55",
    command="C:/Users/me/AppData/Roaming/npm/cline.cmd",
    metadata_path="C:/Users/me/AppData/Roaming/npm/node_modules/cline/package.json",
)

parse_registry_package("cline@3.0.55", ecosystem="npm") == ("cline", "3.0.55")
parse_registry_package("@scope/agent@1.2.0", ecosystem="npm") == ("@scope/agent", "1.2.0")
parse_registry_package("minion-code@0.1.44", ecosystem="python") == ("minion-code", "0.1.44")
```

- [ ] Verify RED with:

```text
uv run pytest tests/test_acp_installations.py -q --no-cov
```

Expected: collection/import failure because the module and symbols do not exist.

- [ ] Implement only:

```python
@dataclass(frozen=True, slots=True)
class InstalledLauncher:
    ecosystem: Literal["npm", "bun", "uv", "pipx", "cargo", "binary"]
    package: str
    version: str | None
    command: str
    metadata_path: str


def discover_installed_launchers(*, env: Mapping[str, str], home: Path) -> tuple[InstalledLauncher, ...]: ...


def match_distribution(
    distribution: RegistryDistribution,
    installed: Sequence[InstalledLauncher],
) -> InstalledLauncher | None: ...
```

Keep root enumeration and ecosystem readers private. Normalize Python names according to PEP 503; preserve npm scope. Reject malformed specs instead of coercing them.

- [ ] Run the focused test and Ty on the new module. Commit `feat: add ACP installation inventory interface`.

### Task 3: Add passive npm and Bun inventory readers

**Files:**
- Modify: `src/untether/acp_installations.py`
- Test: `tests/test_acp_installations.py`

- [ ] Add failing filesystem tests covering:
  - npm global `node_modules/<package>/package.json` with string `bin`;
  - npm scoped package with exactly one mapping in `bin`;
  - Bun global manifest and installed package directory;
  - package version mismatch retained as evidence but not matched to a pinned registry spec;
  - zero, multiple, empty, escaping, or non-string `bin` entries rejected;
  - launcher absent from the package-manager bin directory rejected;
  - a same-named PATH executable without package metadata rejected.
- [ ] Verify RED.
- [ ] Implement metadata readers. Resolve launcher paths from the package manager's own bin directory and platform suffixes (`.cmd`, `.exe`, bare file); require a real file. Never call `shutil.which` to substitute unrelated PATH evidence for package identity.
- [ ] Run focused tests and commit `feat: discover npm and Bun ACP installations`.

### Task 4: Add passive uv and pipx inventory readers

**Files:**
- Modify: `src/untether/acp_installations.py`
- Test: `tests/test_acp_installations.py`

- [ ] Add failing fixtures for:
  - uv `uv-receipt.toml`, its tool environment, installed `.dist-info/METADATA`, and `entry_points.txt` `console_scripts`;
  - pipx `pipx_metadata.json`, venv distribution metadata, and exposed launcher;
  - normalized Python names (`minion_code`, `minion-code`, `Minion.Code`) matching the same package;
  - pinned registry version mismatch;
  - multiple console scripts treated as ambiguous unless exactly one script basename matches the registry agent ID or declared package command;
  - missing receipt, metadata, entry point, or executable omitted safely.
- [ ] Verify RED.
- [ ] Implement with `tomllib` and JSON only. Read receipts and installed distribution metadata; do not import installed packages or run Python entry points.
- [ ] Run focused tests and commit `feat: discover uv and pipx ACP installations`.

### Task 5: Add Cargo and platform-binary evidence

**Files:**
- Modify: `src/untether/acp_installations.py`
- Test: `tests/test_acp_installations.py`

- [ ] Add failing fixtures for Cargo `.crates2.json` and the corresponding `$CARGO_HOME/bin` executable.
- [ ] Prove a Cargo binary is accepted only when a current-platform official binary distribution declares the same command basename; a random installed crate never becomes an engine.
- [ ] Preserve direct current-platform binary discovery through the registry-declared command, including Windows suffix resolution.
- [ ] Verify RED, implement the narrow reader/matcher, run focused tests, and commit `feat: recognise Cargo and binary ACP launchers`.

### Task 6: Join registry distributions to installation inventory

**Files:**
- Modify: `src/untether/acp_registry.py`
- Modify: `tests/test_acp_registry.py`

- [ ] Replace the existing npm-only tests with table-driven failing tests using arbitrary IDs/packages across binary, npm/Bun, and uv/pipx evidence. Keep official singular `distribution` parsing and legacy `distributions` compatibility tests.
- [ ] Add safety regressions: exact pinned package match, scoped npm match, Python normalized-name match, ambiguous launchers omitted, and no metadata fallback to package/agent basename.
- [ ] Verify RED against current `_npm_bin`/`_distribution_command` behavior.
- [ ] Change `discover_installation` to accept the prebuilt inventory and call `match_distribution`; remove `_npm_bin` after all callers/tests migrate. Keep `choose_binary_distribution` as distribution precedence selection, renaming it with LSP only if its interface now covers all distribution types.
- [ ] Ensure `registry_backend` receives the verified absolute launcher plus registry args/env unchanged.
- [ ] Run `uv run pytest tests/test_acp_installations.py tests/test_acp_registry.py -q --no-cov` and commit `refactor: use package inventory for ACP discovery`.

### Task 7: Make cache identity source-aware and self-invalidating

**Files:**
- Modify: `src/untether/runtime_loader.py`
- Modify: `tests/test_runtime_loader.py`

- [ ] Add failing tests proving cache identity includes registry agent/version/target, distribution type/package, ecosystem, metadata path, and resolved command.
- [ ] Add regressions proving a fresh negative cache is ignored when a new matching receipt appears, and a fresh positive cache is ignored when its metadata or executable disappears.
- [ ] Verify RED.
- [ ] Build the inventory once per runtime construction, outside the agent loop. Match each registry distribution against it. Store only JSON-safe source identity and executable data. Do not rescan per agent.
- [ ] Keep TTL for unchanged evidence; invalidate by source fingerprint when evidence changes. Bound diagnostic fields to agent ID, distribution type, and reason; never log home paths or raw metadata.
- [ ] Run focused tests and commit `fix: invalidate ACP discovery cache on installation changes`.

### Task 8: Prove arbitrary engines reach RuntimeSpec and Telegram commands

**Files:**
- Modify: `tests/test_runtime_loader.py`
- Modify: existing Telegram command-menu test file found from current menu tests
- Production menu files: unchanged unless the regression demonstrates a real defect

- [ ] Add an integration fixture with at least four neutral registry agents: one npm-installed, one Bun-installed, one uv/pipx-installed, and one platform/Cargo binary. Do not use Cline as the sole proof.
- [ ] Assert all normalized IDs join `RuntimeSpec.dynamic_engine_ids`, `runtime.engine_ids`, router backends, startup `setup.summary.found`, and the Telegram slash-command set exactly once.
- [ ] Assert unavailable, ambiguous, colliding, reserved, and explicit-override entries retain existing behavior.
- [ ] Verify RED before runtime integration changes, implement the minimal wiring, then run:

```text
uv run pytest tests/test_acp_installations.py tests/test_acp_registry.py tests/test_runtime_loader.py <telegram-menu-test-file> -q --no-cov
```

- [ ] Commit `feat: expose discovered ACP engines in runtime commands`.

### Task 9: Document the generic contract

**Files:**
- Modify: `docs/reference/config.md`
- Modify: `docs/reference/runners/acp.md`
- Modify: `EXPERIENCE.md`

- [ ] Document the supported registry distribution/installer matrix, passive metadata roots and environment overrides, ambiguity behavior, cache invalidation, diagnostics, and explicit configuration fallback.
- [ ] State precisely that official registry membership supplies ACP semantics; package-manager metadata supplies installation evidence. An unregistered executable cannot be safely autodetected.
- [ ] Remove obsolete claims that `uvx` is always explicit-only or that only npm metadata is supported.
- [ ] Record all subagent difficulties/pitfalls, including the three read-only scouts that exited without yielding, and how evidence was recovered inline.
- [ ] Run a docs reference search for contradictory `npx`, `uvx`, registry, and cache claims. Commit `docs: describe generic ACP installation discovery`.

### Task 10: Verification and actual startup smoke

**Files:** No planned source edits; any failure returns to the owning task with a new failing regression.

- [ ] Run formatting and static checks:

```text
uv run ruff format --check src tests
uv run ruff check src tests
uv run ty check src tests
uv lock --check
```

- [ ] Run the full suite:

```text
uv run pytest
```

- [ ] Rebuild/install Untether from canonical `master` using the repository's established uv command; verify installed source is the committed source, not stale site-packages.
- [ ] With no other Telegram poller running, start one authorised source poller and capture startup diagnostics. Confirm every locally installed registry-backed ACP engine appears in `setup.summary.found` and the Telegram command-menu update. Stop it cleanly.
- [ ] Exercise one harmless slash-command routing path for a discovered engine if the authorised Telegram input surface accepts input. If desktop input remains unavailable, report that gate as blocked rather than fabricating interaction evidence; startup/runtime/menu diagnostics remain required.
- [ ] Inspect final branch/worktree state, confirm all subagents have terminated, and commit any verification-driven fixes. Do not push without explicit instruction.

## Acceptance criteria

- No agent-specific detection rules.
- Every locally installed official-registry agent with deterministic supported metadata becomes a dynamic engine automatically.
- npm and Bun satisfy official `npx` distributions; uv and pipx satisfy official `uvx` distributions; Cargo/platform executables require an official current-platform binary declaration.
- Package identity, version, launcher declaration, and executable existence are all verified without executing third-party code.
- Ambiguous or unregistered candidates are omitted with safe diagnostics and an explicit-config path.
- Cache entries invalidate when installation evidence changes.
- Discovered IDs flow through existing RuntimeSpec/router/startup/menu paths exactly once.
- Focused tests, Ruff, Ty, lock check, full pytest, installed-runtime startup, and command-menu smoke all pass or any externally blocked live interaction is reported exactly.
