# Generic ACP runner

Untether exposes ACP agents through the ordinary engine backend and Telegram
routing paths. It does not add an ACP-specific execution branch.

## Protocol versions

ACP v1 is the stable protocol. Draft ACP v2 is supported but remains explicitly
marked draft in configuration and diagnostics. `protocol = "1"` or `protocol =
"2"` pins an engine to that version. `protocol = "auto"` starts with a v2-shaped
`initialize`; it accepts a selected v2 version, or selected v1 when
`[acp].allow_v1 = true`. A v1 fallback is a single fresh connection after the
first connection rejects or closes before selecting a version. Other
initialization failures do not trigger fallback.

The adapter selects lifecycle payloads and completion rules from the negotiated
version and capabilities. It does not infer behaviour from an agent or product
name. Unknown extensible v2 variants are retained safely and rendered as a
neutral note where user-visible text is safe.

## Registry discovery and caches

Official-registry discovery is enabled by default. The fixed registry endpoint
is `https://cdn.agentclientprotocol.com/registry/v1/latest/registry.json`.
Untether considers a current-platform `binary` distribution and locally
installed launchers for official `npx` and `uvx` package distributions.

Two cache files live under `~/.untether/cache/`:

- `acp-registry-v1.json` stores the last valid registry document.
- `acp-install-state-v1.json` stores each agent's installed/not-installed
  result, registry identity, platform target, command, and resolved executable.

Both use `[acp.registry].cache_ttl_days`, a shared positive integer defaulting to
`3`. Fresh caches avoid network access and `PATH` scanning. Expired or missing
installation records are rescanned; a stale positive executable is checked again
before routing. Registry fetch or validation failure falls back to a valid stale
registry cache, otherwise dynamic engines are omitted without removing static or
explicit engines. Configuration reload reuses unexpired cache entries.

Discovery never downloads or installs packages, runs a registry command, hashes
an unrelated executable, or constructs a shell command. It resolves launchers
only from `PATH`; for npm packages whose package name differs from the launcher,
it reads the installed package's local `bin` metadata. This passive check also
supports scoped npm packages. Unavailable or ambiguous launchers remain omitted
until an explicit engine configuration supplies an absolute command.

Registry IDs are normalized by replacing `-` with `_`; for example,
`amp-acp` becomes engine and command ID `amp_acp`. Invalid, overlong, reserved,
or colliding IDs are skipped. Explicit configuration wins over a registry entry
with the same normalized ID. A backend is exposed only after its executable is
locally resolvable and the runner can be constructed. Protocol compatibility is
negotiated on first invocation, not during startup.

## Configuration

```toml
[acp]
allow_v1 = true

[acp.registry]
enabled = true
cache_ttl_days = 3

[acp.engines.local_agent]
command = "D:/Tools/local-agent.exe"
args = ["--acp"]
protocol = "auto"
env = { NO_COLOR = "1" }
startup_timeout_s = 30.0
request_timeout_s = 60.0
mcp_servers = [{ name = "tools", command = "mcp-server", args = ["--serve"] }]

[acp.engines.local_agent.client]
filesystem = true
terminal = true
elicitation_form = true
elicitation_url = false
interaction_timeout_s = 600.0

Explicit engine IDs must match `[a-z0-9_]{1,32}`. `command` must be an
absolute path to an existing executable; relative and bare commands are
rejected. Arguments are passed as an argv list and `env` is an unlogged static
overlay. Explicit engines may be used when registry discovery is disabled.

`startup_timeout_s` bounds both `initialize` requests and negotiation;
`request_timeout_s` bounds the v2 prompt acknowledgement. `mcp_servers` entries
pass through verbatim as `session/new` `mcpServers`; only `name` is required,
and unknown keys (for example `url`-based transports) are preserved for the
agent. The `[acp.engines.<id>.client]` table configures the client facilities
Untether advertises: `filesystem`, `terminal`, `elicitation_form`,
`elicitation_url` toggles and the shared `interaction_timeout_s` for permission
and elicitation waits.

## Sessions, updates, and safety

New and resumed sessions use `session/new` or the negotiated resume method.
Resume tokens are engine-specific; a token from another engine fails before
spawn. Model, reasoning, permission, and plan overrides are applied only when a
compatible advertised session option can be validated and set. An explicit
unsupported override fails before prompting rather than being silently ignored.

The runner preserves ordered updates and stable action IDs for messages, tools,
plans, and terminals. It emits Untether's normal
`StartedEvent -> ActionEvent* -> CompletedEvent` contract. Cancellation uses
`session/cancel` and waits for protocol completion or bounded teardown. Prompt,
request, cancellation, frame, queue, reducer, output, and registry-download
sizes/timeouts are bounded. Subprocesses use argv only and managed process-tree
cleanup; raw stderr, environment values, tokens, and unsanitized paths are not
sent to Telegram.

## Capabilities and authentication

Optional behavior is capability-gated. Authentication uses only configured or
unambiguously advertised protocol-driven methods. `auth_required` may trigger
one authentication-and-retry of `session/new` or resume; a second request fails
with an actionable phase and eligible method IDs. Untether never copies secrets
to chat or logs and does not log out persistent agent credentials during normal
teardown.

ACP v1 Client filesystem, terminal, and elicitation adapters are disabled by
default. If enabled, filesystem access is restricted to resolved session roots
and writes are atomic; terminal execution accepts argv arrays, restricts cwd to
those roots, uses managed subprocesses, and bounds retained output; URL
elicitation validates payloads and never opens a URL automatically. ACP v2 does
not advertise the removed Client filesystem or terminal methods. Agent-owned v2
display terminals remain supported as updates. Reverse requests are authorized
through the existing sender/owner checks, and unknown reverse methods return
JSON-RPC `-32601` without crashing the peer.

Aggregate reducer limits (`max_messages`, `max_actions`, `max_unknown_updates`)
fail the run when exceeded. Per-item text trims (`max_answer`,
`max_message_content`, `max_output`) are bounded projections of a single
logical item and do not fail the run — a documented deviation. Batches are
accepted only under negotiated v2; an oversized frame fails the run rather than
being misread as EOF. `$/cancel_request` cancels the matching reverse request
and answers `-32800`.

## Telegram commands

Each installed registry agent receives one ordinary normalized slash command.
For example:

```text
/amp_acp fix tests
```

This is the same directive, queue, router, options, progress, and result path as
any other engine override. The Telegram command menu remains subject to its
**no `/acp` command** and no aggregate ACP list or dispatch path.

Agent-initiated permission and elicitation requests surface as ordinary
progress-card buttons. Selecting one resolves through the internal
`acp_control` callback backend, which maps the pressed option back to the
pending request's nonce and answers the agent on the wire (`selected` with the
option ID, or `cancelled`). Stale buttons from finished or timed-out turns are
ignored benignly.

## Opt-in live probes

Live probes are optional and must point at an explicitly configured, locally
installed ACP executable. Do not make CI or normal startup depend on third-party
credentials or network access. Configure a temporary `[acp.engines.<id>]` entry
with an absolute command, then run the focused ACP tests and the project test
suite. For a real-agent probe, run the normal Telegram/dev instance path with
that engine ID and record the negotiated protocol version plus initialize,
new/resume, prompt/update, cancellation, and teardown results. Remove the
temporary credentials and engine entry afterward; redact tokens, prompts,
paths, and agent output from diagnostics.
