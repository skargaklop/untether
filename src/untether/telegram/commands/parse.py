from __future__ import annotations

from collections.abc import Container
from dataclasses import dataclass

from ...compact import normalize_instructions
from ...model import EngineId


def is_cancel_command(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    command = stripped.split(maxsplit=1)[0]
    return command == "/cancel" or command.startswith("/cancel@")


def _parse_slash_command(text: str) -> tuple[str | None, str]:
    stripped = text.lstrip()
    if not stripped.startswith("/"):
        return None, text
    lines = stripped.splitlines()
    if not lines:
        return None, text
    first_line = lines[0]
    token, _, rest = first_line.partition(" ")
    command = token[1:]
    if not command:
        return None, text
    if "@" in command:
        command = command.split("@", 1)[0]
    args_text = rest
    if len(lines) > 1:
        tail = "\n".join(lines[1:])
        args_text = f"{args_text}\n{tail}" if args_text else tail
    return command.lower(), args_text


# #523: `.new` and similar leading-dot typos used to dispatch a full agent
# subprocess (full OAuth handshake, preamble, MCP catalog probe) before the
# user could cancel — wasting a non-trivial per-run cold-start cost. `.` and
# `/` are adjacent on iOS/Android punctuation rows, and several keyboards
# auto-replace a leading `/` with `.`.
def parse_dot_typo(text: str, known_commands: Container[str]) -> str | None:
    """Return the registered command name if ``text`` looks like a typo of
    ``/<cmd>`` (i.e. begins with ``.<cmd>`` with no whitespace before the
    command and ``<cmd>`` is a known slash command). Else ``None``.

    Only fires on simple shapes ``.cmd`` or ``.cmd args``. Multi-line and
    sentence-shaped inputs are left alone (they'd usually be real prose
    that happens to start with a dot, e.g. ``..wait, what?``).
    """
    if not text:
        return None
    stripped = text.lstrip()
    if not stripped.startswith("."):
        return None
    if stripped.startswith(("..", "./")):
        # Multi-dot ellipsis or a literal path; not a command typo.
        return None
    first_line = stripped.splitlines()[0]
    token, _, _ = first_line.partition(" ")
    cmd = token[1:].lower()
    if not cmd or not cmd.isidentifier():
        return None
    if cmd in known_commands:
        return cmd
    return None


@dataclass(frozen=True, slots=True)
class CompactInvocation:
    """Parsed result of a /compact (or /handoff) invocation.

    Attributes:
        engine: source engine selector (explicit ``/engine`` before the flag).
        instructions: free-form instruction text after flags/selectors.
        destination_engine: target engine for a cross-engine handoff
            (``to <engine>`` clause). None means same-engine (backward compat).
    """

    engine: EngineId | None = None
    instructions: str | None = None
    destination_engine: EngineId | None = None


def parse_command_invocation(
    text: str,
    *,
    flag: str,
    engine_ids: tuple[EngineId, ...],
) -> CompactInvocation | None:
    """Detect a slash command (``/compact`` or ``/handoff``) in any leading position.

    Scans leading slash tokens. Recognizes exactly one ``flag`` token and at
    most one engine selector, in any order. A second engine selector raises
    ``ValueError``. First non-slash or unknown slash token stops scanning;
    the remainder is the instructions.

    After the flag/source tokens, an optional ``to <engine>`` clause may
    appear: the bare word ``to`` followed by a known engine id (leading
    ``/`` tolerated: ``to /grok``). Both tokens are consumed ONLY when the
    id matches a known engine. At most one ``to`` clause is consumed.

    Returns ``None`` when no ``flag`` token is found among the leading tokens.
    """
    stripped = text.lstrip()
    if not stripped.startswith("/"):
        return None

    lines = stripped.splitlines()
    if not lines:
        return None
    first_line = lines[0]
    tokens = first_line.split()
    if not tokens:
        return None

    engine_map = {eid.lower(): eid for eid in engine_ids}

    engine: EngineId | None = None
    found_flag = False
    consumed = 0

    for token in tokens:
        if not token.startswith("/"):
            break
        name = token[1:]
        if "@" in name:
            name = name.split("@", 1)[0]
        if not name:
            break
        key = name.lower()

        if key == flag:
            found_flag = True
            consumed += 1
            continue

        engine_candidate = engine_map.get(key)
        if engine_candidate is not None:
            if engine is not None:
                raise ValueError(f"multiple engine selectors in /{flag}")
            engine = engine_candidate
            consumed += 1
            continue

        break

    if not found_flag:
        return None

    destination_engine: EngineId | None = None
    remaining = tokens[consumed:]
    if len(remaining) >= 2 and remaining[0].lower() == "to":
        dest_token = remaining[1]
        dest_name = dest_token.lstrip("/")
        if "@" in dest_name:
            dest_name = dest_name.split("@", 1)[0]
        dest_key = dest_name.lower()
        dest_candidate = engine_map.get(dest_key)
        if dest_candidate is not None:
            destination_engine = dest_candidate
            consumed += 2

    remaining_on_line = tokens[consumed:]
    tail_lines = lines[1:]
    parts: list[str] = []
    if remaining_on_line:
        parts.append(" ".join(remaining_on_line))
    if tail_lines:
        parts.append("\n".join(tail_lines))
    raw_instructions = " ".join(parts).strip() if parts else ""
    instructions = normalize_instructions(raw_instructions)

    return CompactInvocation(
        engine=engine,
        instructions=instructions,
        destination_engine=destination_engine,
    )


def parse_compact_invocation(
    text: str,
    *,
    engine_ids: tuple[EngineId, ...],
) -> CompactInvocation | None:
    """Detect a ``/compact`` command."""
    return parse_command_invocation(text, flag="compact", engine_ids=engine_ids)


def parse_handoff_invocation(
    text: str,
    *,
    engine_ids: tuple[EngineId, ...],
) -> CompactInvocation | None:
    """Detect a ``/handoff`` command."""
    return parse_command_invocation(text, flag="handoff", engine_ids=engine_ids)
