from __future__ import annotations

from dataclasses import dataclass

from .config import ProjectsConfig
from .context import RunContext
from .model import EngineId


@dataclass(frozen=True, slots=True)
class ParsedDirectives:
    prompt: str
    engine: EngineId | None
    project: str | None
    branch: str | None
    plan: bool = False
    goal: str | None = None
    skill: str | None = None
    subagent: str | None = None


# Mode tokens reserved ahead of project aliases (e.g. a project named "plan").
_MODE_PLAN = "plan"
_MODE_GOAL = "goal"
_RESERVED_MODE_TOKENS = frozenset({_MODE_PLAN, _MODE_GOAL})


class DirectiveError(RuntimeError):
    pass

def parse_directives(
    text: str,
    *,
    engine_ids: tuple[EngineId, ...],
    projects: ProjectsConfig,
) -> ParsedDirectives:
    if not text:
        return ParsedDirectives(prompt="", engine=None, project=None, branch=None)

    lines = text.splitlines()
    idx = next((i for i, line in enumerate(lines) if line.strip()), None)
    if idx is None:
        return ParsedDirectives(prompt=text, engine=None, project=None, branch=None)

    line = lines[idx].lstrip()
    tokens = line.split()
    if not tokens:
        return ParsedDirectives(prompt=text, engine=None, project=None, branch=None)

    engine_map = {engine.lower(): engine for engine in engine_ids}
    project_map = {alias.lower(): alias for alias in projects.projects}

    engine: EngineId | None = None
    project: str | None = None
    branch: str | None = None
    plan = False
    goal: str | None = None
    skill: str | None = None
    subagent: str | None = None
    consumed = 0

    while consumed < len(tokens):
        token = tokens[consumed]
        # Inline options: --skill <name> / --subagent <name>
        lower = token.lower()
        if lower == "--skill":
            next_pos = consumed + 1
            if next_pos >= len(tokens):
                raise DirectiveError("--skill requires a value")
            if skill is not None:
                raise DirectiveError("multiple --skill directives")
            skill = tokens[next_pos]
            consumed += 2
            continue
        if lower == "--subagent":
            next_pos = consumed + 1
            if next_pos >= len(tokens):
                raise DirectiveError("--subagent requires a value")
            if subagent is not None:
                raise DirectiveError("multiple --subagent directives")
            subagent = tokens[next_pos]
            consumed += 2
            continue
        if token.startswith("/"):
            name = token[1:]
            if "@" in name:
                name = name.split("@", 1)[0]
            if not name:
                break
            key = name.lower()
            # Slash forms: /skill <name> / /subagent <name> (generic one-arg)
            if key == "skill":
                next_pos = consumed + 1
                if next_pos >= len(tokens):
                    raise DirectiveError("/skill requires a value")
                if skill is not None:
                    raise DirectiveError("multiple /skill directives")
                skill = tokens[next_pos]
                consumed += 2
                continue
            if key == "subagent":
                next_pos = consumed + 1
                if next_pos >= len(tokens):
                    raise DirectiveError("/subagent requires a value")
                if subagent is not None:
                    raise DirectiveError("multiple /subagent directives")
                subagent = tokens[next_pos]
                consumed += 2
                continue
            if key == _MODE_PLAN:
                plan = True
                consumed += 1
                continue
            if key == _MODE_GOAL:
                # Remainder of the message is the goal condition.
                rest_on_line = tokens[consumed + 1 :]
                rest_lines = lines[idx + 1 :]
                parts: list[str] = []
                if rest_on_line:
                    parts.append(" ".join(rest_on_line))
                if rest_lines:
                    parts.append("\n".join(rest_lines))
                goal = "\n".join(parts).strip() or None
                # Consume entire message as directives-only (prompt empty).
                return ParsedDirectives(
                    prompt="",
                    engine=engine,
                    project=project,
                    branch=branch,
                    plan=plan and goal is None,  # goal wins over plan
                    goal=goal,
                    skill=skill,
                    subagent=subagent,
                )
            engine_candidate = engine_map.get(key)
            project_candidate = project_map.get(key)
            if engine_candidate is not None:
                if engine is not None:
                    raise DirectiveError("multiple engine directives")
                engine = engine_candidate
                consumed += 1
                continue
            if project_candidate is not None:
                if project is not None:
                    raise DirectiveError("multiple project directives")
                project = project_candidate
                consumed += 1
                continue
            break
        if token.startswith("@"):
            value = token[1:]
            if not value:
                break
            if branch is not None:
                raise DirectiveError("multiple @branch directives")
            branch = value
            consumed += 1
            continue
        break

    if (
        consumed == 0
        and not plan
        and goal is None
        and skill is None
        and subagent is None
    ):
        return ParsedDirectives(
            prompt=text,
            engine=None,
            project=None,
            branch=None,
            plan=False,
            goal=None,
        )
    if consumed < len(tokens):
        remainder = " ".join(tokens[consumed:])
        lines[idx] = remainder
    else:
        lines.pop(idx)
    prompt = "\n".join(lines).strip()
    # Goal already handled via early return; plan alone leaves remaining prompt.
    if goal is not None:
        plan = False
    return ParsedDirectives(
        prompt=prompt,
        engine=engine,
        project=project,
        branch=branch,
        plan=plan,
        goal=goal,
        skill=skill,
        subagent=subagent,
    )


def parse_context_line(
    text: str | None, *, projects: ProjectsConfig
) -> RunContext | None:
    if not text:
        return None
    ctx: RunContext | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("`") and stripped.endswith("`") and len(stripped) > 1:
            stripped = stripped[1:-1].strip()
        elif stripped.startswith("`"):
            stripped = stripped[1:].strip()
        elif stripped.endswith("`"):
            stripped = stripped[:-1].strip()
        # Strip 🏷 emoji prefix from combined info line
        if stripped.startswith("\N{LABEL} "):
            stripped = stripped[2:].strip()
        if not (
            stripped.lower().startswith("ctx:") or stripped.lower().startswith("dir:")
        ):
            continue
        content = stripped.split(":", 1)[1].strip()
        if not content:
            continue
        tokens = content.split()
        if not tokens:
            continue
        project = tokens[0]
        branch = None
        if len(tokens) >= 2:
            if tokens[1] == "@" and len(tokens) >= 3:
                branch = tokens[2]
            elif tokens[1].startswith("@"):
                branch = tokens[1][1:]
        project_key = project.lower()
        if project_key not in projects.projects:
            raise DirectiveError(
                f"unknown project {project!r} in ctx line; start a new thread or "
                "add it back to your config"
            )
        ctx = RunContext(project=project_key, branch=branch)
    return ctx


def format_context_line(
    context: RunContext | None, *, projects: ProjectsConfig
) -> str | None:
    if context is None or context.project is None:
        return None
    project_cfg = projects.projects.get(context.project)
    alias = project_cfg.alias if project_cfg is not None else context.project
    if context.branch:
        return f"dir: {alias} @{context.branch}"
    return f"dir: {alias}"

def format_mode_badge(*, plan: bool, goal: str | None) -> str | None:
    """Return the plan/goal footer badge, or None when no mode is active.

    Goal wins over plan when both are set (matches ``run_modes`` precedence).
    The badge is rendered as inline code so it sits next to the ``ctx:`` line.
    """
    if goal:
        return "`goal`"
    if plan:
        return "`plan`"
    return None


def compose_context_line(
    context: RunContext | None,
    projects: ProjectsConfig,
    *,
    plan: bool = False,
    goal: str | None = None,
) -> str | None:
    """Build the footer context line with an optional plan/goal mode badge.

    The badge (when present) precedes the ``ctx: <project>`` segment,
    space-separated on the same footer line. Returns None when neither a badge
    nor a context line applies.
    """
    badge = format_mode_badge(plan=plan, goal=goal)
    ctx = format_context_line(context, projects=projects)
    if badge is not None and ctx is not None:
        return f"{badge} {ctx}"
    return badge or ctx
