"""Shared plan/goal mode helpers for engine runners."""

from __future__ import annotations

from .run_options import EngineRunOptions, get_run_options

SOFT_PLAN_PREFIX = (
    "[Untether plan mode] Work in read-only planning mode. Explore and analyze freely, "
    "then produce a structured implementation plan. Do not edit files or run "
    "destructive commands."
)


def run_modes(
    options: EngineRunOptions | None = None,
) -> tuple[bool, str | None]:
    """Return (plan, goal). Goal wins over plan when both are set."""
    opts = options if options is not None else get_run_options()
    if opts is None:
        return False, None
    goal = opts.goal.strip() if opts.goal else None
    if goal:
        return False, goal
    return bool(opts.plan), None


def apply_goal_prompt(prompt: str, goal: str) -> str:
    stripped = prompt.lstrip()
    if stripped.startswith("/goal"):
        return prompt
    condition = goal.strip()
    if not condition:
        return prompt
    return f"/goal {condition}"


def apply_soft_plan_prompt(prompt: str) -> str:
    body = prompt.strip()
    if not body:
        return SOFT_PLAN_PREFIX
    if body.startswith(SOFT_PLAN_PREFIX):
        return prompt
    return f"{SOFT_PLAN_PREFIX}\n\n{body}"


def effective_prompt(
    prompt: str,
    *,
    soft_plan: bool = False,
    options: EngineRunOptions | None = None,
) -> str:
    plan, goal = run_modes(options)
    if goal is not None:
        return apply_goal_prompt(prompt, goal)
    if plan and soft_plan:
        return apply_soft_plan_prompt(prompt)
    return prompt
