from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PromptAttachment:
    """Project-relative media for agents (Layer A path + Layer B CLI flags)."""

    rel_path: str
    abs_path: str
    mime_type: str | None = None
    kind: str = "image"  # image | file


@dataclass(frozen=True, slots=True)
class EngineRunOptions:
    model: str | None = None
    reasoning: str | None = None
    permission_mode: str | None = None
    ask_questions: bool | None = None
    diff_preview: bool | None = None
    show_api_cost: bool | None = None
    show_subscription_usage: bool | None = None
    show_resume_line: bool | None = None
    budget_enabled: bool | None = None
    budget_auto_cancel: bool | None = None
    # #289 — per-chat /loop and ScheduleWakeup observation toggle.  ``None``
    # means "follow global ``[loop] enabled``"; True/False is an explicit
    # per-chat override set via ``/config → 🔁 Loop mode``.
    loop_enabled: bool | None = None
    attachments: tuple[PromptAttachment, ...] = ()
    plan: bool = False
    goal: str | None = None
    skill: str | None = None
    subagent: str | None = None


def merge_run_options(
    base: EngineRunOptions | None,
    *,
    attachments: Sequence[PromptAttachment] | None = None,
    plan: bool | None = None,
    goal: str | None = None,
    model: str | None = None,
    reasoning: str | None = None,
    skill: str | None = None,
    subagent: str | None = None,
    permission_mode: str | None = None,
    ask_questions: bool | None = None,
    diff_preview: bool | None = None,
    show_api_cost: bool | None = None,
    show_subscription_usage: bool | None = None,
    show_resume_line: bool | None = None,
    budget_enabled: bool | None = None,
    budget_auto_cancel: bool | None = None,
    loop_enabled: bool | None = None,
) -> EngineRunOptions | None:
    """Merge per-run overrides into ``base``.

    Supplied values override, omitted values inherit from ``base``.
    ``goal`` is stripped/empty-to-``None``; attachments become tuples.
    Returns ``None`` if the result carries no active field.
    """
    if (
        base is None
        and not attachments
        and plan is None
        and goal is None
        and model is None
        and reasoning is None
        and skill is None
        and subagent is None
        and permission_mode is None
        and ask_questions is None
        and diff_preview is None
        and show_api_cost is None
        and show_subscription_usage is None
        and show_resume_line is None
        and budget_enabled is None
        and budget_auto_cancel is None
        and loop_enabled is None
    ):
        return None

    def _inherit(field: str, override: object) -> object:
        if override is not None:
            return override
        return getattr(base, field) if base is not None else None

    base_atts = base.attachments if base is not None else ()
    new_atts = tuple(attachments) if attachments is not None else base_atts
    new_plan = bool(base.plan) if base is not None else False
    if plan is not None:
        new_plan = bool(plan)
    new_goal = base.goal if base is not None else None
    if goal is not None:
        cleaned = goal.strip()
        new_goal = cleaned or None

    opts = EngineRunOptions(
        model=_inherit("model", model),  # type: ignore[arg-type]
        reasoning=_inherit("reasoning", reasoning),  # type: ignore[arg-type]
        permission_mode=_inherit("permission_mode", permission_mode),  # type: ignore[arg-type]
        ask_questions=_inherit("ask_questions", ask_questions),  # type: ignore[arg-type]
        diff_preview=_inherit("diff_preview", diff_preview),  # type: ignore[arg-type]
        show_api_cost=_inherit("show_api_cost", show_api_cost),  # type: ignore[arg-type]
        show_subscription_usage=_inherit(
            "show_subscription_usage", show_subscription_usage
        ),  # type: ignore[arg-type]
        show_resume_line=_inherit("show_resume_line", show_resume_line),  # type: ignore[arg-type]
        budget_enabled=_inherit("budget_enabled", budget_enabled),  # type: ignore[arg-type]
        budget_auto_cancel=_inherit("budget_auto_cancel", budget_auto_cancel),  # type: ignore[arg-type]
        loop_enabled=_inherit("loop_enabled", loop_enabled),  # type: ignore[arg-type]
        attachments=new_atts,
        plan=new_plan,
        goal=new_goal,
        skill=_inherit("skill", skill),  # type: ignore[arg-type]
        subagent=_inherit("subagent", subagent),  # type: ignore[arg-type]
    )

    if (
        opts.model is None
        and opts.reasoning is None
        and opts.permission_mode is None
        and opts.ask_questions is None
        and opts.diff_preview is None
        and opts.show_api_cost is None
        and opts.show_subscription_usage is None
        and opts.show_resume_line is None
        and opts.budget_enabled is None
        and opts.budget_auto_cancel is None
        and opts.loop_enabled is None
        and not opts.attachments
        and not opts.plan
        and opts.goal is None
        and opts.skill is None
        and opts.subagent is None
    ):
        return None
    return opts


# Canonical per-engine permission_mode value sets. Used by trigger config
# validators to reject typos at parse time while staying forward-compatible for
# engines not yet listed (the validator accepts any non-empty string for those).
# Extending this dict requires auditing the runner to ensure each value maps to
# a defined CLI / protocol outcome — see issues #331 (Codex + Gemini completion)
# and #332 (full cross-engine extension).
VALID_PERMISSION_MODES_BY_ENGINE: dict[str, frozenset[str]] = {
    "claude": frozenset(
        {"default", "plan", "auto", "acceptEdits", "bypassPermissions"}
    ),
}


_RUN_OPTIONS: ContextVar[EngineRunOptions | None] = ContextVar(
    "untether.engine_run_options", default=None
)


def get_run_options() -> EngineRunOptions | None:
    return _RUN_OPTIONS.get()


def set_run_options(options: EngineRunOptions | None) -> Token:
    return _RUN_OPTIONS.set(options)


def reset_run_options(token: Token) -> None:
    _RUN_OPTIONS.reset(token)


@contextmanager
def apply_run_options(options: EngineRunOptions | None) -> Iterator[None]:
    token = set_run_options(options)
    try:
        yield
    finally:
        reset_run_options(token)
