"""Tests for shared plan/goal mode helpers."""

from __future__ import annotations

from untether.runners.modes import (
    SOFT_PLAN_PREFIX,
    apply_goal_prompt,
    apply_soft_plan_prompt,
    effective_prompt,
    run_modes,
)
from untether.runners.run_options import EngineRunOptions, apply_run_options


class TestRunModes:
    def test_no_options(self) -> None:
        with apply_run_options(None):
            assert run_modes() == (False, None)

    def test_plan_only(self) -> None:
        with apply_run_options(EngineRunOptions(plan=True)):
            assert run_modes() == (True, None)

    def test_goal_wins_over_plan(self) -> None:
        with apply_run_options(EngineRunOptions(plan=True, goal="all tests pass")):
            plan, goal = run_modes()
            assert plan is False
            assert goal == "all tests pass"

    def test_goal_strips_whitespace(self) -> None:
        with apply_run_options(EngineRunOptions(goal="  spaced  ")):
            _, goal = run_modes()
            assert goal == "spaced"

    def test_empty_goal_is_none(self) -> None:
        with apply_run_options(EngineRunOptions(goal="")):
            assert run_modes() == (False, None)


class TestApplyGoalPrompt:
    def test_adds_goal_prefix(self) -> None:
        result = apply_goal_prompt("do the work", "all tests pass")
        assert result == "/goal all tests pass"

    def test_skips_if_already_goal(self) -> None:
        result = apply_goal_prompt("/goal condition", "condition")
        assert result == "/goal condition"

    def test_empty_condition_returns_prompt(self) -> None:
        result = apply_goal_prompt("do the work", "")
        assert result == "do the work"


class TestApplySoftPlanPrompt:
    def test_empty_prompt(self) -> None:
        assert apply_soft_plan_prompt("") == SOFT_PLAN_PREFIX

    def test_adds_prefix(self) -> None:
        result = apply_soft_plan_prompt("do the work")
        assert result.startswith(SOFT_PLAN_PREFIX)
        assert "do the work" in result

    def test_idempotent(self) -> None:
        already = f"{SOFT_PLAN_PREFIX}\n\ndo the work"
        assert apply_soft_plan_prompt(already) == already


class TestEffectivePrompt:
    def test_goal_applied(self) -> None:
        with apply_run_options(EngineRunOptions(goal="all tests pass")):
            result = effective_prompt("do the work")
            assert result == "/goal all tests pass"

    def test_soft_plan_applied(self) -> None:
        with apply_run_options(EngineRunOptions(plan=True)):
            result = effective_prompt("do the work", soft_plan=True)
            assert result.startswith(SOFT_PLAN_PREFIX)

    def test_no_modes(self) -> None:
        with apply_run_options(None):
            assert effective_prompt("do the work") == "do the work"

    def test_goal_overrides_soft_plan(self) -> None:
        with apply_run_options(EngineRunOptions(plan=True, goal="condition")):
            result = effective_prompt("do the work", soft_plan=True)
            assert result == "/goal condition"
