from untether.model import ResumeToken
from untether.runners.claude import ClaudeRunner
from untether.runners.codex import CodexRunner
from untether.runners.opencode import OpenCodeRunner, OpenCodeStreamState
from untether.runners.pi import ENGINE as PI_ENGINE
from untether.runners.pi import PiRunner, PiStreamState
from untether.runners.run_options import (
    EngineRunOptions,
    PromptAttachment,
    apply_run_options,
    get_run_options,
    merge_run_options,
)


def test_codex_run_options_override_model_and_reasoning() -> None:
    runner = CodexRunner(codex_cmd="codex", extra_args=["-c", "notify=[]"])
    state = runner.new_state("hi", None)
    with apply_run_options(EngineRunOptions(model="gpt-4.1-mini", reasoning="low")):
        args = runner.build_args("hi", None, state=state)

    assert args == [
        "-c",
        "notify=[]",
        "--model",
        "gpt-4.1-mini",
        "-c",
        "model_reasoning_effort=low",
        "--ask-for-approval",
        "never",
        "exec",
        "--json",
        "--skip-git-repo-check",
        "--color=never",
        "-",
    ]


def test_claude_run_options_override_model() -> None:
    runner = ClaudeRunner(claude_cmd="claude", model="claude-sonnet")
    with apply_run_options(EngineRunOptions(model="claude-opus")):
        args = runner.build_args("hi", None, state=None)

    assert "--model" in args
    model_idx = args.index("--model") + 1
    assert args[model_idx] == "claude-opus"


def test_opencode_run_options_override_model() -> None:
    runner = OpenCodeRunner(opencode_cmd="opencode", model="claude-sonnet")
    state = OpenCodeStreamState()
    with apply_run_options(EngineRunOptions(model="gpt-4o-mini")):
        args = runner.build_args("hi", None, state=state)

    assert "--model" in args
    model_idx = args.index("--model") + 1
    assert args[model_idx] == "gpt-4o-mini"


def test_pi_run_options_override_model() -> None:
    runner = PiRunner(pi_cmd="pi", extra_args=[], model="pi-default", provider=None)
    state = PiStreamState(resume=ResumeToken(engine=PI_ENGINE, value="sess.jsonl"))
    with apply_run_options(EngineRunOptions(model="pi-override")):
        args = runner.build_args("hi", None, state=state)

    assert "--model" in args
    model_idx = args.index("--model") + 1
    assert args[model_idx] == "pi-override"


def test_claude_auto_mode_passes_plan_to_cli() -> None:
    """permission_mode 'auto' produces '--permission-mode plan' in CLI args."""
    runner = ClaudeRunner(claude_cmd="claude", permission_mode="auto")
    args = runner.build_args("hi", None, state=None)

    assert "--permission-mode" in args
    mode_idx = args.index("--permission-mode") + 1
    assert args[mode_idx] == "plan"


def test_merge_run_options_returns_none_without_active_values() -> None:
    assert merge_run_options(None) is None


def test_merge_run_options_normalizes_goal_and_attachments() -> None:
    attachment = PromptAttachment("image.png", "C:/project/image.png", "image/png")

    options = merge_run_options(None, attachments=[attachment], goal="  ship it  ")

    assert options is not None
    assert options.attachments == (attachment,)
    assert options.goal == "ship it"


def test_merge_run_options_inherits_and_overrides_explicit_values() -> None:
    base = EngineRunOptions(model="base", plan=True, ask_questions=True, goal="retain")

    options = merge_run_options(base, model="override", plan=False, goal="   ")

    assert options == EngineRunOptions(
        model="override", plan=False, ask_questions=True, goal=None
    )


def test_apply_run_options_restores_prior_context() -> None:
    outer = EngineRunOptions(model="outer")
    inner = EngineRunOptions(model="inner")

    with apply_run_options(outer):
        assert get_run_options() == outer
        with apply_run_options(inner):
            assert get_run_options() == inner
        assert get_run_options() == outer

    assert get_run_options() is None
