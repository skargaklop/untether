"""Contract tests for the slash-model command enhancement.

Covers the directive grammar (``/model``, ``--model``, ``--model=``, quoted
``/goal``), chat/topic persistent model overrides, one-message propagation
(``ResolvedMessage.model`` → ``_directive_options``), the pre-enqueue
validation gate, the ``unknown_model_fallback`` setting, and prompt-batch
stickiness for the new model forms.

All engines receive ``--model`` on resume; Untether does not pre-reject model
overrides based on engine capabilities — the engine CLI is authoritative.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from untether.config import ProjectsConfig
from untether.directives import DirectiveError, parse_directives
from untether.router import AutoRouter, RunnerEntry
from untether.runners.mock import Return, ScriptRunner
from untether.settings import TelegramTransportSettings
from untether.telegram.chat_prefs import ChatPrefsStore
from untether.telegram.engine_overrides import EngineOverrides, merge_overrides
from untether.telegram.loop import (
    _directive_options,
    _validate_model_override,
)
from untether.telegram.prompt_batch import (
    PromptBatchSettings,
    is_sticky_model_args,
    should_batch_text,
)
from untether.telegram.topic_state import TopicStateStore
from untether.transport_runtime import ResolvedMessage


def _projects() -> ProjectsConfig:
    return ProjectsConfig(projects={}, default_project=None)


ENGINE_IDS: tuple[str, ...] = ("codex", "claude", "pi")


# ---------------------------------------------------------------------------
# 1. Directive parser cases


def test_model_quoted_goal_with_engine_and_model_eq() -> None:
    """Required observable case: /goal "tests pass" --model=opus /claude fix it."""
    parsed = parse_directives(
        '/goal "tests pass" --model=opus /claude fix it',
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.engine == "claude"
    assert parsed.goal == "tests pass"
    assert parsed.model == "opus"
    assert parsed.prompt == "fix it"
    assert parsed.plan is False


def test_slash_model_with_prompt() -> None:
    parsed = parse_directives(
        "/model opus fix it",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.model == "opus"
    assert parsed.prompt == "fix it"


def test_dashdash_model_with_prompt() -> None:
    parsed = parse_directives(
        "--model opus fix it",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.model == "opus"
    assert parsed.prompt == "fix it"


def test_dashdash_model_eq_with_prompt() -> None:
    parsed = parse_directives(
        "--model=opus fix it",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.model == "opus"
    assert parsed.prompt == "fix it"


def test_bare_model_falls_through() -> None:
    parsed = parse_directives(
        "/model opus",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.model is None
    assert parsed.prompt == "/model opus"


def test_quoted_goal_reorderable() -> None:
    parsed = parse_directives(
        '/goal "a" /model opus /claude fix it',
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.goal == "a"
    assert parsed.model == "opus"
    assert parsed.engine == "claude"
    assert parsed.prompt == "fix it"


def test_legacy_unquoted_goal_terminal() -> None:
    parsed = parse_directives(
        "/goal tests pass",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.goal == "tests pass"
    assert parsed.prompt == ""


def test_model_last_one_wins() -> None:
    parsed = parse_directives(
        "--model a --model b fix it",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.model == "b"
    assert parsed.prompt == "fix it"


def test_unterminated_goal_quote_raises() -> None:
    with pytest.raises(DirectiveError):
        parse_directives(
            '/goal "tests',
            engine_ids=ENGINE_IDS,
            projects=_projects(),
        )


def test_missing_model_value_raises() -> None:
    with pytest.raises(DirectiveError):
        parse_directives(
            "--model",
            engine_ids=ENGINE_IDS,
            projects=_projects(),
        )


def test_model_with_multiline_prompt() -> None:
    parsed = parse_directives(
        "/model opus\nsecond line",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.model == "opus"
    assert parsed.prompt == "second line"


def test_model_with_prompt_and_engine_name_as_model() -> None:
    """Regression: /model gemini-3.6-flash use seversl tools must set
    model=gemini-3.6-flash, prompt='use seversl tools', not dispatch to
    the command handler as a usage error."""
    parsed = parse_directives(
        "/model gemini-3.6-flash use seversl tools",
        engine_ids=ENGINE_IDS,
        projects=_projects(),
    )
    assert parsed.model == "gemini-3.6-flash"
    assert parsed.prompt == "use seversl tools"


def test_parse_set_args_takes_only_first_token_as_model() -> None:
    """Regression: /model set gemini-3.6-flash use seversl tools must set
    model=gemini-3.6-flash, not 'gemini-3.6-flash  use seversl tools'."""
    from untether.telegram.commands.overrides import parse_set_args

    # Without engine prefix: first token is model, rest is ignored.
    engine_arg, model = parse_set_args(
        ("set", "gemini-3.6-flash", "use", "seversl", "tools"),
        engine_ids={"codex", "claude", "pi"},
    )
    assert engine_arg is None
    assert model == "gemini-3.6-flash"

    # With engine prefix: second token is model, rest is ignored.
    engine_arg, model = parse_set_args(
        ("set", "codex", "gemini-3.6-flash", "use", "seversl", "tools"),
        engine_ids={"codex", "claude", "pi"},
    )
    assert engine_arg == "codex"
    assert model == "gemini-3.6-flash"

    # Single model still works.
    engine_arg, model = parse_set_args(
        ("set", "gpt-4.1-mini"),
        engine_ids={"codex", "claude", "pi"},
    )
    assert engine_arg is None
    assert model == "gpt-4.1-mini"

    # Engine + single model still works.
    engine_arg, model = parse_set_args(
        ("set", "codex", "gpt-4.1-mini"),
        engine_ids={"codex", "claude", "pi"},
    )
    assert engine_arg == "codex"
    assert model == "gpt-4.1-mini"


# ---------------------------------------------------------------------------
# 2. Command/store cases


@pytest.mark.anyio
async def test_chat_model_persistence_roundtrip(tmp_path) -> None:
    path = tmp_path / "telegram_chat_prefs_state.json"
    store = ChatPrefsStore(path)
    await store.set_engine_override(123, "claude", EngineOverrides(model="sonnet"))

    override = await store.get_engine_override(123, "claude")
    assert override is not None
    assert override.model == "sonnet"

    store2 = ChatPrefsStore(path)
    override2 = await store2.get_engine_override(123, "claude")
    assert override2 is not None
    assert override2.model == "sonnet"

    await store2.clear_engine_override(123, "claude")
    override3 = await store2.get_engine_override(123, "claude")
    assert override3 is None


@pytest.mark.anyio
async def test_topic_model_persistence_roundtrip(tmp_path) -> None:
    path = tmp_path / "telegram_topics_state.json"
    store = TopicStateStore(path)
    await store.set_engine_override(1, 10, "claude", EngineOverrides(model="opus"))

    override = await store.get_engine_override(1, 10, "claude")
    assert override is not None
    assert override.model == "opus"

    store2 = TopicStateStore(path)
    override2 = await store2.get_engine_override(1, 10, "claude")
    assert override2 is not None
    assert override2.model == "opus"

    await store2.clear_engine_override(1, 10, "claude")
    override3 = await store2.get_engine_override(1, 10, "claude")
    assert override3 is None


def test_topic_over_chat_model_precedence() -> None:
    topic = EngineOverrides(model="opus")
    chat = EngineOverrides(model="sonnet")
    merged = merge_overrides(topic, chat)
    assert merged is not None
    assert merged.model == "opus"


def test_clear_topic_reveals_chat_model() -> None:
    topic = EngineOverrides(model=None)
    chat = EngineOverrides(model="sonnet")
    merged = merge_overrides(topic, chat)
    assert merged is not None
    assert merged.model == "sonnet"


@pytest.mark.anyio
async def test_engine_isolation_in_overrides(tmp_path) -> None:
    path = tmp_path / "telegram_chat_prefs_state.json"
    store = ChatPrefsStore(path)
    await store.set_engine_override(123, "codex", EngineOverrides(model="opus"))

    claude = await store.get_engine_override(123, "claude")
    assert claude is None

    codex = await store.get_engine_override(123, "codex")
    assert codex is not None
    assert codex.model == "opus"


# ---------------------------------------------------------------------------
# 3. Propagation case


def test_directive_options_includes_model() -> None:
    resolved = ResolvedMessage(
        prompt="fix it",
        resume_token=None,
        engine_override=None,
        context=None,
        model="opus",
    )
    opts = _directive_options(resolved)
    assert opts is not None
    assert opts.model == "opus"


# ---------------------------------------------------------------------------
# 4. Capability cases


def test_list_models_hit() -> None:
    runner = ScriptRunner([Return(answer="ok")], engine="claude")
    entry = RunnerEntry(
        engine=runner.engine,
        runner=runner,
        list_models=lambda: ("opus", "sonnet", "haiku"),
    )
    router = AutoRouter(entries=[entry], default_engine=runner.engine)
    assert router.list_models("claude") == ("opus", "sonnet", "haiku")


def test_list_models_unavailable_returns_none() -> None:
    runner = ScriptRunner([Return(answer="ok")], engine="claude")
    entry = RunnerEntry(engine=runner.engine, runner=runner)
    router = AutoRouter(entries=[entry], default_engine=runner.engine)
    assert router.list_models("claude") is None


def test_list_models_exception_returns_none() -> None:
    def _boom() -> tuple[str, ...]:
        raise RuntimeError("discovery failed")

    runner = ScriptRunner([Return(answer="ok")], engine="claude")
    entry = RunnerEntry(engine=runner.engine, runner=runner, list_models=_boom)
    router = AutoRouter(entries=[entry], default_engine=runner.engine)
    assert router.list_models("claude") is None


def test_runner_entry_no_supports_model_on_resume_field() -> None:
    """supports_model_on_resume was removed: Untether always passes --model to
    the engine CLI and lets the engine report its own errors."""
    import dataclasses

    field_names = {f.name for f in dataclasses.fields(RunnerEntry)}
    assert "supports_model_on_resume" not in field_names


# ---------------------------------------------------------------------------
# 5. Validation gate cases


class _FakeRuntime:
    def __init__(
        self,
        models: tuple[str, ...] | None = None,
    ) -> None:
        self._models = models

    def list_models(self, engine: str | None) -> tuple[str, ...] | None:
        return self._models


def test_validation_catalog_hit_allows() -> None:
    runtime = _FakeRuntime(models=("opus", "sonnet"))
    result = _validate_model_override(
        "opus", "claude", runtime=runtime, fallback_enabled=False
    )
    assert result.action == "allow"
    assert result.model == "opus"


def test_validation_confirmed_miss_rejects() -> None:
    runtime = _FakeRuntime(models=("opus", "sonnet"))
    result = _validate_model_override(
        "haiku", "claude", runtime=runtime, fallback_enabled=False
    )
    assert result.action == "reject"
    assert result.message is not None
    assert "haiku" in result.message
    assert "claude" in result.message


def test_validation_confirmed_miss_fallback() -> None:
    runtime = _FakeRuntime(models=("opus", "sonnet"))
    result = _validate_model_override(
        "haiku", "claude", runtime=runtime, fallback_enabled=True
    )
    assert result.action == "fallback"
    assert result.message is not None
    assert "haiku" in result.message
    assert "claude" in result.message


def test_validation_catalog_unavailable_passes_through() -> None:
    runtime = _FakeRuntime(models=None)
    result = _validate_model_override(
        "haiku", "claude", runtime=runtime, fallback_enabled=False
    )
    assert result.action == "allow"
    assert result.model == "haiku"

# ---------------------------------------------------------------------------
# 6. Settings case


def test_unknown_model_fallback_defaults_false() -> None:
    settings = TelegramTransportSettings(
        bot_token=SecretStr("token"),
        chat_id=123,
        allow_any_user=True,
    )
    assert settings.unknown_model_fallback is False


# ---------------------------------------------------------------------------
# 7. Prompt batch cases


def test_model_show_is_sticky() -> None:
    assert is_sticky_model_args("") is True


def test_model_set_is_sticky() -> None:
    assert is_sticky_model_args("set opus") is True


def test_model_clear_is_sticky() -> None:
    assert is_sticky_model_args("clear") is True


def test_model_bare_value_is_sticky() -> None:
    assert is_sticky_model_args("opus") is True


def test_model_with_prompt_not_sticky() -> None:
    assert is_sticky_model_args("opus fix it") is False


def test_model_with_prompt_is_batchable() -> None:
    settings = PromptBatchSettings(enabled=True)
    assert should_batch_text("/model opus fix it", settings=settings) is True
