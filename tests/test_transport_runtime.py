from pathlib import Path

from untether.config import ProjectConfig, ProjectsConfig
from untether.context import RunContext
from untether.router import AutoRouter, RunnerEntry
from untether.runners.mock import Return, ScriptRunner
from untether.transport_runtime import TransportRuntime


def _make_runtime(*, project_default_engine: str | None = None) -> TransportRuntime:
    codex = ScriptRunner([Return(answer="ok")], engine="codex")
    pi = ScriptRunner([Return(answer="ok")], engine="pi")
    router = AutoRouter(
        entries=[
            RunnerEntry(engine=codex.engine, runner=codex),
            RunnerEntry(engine=pi.engine, runner=pi),
        ],
        default_engine=codex.engine,
    )
    project = ProjectConfig(
        alias="proj",
        path=Path("."),
        worktrees_dir=Path(".worktrees"),
        default_engine=project_default_engine,
    )
    projects = ProjectsConfig(projects={"proj": project}, default_project=None)
    return TransportRuntime(router=router, projects=projects)


def test_dynamic_engine_ids_field_exists_and_threads_through_update() -> None:
    """The getattr-based telegram guard needs a real `dynamic_engine_ids`
    attribute; default empty, threaded through update().
    """
    runtime = _make_runtime()
    assert runtime.dynamic_engine_ids == frozenset()
    runtime.update(
        router=runtime._router,
        projects=runtime._projects,
        dynamic_engine_ids=frozenset({"agent_id"}),
    )
    assert runtime.dynamic_engine_ids == frozenset({"agent_id"})


def test_resolve_message_extracts_pi_engine_directive() -> None:
    runtime = _make_runtime()

    resolved = runtime.resolve_message(text="/pi hello", reply_text=None)

    assert resolved.prompt == "hello"
    assert resolved.engine_override == "pi"


def test_resolve_engine_uses_project_default() -> None:
    runtime = _make_runtime(project_default_engine="pi")
    engine = runtime.resolve_engine(
        engine_override=None,
        context=RunContext(project="proj"),
    )
    assert engine == "pi"


def test_resolve_engine_prefers_override() -> None:
    runtime = _make_runtime(project_default_engine="pi")
    engine = runtime.resolve_engine(
        engine_override="codex",
        context=RunContext(project="proj"),
    )
    assert engine == "codex"


def test_resolve_message_defaults_to_chat_project() -> None:
    codex = ScriptRunner([Return(answer="ok")], engine="codex")
    router = AutoRouter(
        entries=[RunnerEntry(engine=codex.engine, runner=codex)],
        default_engine=codex.engine,
    )
    project = ProjectConfig(
        alias="proj",
        path=Path("."),
        worktrees_dir=Path(".worktrees"),
        chat_id=-42,
    )
    projects = ProjectsConfig(
        projects={"proj": project},
        default_project=None,
        chat_map={-42: "proj"},
    )
    runtime = TransportRuntime(router=router, projects=projects)

    resolved = runtime.resolve_message(
        text="hello",
        reply_text=None,
        chat_id=-42,
    )

    assert resolved.context == RunContext(project="proj", branch=None)


def test_resolve_message_uses_ambient_context() -> None:
    runtime = _make_runtime()
    ambient = RunContext(project="proj", branch="feat/ambient")

    resolved = runtime.resolve_message(
        text="hello",
        reply_text=None,
        ambient_context=ambient,
    )

    assert resolved.context == ambient
    assert resolved.context_source == "ambient"


def test_resolve_message_reply_ctx_overrides_ambient() -> None:
    runtime = _make_runtime()
    ambient = RunContext(project="proj", branch="feat/ambient")

    resolved = runtime.resolve_message(
        text="hello",
        reply_text="`ctx: proj @reply`",
        ambient_context=ambient,
    )

    assert resolved.context == RunContext(project="proj", branch="reply")
    assert resolved.context_source == "reply_ctx"


def test_resolve_message_directives_override_ambient() -> None:
    runtime = _make_runtime()
    ambient = RunContext(project="proj", branch="feat/ambient")

    resolved = runtime.resolve_message(
        text="/proj @main do it",
        reply_text=None,
        ambient_context=ambient,
    )

    assert resolved.context == RunContext(project="proj", branch="main")
    assert resolved.context_source == "directives"


def test_resolve_message_branch_directive_merges_with_ambient_project() -> None:
    runtime = _make_runtime()
    ambient = RunContext(project="proj", branch="feat/ambient")

    resolved = runtime.resolve_message(
        text="@hotfix do it",
        reply_text=None,
        ambient_context=ambient,
    )

    assert resolved.context == RunContext(project="proj", branch="hotfix")
    assert resolved.context_source == "directives"


def test_resolve_message_project_directive_clears_ambient_branch() -> None:
    codex = ScriptRunner([Return(answer="ok")], engine="codex")
    router = AutoRouter(
        entries=[RunnerEntry(engine=codex.engine, runner=codex)],
        default_engine=codex.engine,
    )
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
            ),
            "other": ProjectConfig(
                alias="other",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
            ),
        },
        default_project=None,
    )
    runtime = TransportRuntime(router=router, projects=projects)
    ambient = RunContext(project="proj", branch="feat/ambient")

    resolved = runtime.resolve_message(
        text="/other do it",
        reply_text=None,
        ambient_context=ambient,
    )

    assert resolved.context == RunContext(project="other", branch=None)
    assert resolved.context_source == "directives"


def test_resolve_message_slash_engine_bare_resume_directive() -> None:
    """`/pi --resume <id>` — engine directive consumed, bare resume binds to pi.

    Regression: the id was silently ignored and the run continued in the
    topic's stored session (or fresh). The bare resume form must resolve
    against the directive engine and be stripped from the prompt.
    """
    runtime = _make_runtime()

    resolved = runtime.resolve_message(
        text="/pi --resume 019f589d-9c90-7000-a710-f828d1a7c716", reply_text=None
    )

    assert resolved.engine_override == "pi"
    assert resolved.resume_token is not None
    assert resolved.resume_token.engine == "pi"
    assert resolved.resume_token.value == "019f589d-9c90-7000-a710-f828d1a7c716"
    assert resolved.prompt == ""
    assert resolved.resume_from_bare is True


def test_resolve_message_bare_resume_without_engine_uses_default() -> None:
    """Flag form `--resume <id>` with no engine directive binds to the default
    engine (flag forms are unambiguous — never natural language)."""
    runtime = _make_runtime()  # default engine: codex

    resolved = runtime.resolve_message(text="--resume abc123", reply_text=None)

    assert resolved.resume_token is not None
    assert resolved.resume_token.engine == "codex"
    assert resolved.resume_token.value == "abc123"
    assert resolved.prompt == ""
    assert resolved.resume_from_bare is True


def test_resolve_message_bare_resume_with_prompt_rest() -> None:
    """Bare resume followed by prompt text keeps the text as the prompt."""
    runtime = _make_runtime()

    resolved = runtime.resolve_message(
        text="/pi --resume sess-1 fix the failing test now", reply_text=None
    )

    assert resolved.resume_token is not None
    assert resolved.resume_token.value == "sess-1"
    assert resolved.prompt == "fix the failing test now"


def test_resolve_message_engine_resume_alias_overrides_engine() -> None:
    """`codex resume <id>` anywhere — alias engine wins over directives."""
    runtime = _make_runtime()

    resolved = runtime.resolve_message(text="codex resume abc", reply_text=None)

    assert resolved.resume_token is not None
    assert resolved.resume_token.engine == "codex"
    assert resolved.resume_token.value == "abc"


def test_resolve_message_engine_alias_no_engine_registered_keeps_none() -> None:
    """Unknown engine in the alias (`foo resume x`) must not produce a token."""
    runtime = _make_runtime()

    resolved = runtime.resolve_message(text="foo resume abc", reply_text=None)

    assert resolved.resume_token is None
    assert resolved.prompt == "foo resume abc"


def test_resolve_message_plain_prompt_unchanged() -> None:
    """Sanity: no resume forms — nothing consumed, no token."""
    runtime = _make_runtime()

    resolved = runtime.resolve_message(text="/pi hello world", reply_text=None)

    assert resolved.resume_token is None
    assert resolved.prompt == "hello world"


def test_resolve_message_slash_engine_resume_alias_keeps_prompt() -> None:
    """`/pi pi resume x` — the in-prompt engine-prefixed form still wins
    and the line is stripped, leaving the rest of the prompt."""
    runtime = _make_runtime()

    resolved = runtime.resolve_message(
        text="/pi `pi resume session.jsonl`\ncontinue the work", reply_text=None
    )

    assert resolved.resume_token is not None
    assert resolved.resume_token.engine == "pi"
    assert resolved.resume_token.value == "session.jsonl"
    assert resolved.prompt == "continue the work"


def test_resolve_message_bare_keyword_resume_requires_engine_directive() -> None:
    """Bare `resume <word>` must NOT hijack a plain prompt (no engine context);
    with an explicit engine directive it is an unambiguous session reference."""
    runtime = _make_runtime()

    plain = runtime.resolve_message(text="resume work on the parser", reply_text=None)
    assert plain.resume_token is None
    assert plain.prompt == "resume work on the parser"

    addressed = runtime.resolve_message(text="/pi resume sess-9", reply_text=None)
    assert addressed.resume_token is not None
    assert addressed.resume_token.engine == "pi"
    assert addressed.resume_token.value == "sess-9"
    assert addressed.prompt == ""


def test_resolve_message_slash_bare_resume_all_engines() -> None:
    """Cross-engine regression: `/ENGINE --resume <id>` must bind the id to
    ENGINE for every registered engine (universal alias), not silently fall
    back to a stored topic session.
    """
    from untether.runners.agy import AgyRunner
    from untether.runners.amp import AmpRunner
    from untether.runners.claude import ClaudeRunner
    from untether.runners.codex import CodexRunner
    from untether.runners.gemini import GeminiRunner
    from untether.runners.grok import GrokRunner
    from untether.runners.omp import OmpRunner
    from untether.runners.opencode import OpenCodeRunner

    runners: list = [
        ClaudeRunner(),
        CodexRunner(codex_cmd="codex", extra_args=[]),
        GrokRunner(),
        GeminiRunner(),
        AgyRunner(),
        OpenCodeRunner(),
        OmpRunner(extra_args=[], model=None, provider=None),
        AmpRunner(),
    ]
    entries = [RunnerEntry(engine=r.engine, runner=r) for r in runners]
    router = AutoRouter(entries=entries, default_engine="codex")
    projects = ProjectsConfig(projects={}, default_project=None)
    runtime = TransportRuntime(router=router, projects=projects)

    for runner in runners:
        engine = runner.engine
        sid = "sess-abc123"
        resolved = runtime.resolve_message(
            text=f"/{engine} --resume {sid} continue the work", reply_text=None
        )
        assert resolved.engine_override == engine
        assert resolved.resume_token is not None, f"{engine}: no token"
        assert resolved.resume_token.engine == engine, f"{engine}: wrong engine"
        assert resolved.resume_token.value == sid, f"{engine}: wrong id"
        assert resolved.prompt == "continue the work", f"{engine}: prompt dirty"


def test_resolve_message_engine_resume_alias_all_engines() -> None:
    """`ENGINE resume <id>` (unbackticked, no slash) must resolve for every
    engine via the universal alias — today only some runners' own regexes
    accept it.
    """
    from untether.runners.agy import AgyRunner
    from untether.runners.amp import AmpRunner
    from untether.runners.claude import ClaudeRunner
    from untether.runners.codex import CodexRunner
    from untether.runners.gemini import GeminiRunner
    from untether.runners.grok import GrokRunner
    from untether.runners.omp import OmpRunner
    from untether.runners.opencode import OpenCodeRunner

    runners: list = [
        ClaudeRunner(),
        CodexRunner(codex_cmd="codex", extra_args=[]),
        GrokRunner(),
        GeminiRunner(),
        AgyRunner(),
        OpenCodeRunner(),
        OmpRunner(extra_args=[], model=None, provider=None),
        AmpRunner(),
    ]
    entries = [RunnerEntry(engine=r.engine, runner=r) for r in runners]
    router = AutoRouter(entries=entries, default_engine="codex")
    projects = ProjectsConfig(projects={}, default_project=None)
    runtime = TransportRuntime(router=router, projects=projects)

    for runner in runners:
        engine = runner.engine
        sid = "conv-7788"
        resolved = runtime.resolve_message(
            text=f"{engine} resume {sid}\ncontinue the work", reply_text=None
        )
        assert resolved.resume_token is not None, f"{engine}: no token"
        assert resolved.resume_token.engine == engine, f"{engine}: wrong engine"
        assert resolved.resume_token.value == sid, f"{engine}: wrong id"
        assert resolved.prompt == "continue the work", f"{engine}: prompt dirty"
