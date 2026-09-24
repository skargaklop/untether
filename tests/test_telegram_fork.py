from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from untether.config import ProjectConfig, ProjectsConfig
from untether.markdown import MarkdownPresenter
from untether.model import ResumeToken
from untether.router import AutoRouter, RunnerEntry
from untether.runner import ForkableRunner
from untether.runner_bridge import ExecBridgeConfig
from untether.runners.acp.runner import AcpRunner
from untether.runners.agy import AgyRunner
from untether.runners.amp import AmpRunner
from untether.runners.claude import ClaudeRunner
from untether.runners.codex import AppServerCodexRunner, CodexRunner
from untether.runners.gemini import GeminiRunner
from untether.runners.grok import GrokRunner
from untether.runners.mock import Return, ScriptRunner
from untether.runners.omp import OmpRunner
from untether.runners.opencode import OpenCodeRunner
from untether.runners.pi import PiRunner
from untether.settings import TelegramTopicsSettings
from untether.telegram.bridge import TelegramBridgeConfig, run_main_loop
from untether.telegram.chat_sessions import ChatSessionStore
from untether.telegram.commands.fork import fork_session
from untether.telegram.commands.parse import ForkInvocation, parse_fork_invocation
from untether.telegram.topic_state import TopicStateStore, resolve_state_path
from untether.telegram.types import TelegramIncomingMessage
from untether.transport_runtime import TransportRuntime

from .telegram_fakes import FakeBot, FakeTransport


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("/fork", ForkInvocation()),
        ("/fork CODEX", ForkInvocation(engine="codex")),
        (
            "/fork@untether_bot codex source-thread",
            ForkInvocation(engine="codex", session_id="source-thread"),
        ),
    ],
)
def test_parse_fork_accepts_only_approved_forms(
    text: str, expected: ForkInvocation
) -> None:
    assert parse_fork_invocation(text, engine_ids=("claude", "codex")) == expected


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("/fork source-thread", "unknown engine"),
        ("/fork codex source-thread extra", "usage"),
        ("/fork codex\nsource-thread", "usage"),
        ("/fork codex source-thread\nextra", "usage"),
    ],
)
def test_parse_fork_rejects_unapproved_forms(text: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_fork_invocation(text, engine_ids=("claude", "codex"))


def test_only_codex_app_server_advertises_native_fork() -> None:
    unsupported = [
        CodexRunner(codex_cmd="codex", extra_args=[]),
        ClaudeRunner(claude_cmd="claude"),
        PiRunner(pi_cmd="pi", extra_args=[], model=None, provider=None),
        GrokRunner(grok_cmd="grok", extra_args=[]),
        GeminiRunner(gemini_cmd="gemini"),
        OpenCodeRunner(opencode_cmd="opencode"),
        AmpRunner(amp_cmd="amp"),
        OmpRunner(extra_args=[], model=None, provider=None),
        AgyRunner(agy_cmd="agy", extra_args=[]),
        AcpRunner(engine="acp-test", command="agent"),
    ]

    assert isinstance(
        AppServerCodexRunner(codex_cmd="codex", extra_args=[]), ForkableRunner
    )
    assert all(not isinstance(runner, ForkableRunner) for runner in unsupported)


@pytest.mark.anyio
async def test_fork_requires_runner_capability_and_does_not_write(
    tmp_path: Path,
) -> None:
    store = ChatSessionStore(tmp_path / "chat.json")
    await store.set_session_resume(7, None, ResumeToken("mock", "source"))

    result = await fork_session(
        runner=ScriptRunner([Return(answer="ok")], engine="mock", resume_value="new"),
        engine="mock",
        explicit_session=None,
        chat_store=store,
        chat_key=(7, None),
    )

    assert not result.ok
    assert "not supported" in result.message.lower()
    assert await store.get_session_resume(7, None, "mock") == ResumeToken(
        "mock", "source"
    )


@pytest.mark.anyio
async def test_fork_persists_only_after_runner_returns_new_session(
    tmp_path: Path,
) -> None:
    class ForkRunner(ScriptRunner):
        async def fork(self, session: ResumeToken) -> ResumeToken:
            assert session == ResumeToken("mock", "source")
            return ResumeToken("mock", "forked")

    store = ChatSessionStore(tmp_path / "chat.json")
    await store.set_session_resume(7, None, ResumeToken("mock", "source"))
    result = await fork_session(
        runner=ForkRunner([Return(answer="ok")], engine="mock"),
        engine="mock",
        explicit_session=None,
        chat_store=store,
        chat_key=(7, None),
    )

    assert result.ok
    assert result.token == ResumeToken("mock", "forked")
    assert await store.get_session_resume(7, None, "mock") == ResumeToken(
        "mock", "forked"
    )


@pytest.mark.anyio
async def test_fork_supports_topic_store_and_explicit_session(tmp_path: Path) -> None:
    class ForkRunner(ScriptRunner):
        async def fork(self, session: ResumeToken) -> ResumeToken:
            return ResumeToken("mock", session.value + "-fork")

    store = TopicStateStore(tmp_path / "topic.json")
    await store.set_session_resume(7, 3, ResumeToken("mock", "latest"))
    result = await fork_session(
        runner=ForkRunner([Return(answer="ok")], engine="mock"),
        engine="mock",
        explicit_session=ResumeToken("mock", "chosen"),
        topic_store=store,
        topic_key=(7, 3),
    )

    assert result.ok
    assert await store.get_session_resume(7, 3, "mock") == ResumeToken(
        "mock", "chosen-fork"
    )


@pytest.mark.anyio
async def test_fork_prefers_topic_source_for_current_topic(tmp_path: Path) -> None:
    seen: list[ResumeToken] = []

    class ForkRunner(ScriptRunner):
        async def fork(self, session: ResumeToken) -> ResumeToken:
            seen.append(session)
            return ResumeToken("mock", "forked")

    chat_store = ChatSessionStore(tmp_path / "chat.json")
    topic_store = TopicStateStore(tmp_path / "topic.json")
    await chat_store.set_session_resume(7, None, ResumeToken("mock", "chat-source"))
    await topic_store.set_session_resume(7, 3, ResumeToken("mock", "topic-source"))

    result = await fork_session(
        runner=ForkRunner([Return(answer="unused")], engine="mock"),
        engine="mock",
        chat_store=chat_store,
        chat_key=(7, None),
        topic_store=topic_store,
        topic_key=(7, 3),
    )

    assert result.ok
    assert seen == [ResumeToken("mock", "topic-source")]


@pytest.mark.anyio
async def test_fork_provider_failure_preserves_old_state(tmp_path: Path) -> None:
    class FailingForkRunner(ScriptRunner):
        async def fork(self, session: ResumeToken) -> ResumeToken:
            raise RuntimeError("secret provider detail")

    store = ChatSessionStore(tmp_path / "chat.json")
    old = ResumeToken("mock", "source")
    await store.set_session_resume(7, None, old)

    result = await fork_session(
        runner=FailingForkRunner([Return(answer="unused")], engine="mock"),
        engine="mock",
        chat_store=store,
        chat_key=(7, None),
    )

    assert not result.ok
    assert result.message == "could not fork mock session"
    assert await store.get_session_resume(7, None, "mock") == old


@pytest.mark.anyio
async def test_fork_rejects_mismatched_returned_engine_without_writing(
    tmp_path: Path,
) -> None:
    class WrongEngineForkRunner(ScriptRunner):
        async def fork(self, session: ResumeToken) -> ResumeToken:
            return ResumeToken("claude", "wrong")

    store = ChatSessionStore(tmp_path / "chat.json")
    old = ResumeToken("mock", "source")
    await store.set_session_resume(7, None, old)

    result = await fork_session(
        runner=WrongEngineForkRunner([Return(answer="unused")], engine="mock"),
        engine="mock",
        chat_store=store,
        chat_key=(7, None),
    )

    assert not result.ok
    assert "valid session" in result.message
    assert await store.get_session_resume(7, None, "mock") == old


@pytest.mark.anyio
async def test_main_loop_fork_uses_topic_effective_engine_and_persists(
    tmp_path: Path,
) -> None:
    class ForkRunner(ScriptRunner):
        async def fork(self, session: ResumeToken) -> ResumeToken:
            assert session == ResumeToken("claude", "source")
            return ResumeToken("claude", "forked")

    state_path = tmp_path / "untether.toml"
    store = TopicStateStore(resolve_state_path(state_path))
    await store.set_default_engine(123, 77, "claude")
    await store.set_session_resume(123, 77, ResumeToken("claude", "source"))
    codex = ScriptRunner([Return(answer="unused")], engine="codex")
    claude = ForkRunner([Return(answer="unused")], engine="claude")
    runtime = TransportRuntime(
        router=AutoRouter(
            entries=[
                RunnerEntry(engine="codex", runner=codex),
                RunnerEntry(engine="claude", runner=claude),
            ],
            default_engine="codex",
        ),
        projects=ProjectsConfig(
            projects={
                "proj": ProjectConfig(
                    alias="proj",
                    path=tmp_path,
                    worktrees_dir=Path(".worktrees"),
                    chat_id=123,
                )
            },
            chat_map={123: "proj"},
        ),
        config_path=state_path,
    )
    transport = FakeTransport()
    cfg = TelegramBridgeConfig(
        bot=FakeBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport, presenter=MarkdownPresenter(), final_notify=True
        ),
        topics=TelegramTopicsSettings(enabled=True, scope="main"),
    )

    async def poller(
        _cfg: TelegramBridgeConfig,
    ) -> AsyncIterator[TelegramIncomingMessage]:
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/fork",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            thread_id=77,
        )

    await run_main_loop(cfg, poller)

    assert await TopicStateStore(resolve_state_path(state_path)).get_session_resume(
        123, 77, "claude"
    ) == ResumeToken("claude", "forked")
    assert any(
        "forked claude session" in call["message"].text.lower()
        for call in transport.send_calls
    )


@pytest.mark.anyio
async def test_main_loop_fork_reply_uses_replied_session_engine(
    tmp_path: Path,
) -> None:
    class ForkRunner(ScriptRunner):
        def extract_resume(self, text: str | None) -> ResumeToken | None:
            if text and "pi --session source" in text:
                return ResumeToken("pi", "source")
            return None

        async def fork(self, session: ResumeToken) -> ResumeToken:
            assert session == ResumeToken("pi", "source")
            return ResumeToken("pi", "forked")

    transport = FakeTransport()
    pi_runner = ForkRunner([Return(answer="unused")], engine="pi")
    omp_runner = ScriptRunner([Return(answer="unused")], engine="omp")
    runtime = TransportRuntime(
        router=AutoRouter(
            entries=[
                RunnerEntry(engine="pi", runner=pi_runner),
                RunnerEntry(engine="omp", runner=omp_runner),
            ],
            default_engine="omp",
        ),
        projects=ProjectsConfig(projects={}),
        config_path=tmp_path / "untether.toml",
    )
    cfg = TelegramBridgeConfig(
        bot=FakeBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport, presenter=MarkdownPresenter(), final_notify=True
        ),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/fork",
            reply_to_message_id=99,
            reply_to_text="done · pi\n↩️ `pi --session source`",
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert any(
        "forked pi session" in call["message"].text.lower()
        for call in transport.send_calls
    )
    assert omp_runner.calls == []


@pytest.mark.anyio
async def test_main_loop_fork_parse_error_is_a_reply_not_a_loop_crash(
    tmp_path: Path,
) -> None:
    runner = ScriptRunner([Return(answer="unused")], engine="codex")
    transport = FakeTransport()
    runtime = TransportRuntime(
        router=AutoRouter(
            entries=[RunnerEntry(engine="codex", runner=runner)],
            default_engine="codex",
        ),
        projects=ProjectsConfig(projects={}),
        config_path=tmp_path / "untether.toml",
    )
    cfg = TelegramBridgeConfig(
        bot=FakeBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport, presenter=MarkdownPresenter(), final_notify=True
        ),
    )

    async def poller(
        _cfg: TelegramBridgeConfig,
    ) -> AsyncIterator[TelegramIncomingMessage]:
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/fork codex source extra",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert any("usage: /fork" in call["message"].text for call in transport.send_calls)
