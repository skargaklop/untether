from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import anyio
import pytest

from tests.telegram_fakes import FakeTransport, make_cfg
from untether.context import RunContext
from untether.telegram.bridge import TelegramBridgeConfig, run_main_loop
from untether.telegram.commands.direct_shell import (
    DirectShellError,
    ShellResult,
    build_shell_argv,
    discover_shell,
    execute_shell,
    format_shell_result,
    parse_shell_command,
    resolve_shell_cwd,
)
from untether.telegram.types import TelegramIncomingMessage


def test_parse_shell_command_requires_content_and_only_leading_nohup_detaches() -> None:
    with pytest.raises(DirectShellError, match="command is required"):
        parse_shell_command("  ")

    assert parse_shell_command("echo hi") == (False, "echo hi")
    assert parse_shell_command("nohup echo hi") == (True, "echo hi")
    assert parse_shell_command(" nohup   echo hi ") == (True, "echo hi")
    assert parse_shell_command("echo nohup") == (False, "echo nohup")

    with pytest.raises(DirectShellError, match="command is required"):
        parse_shell_command("nohup")


def test_discover_and_build_shell_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.shutil.which",
        lambda name: {"bash": "/bin/bash", "pwsh": "/bin/pwsh"}.get(name),
    )

    assert discover_shell("bash", platform="linux") == "/bin/bash"
    assert build_shell_argv("bash", "/bin/bash", "printf hi") == [
        "/bin/bash",
        "-lc",
        "printf hi",
    ]
    assert discover_shell("powershell", platform="win32") == "/bin/pwsh"
    assert build_shell_argv("powershell", "/bin/pwsh", "Write-Output hi") == [
        "/bin/pwsh",
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "Write-Output hi",
    ]


def test_powershell_rejected_off_windows_and_missing_bash_is_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.shutil.which", lambda _name: None
    )
    with pytest.raises(DirectShellError, match="Windows only"):
        discover_shell("powershell", platform="linux")
    with pytest.raises(DirectShellError, match=r"bash.*not found"):
        discover_shell("bash", platform="win32")


def test_powershell_discovery_prefers_pwsh(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def which(name: str) -> str | None:
        calls.append(name)
        return f"C:/{name}.exe"

    monkeypatch.setattr("untether.telegram.commands.direct_shell.shutil.which", which)
    assert discover_shell("powershell", platform="win32") == "C:/pwsh.exe"
    assert calls == ["pwsh"]


def test_resolve_shell_cwd_prefers_context_then_chat_then_run_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = RunContext(project="topic")

    class Runtime:
        use_chat_context = False

        def default_context_for_chat(
            self, chat_id: int | str | None
        ) -> RunContext | None:
            return RunContext(project="chat") if chat_id == 7 else None

        def resolve_run_cwd(self, context: RunContext | None) -> Path | None:
            if self.use_chat_context:
                return tmp_path / "chat" if context else None
            return tmp_path / "topic" if context is topic_context else None

    topic_context = context
    runtime = Runtime()
    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.get_run_base_dir",
        lambda: tmp_path / "base",
    )

    assert resolve_shell_cwd(runtime, context, 7) == tmp_path / "topic"
    assert resolve_shell_cwd(runtime, None, 7) == tmp_path / "base"
    runtime.use_chat_context = True
    assert resolve_shell_cwd(runtime, None, 7) == tmp_path / "chat"
    assert resolve_shell_cwd(runtime, None, 8) == tmp_path / "base"


@pytest.mark.anyio
async def test_execute_shell_foreground_success_failure_empty_and_decoding() -> None:
    success = await execute_shell(
        [
            os.fspath(Path(sys.executable)),
            "-c",
            "import os; os.write(1, b'out\\n'); os.write(2, b'err\\n')",
        ],
        cwd=None,
        timeout_s=5,
        max_output_bytes=1024,
    )
    assert success.exit_code == 0
    assert success.output == "out\nerr\n"
    assert not success.timed_out

    failure = await execute_shell(
        [sys.executable, "-c", "import sys; sys.exit(7)"],
        cwd=None,
        timeout_s=5,
        max_output_bytes=1024,
    )
    assert failure.exit_code == 7
    assert failure.output == ""

    undecodable = await execute_shell(
        [sys.executable, "-c", "import os; os.write(1, bytes([255]))"],
        cwd=None,
        timeout_s=5,
        max_output_bytes=1024,
    )
    assert undecodable.output == "�"


@pytest.mark.anyio
async def test_execute_shell_truncates_but_drains_and_times_out() -> None:
    truncated = await execute_shell(
        [sys.executable, "-c", "print('x' * 10000)"],
        cwd=None,
        timeout_s=5,
        max_output_bytes=64,
    )
    assert len(truncated.output.encode()) <= 64
    assert truncated.truncated

    timed_out = await execute_shell(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        cwd=None,
        timeout_s=0.05,
        max_output_bytes=64,
    )
    assert timed_out.timed_out
    assert timed_out.exit_code is not None


@pytest.mark.anyio
async def test_execute_shell_reaps_child_after_shell_exits_before_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reaped: list[bool] = []

    class Manager:
        async def __aenter__(self):
            self.proc = SimpleNamespace(
                stdout=SimpleNamespace(receive=self.receive),
                wait=AsyncMock(return_value=0),
                returncode=0,
            )
            return self.proc

        async def __aexit__(self, *_args):
            reaped.append(True)

        async def receive(self, _size: int) -> bytes:
            await anyio.sleep_forever()
            raise AssertionError("unreachable")

    def manager(*_args, **kwargs):
        assert kwargs["reap_orphans"] is True
        return Manager()

    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.manage_subprocess", manager
    )

    result = await execute_shell(
        ["fake"], cwd=None, timeout_s=0.01, max_output_bytes=10
    )

    assert result.timed_out
    assert reaped == [True]


@pytest.mark.anyio
async def test_execute_shell_cancellation_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleaned = anyio.Event()

    class Manager:
        async def __aenter__(self):
            self.proc = SimpleNamespace(
                stdout=SimpleNamespace(receive=self.receive),
                wait=self.wait,
                returncode=None,
            )
            return self.proc

        async def __aexit__(self, *_args):
            cleaned.set()

        async def receive(self, _size: int) -> bytes:
            await anyio.sleep_forever()
            raise AssertionError("unreachable")

        async def wait(self) -> int:
            await anyio.sleep_forever()
            raise AssertionError("unreachable")

    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.manage_subprocess",
        lambda *_args, **_kwargs: Manager(),
    )
    with anyio.move_on_after(0.01):
        await execute_shell(["fake"], cwd=None, timeout_s=30, max_output_bytes=10)
    assert cleaned.is_set()


@pytest.mark.anyio
async def test_detached_execution_uses_disconnected_stdio_and_platform_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = Mock(return_value=SimpleNamespace(pid=321))
    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.subprocess.Popen", opened
    )

    result = await execute_shell(
        ["bash", "-lc", "work"],
        cwd=Path("workdir"),
        timeout_s=5,
        max_output_bytes=100,
        detached=True,
        platform="win32",
    )

    assert result.pid == 321
    kwargs = opened.call_args.kwargs
    assert kwargs["stdin"] is subprocess.DEVNULL
    assert kwargs["stdout"] is subprocess.DEVNULL
    assert kwargs["stderr"] is subprocess.DEVNULL
    assert kwargs["close_fds"] is True
    assert kwargs["creationflags"] == 0x00000200 | 0x00000008
    assert kwargs["start_new_session"] is False
    assert kwargs["text"] is False


@pytest.mark.anyio
async def test_direct_shell_masks_unexpected_execution_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.discover_shell",
        lambda _kind: "bash",
    )
    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.execute_shell",
        AsyncMock(side_effect=RuntimeError("secret command detail")),
    )
    reply = AsyncMock()

    from untether.telegram.commands.direct_shell import handle_direct_shell_command

    class Runtime:
        def default_context_for_chat(
            self, chat_id: int | str | None
        ) -> RunContext | None:
            return None

        def resolve_run_cwd(self, context: RunContext | None) -> Path | None:
            return None

    await handle_direct_shell_command(
        kind="bash",
        args_text="echo secret",
        runtime=Runtime(),
        context=None,
        chat_id=1,
        timeout_s=1,
        max_output_bytes=1024,
        reply=reply,
    )

    reply.assert_awaited_once_with(text="bash: execution failed")


@pytest.mark.anyio
async def test_direct_shell_uses_normal_authorisation_and_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    cfg.allowed_user_ids = (123,)
    handler = AsyncMock()
    monkeypatch.setattr(
        "untether.telegram.commands.direct_shell.handle_direct_shell_command",
        handler,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        for message_id, sender_id in [(1, 999), (2, 123)]:
            yield TelegramIncomingMessage(
                transport="telegram",
                chat_id=123,
                message_id=message_id,
                text="/bash printf ok",
                reply_to_message_id=None,
                reply_to_text=None,
                sender_id=sender_id,
            )

    await run_main_loop(cfg, poller)

    handler.assert_awaited_once()
    await_args = handler.await_args
    assert await_args is not None
    assert await_args.kwargs["kind"] == "bash"
    assert await_args.kwargs["args_text"] == "printf ok"


def test_format_shell_results_are_plain_bounded_and_explicit() -> None:
    assert (
        format_shell_result(ShellResult(output="", exit_code=0))
        == "(no output)\nexit: 0"
    )
    assert format_shell_result(ShellResult(output="bad", exit_code=2)) == "bad\nexit: 2"
    assert (
        format_shell_result(ShellResult(output="x", exit_code=0, truncated=True))
        == "x\n[output truncated]\nexit: 0"
    )
    assert (
        format_shell_result(ShellResult(output="", exit_code=-9, timed_out=True))
        == "(no output)\n[timeout]\nexit: -9"
    )
    assert (
        format_shell_result(ShellResult(pid=42)) == "started detached process (PID 42)"
    )
