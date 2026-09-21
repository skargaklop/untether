from __future__ import annotations

import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

import anyio
from anyio import EndOfStream

from ...config import ConfigError
from ...context import RunContext
from ...utils.paths import get_run_base_dir
from ...utils.subprocess import manage_subprocess

type ShellKind = Literal["bash", "powershell"]


class ShellRuntime(Protocol):
    def default_context_for_chat(
        self, chat_id: int | str | None
    ) -> RunContext | None: ...

    def resolve_run_cwd(self, context: RunContext | None) -> Path | None: ...


_TELEGRAM_TEXT_LIMIT = 4096
_RESULT_RESERVE = 64


class DirectShellError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ShellResult:
    output: str = ""
    exit_code: int | None = None
    timed_out: bool = False
    truncated: bool = False
    pid: int | None = None


def parse_shell_command(args_text: str) -> tuple[bool, str]:
    command = args_text.strip()
    if not command:
        raise DirectShellError("command is required")
    head, separator, tail = command.partition(" ")
    if head == "nohup":
        command = tail.strip() if separator else ""
        if not command:
            raise DirectShellError("command is required after nohup")
        return True, command
    return False, command


def discover_shell(kind: ShellKind, *, platform: str | None = None) -> str:
    platform = sys.platform if platform is None else platform
    if kind == "powershell":
        if platform != "win32":
            raise DirectShellError("/powershell is Windows only")
        executable = shutil.which("pwsh") or shutil.which("powershell")
        if executable is None:
            raise DirectShellError("PowerShell was not found on PATH")
        return executable
    executable = shutil.which("bash")
    if executable is None:
        raise DirectShellError("bash was not found on PATH")
    return executable


def _normalise_windows_paths(command: str) -> str:
    """Make Windows drive paths safe for Bash without rewriting other escapes."""
    result: list[str] = []
    index = 0
    quote: str | None = None
    while index < len(command):
        char = command[index]
        if char in {"'", '"'}:
            quote = None if quote == char else char if quote is None else quote
        is_drive_path = (
            char.isascii()
            and char.isalpha()
            and index + 2 < len(command)
            and command[index + 1] == ":"
            and command[index + 2] == "\\"
            and (
                index == 0
                or command[index - 1].isspace()
                or command[index - 1] in "'=(["
            )
        )
        if not is_drive_path:
            result.append(char)
            index += 1
            continue

        result.extend((char, ":"))
        index += 2
        while index < len(command):
            char = command[index]
            if (quote and char == quote) or (
                quote is None and (char.isspace() or char in "|&;<>()")
            ):
                break
            if char == "\\":
                result.append("/")
                while index + 1 < len(command) and command[index + 1] == "\\":
                    index += 1
            else:
                result.append(char)
            index += 1
    return "".join(result)


def build_shell_argv(
    kind: ShellKind,
    executable: str,
    command: str,
    *,
    platform: str | None = None,
) -> list[str]:
    platform = sys.platform if platform is None else platform
    if kind == "bash":
        if platform == "win32":
            command = _normalise_windows_paths(command)
        return [executable, "-lc", command]
    return [
        executable,
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        command,
    ]


def resolve_shell_cwd(
    runtime: ShellRuntime,
    context: RunContext | None,
    chat_id: int,
) -> Path | None:
    effective_context = context or runtime.default_context_for_chat(chat_id)
    return runtime.resolve_run_cwd(effective_context) or get_run_base_dir()


async def _read_bounded(
    stream,
    max_output_bytes: int,
    output: bytearray,
    truncated: list[bool],
) -> None:
    while True:
        try:
            chunk = await stream.receive(64 * 1024)
        except EndOfStream:
            return
        remaining = max_output_bytes - len(output)
        if remaining > 0:
            output.extend(chunk[:remaining])
        if len(chunk) > remaining:
            truncated[0] = True


def _detached_process(
    argv: list[str], cwd: Path | None, *, platform: str
) -> subprocess.Popen[bytes]:
    creationflags = 0
    start_new_session = False
    if platform == "win32":
        creationflags = 0x00000200 | 0x00000008
    else:
        start_new_session = True
    return subprocess.Popen(  # nosec B603
        argv,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        creationflags=creationflags,
        start_new_session=start_new_session,
        text=False,
    )


async def execute_shell(
    argv: list[str],
    *,
    cwd: Path | None,
    timeout_s: float,
    max_output_bytes: int,
    detached: bool = False,
    platform: str | None = None,
) -> ShellResult:
    platform = sys.platform if platform is None else platform
    if detached:
        proc = _detached_process(argv, cwd, platform=platform)
        return ShellResult(pid=proc.pid)

    output = bytearray()
    truncated = [False]
    timed_out = False
    exit_code: int | None = None
    async with manage_subprocess(
        argv,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        reap_orphans=True,
    ) as proc:
        assert proc.stdout is not None
        with anyio.move_on_after(timeout_s) as timeout_scope:
            await _read_bounded(
                proc.stdout,
                max_output_bytes,
                output,
                truncated,
            )
            exit_code = await proc.wait()
        timed_out = timeout_scope.cancel_called
    if timed_out:
        exit_code = proc.returncode
    return ShellResult(
        output=bytes(output).decode("utf-8", errors="replace"),
        exit_code=exit_code,
        timed_out=timed_out,
        truncated=truncated[0],
    )


def format_shell_result(result: ShellResult) -> str:
    if result.pid is not None:
        return f"started detached process (PID {result.pid})"
    output = result.output or "(no output)"
    suffix: list[str] = []
    if result.truncated:
        suffix.append("[output truncated]")
    if result.timed_out:
        suffix.append("[timeout]")
    suffix.append(f"exit: {result.exit_code}")
    ending = "\n" + "\n".join(suffix)
    max_output_chars = _TELEGRAM_TEXT_LIMIT - max(_RESULT_RESERVE, len(ending))
    if len(output) > max_output_chars:
        output = output[:max_output_chars]
        if not result.truncated:
            ending = "\n[output truncated]" + ending
    return output + ending


async def handle_direct_shell_command(
    *,
    kind: ShellKind,
    args_text: str,
    runtime: ShellRuntime,
    context: RunContext | None,
    chat_id: int,
    timeout_s: float,
    max_output_bytes: int,
    reply,
) -> None:
    try:
        detached, command = parse_shell_command(args_text)
        executable = discover_shell(kind)
        argv = build_shell_argv(kind, executable, command)
        cwd = resolve_shell_cwd(runtime, context, chat_id)
        result = await execute_shell(
            argv,
            cwd=cwd,
            timeout_s=timeout_s,
            max_output_bytes=max_output_bytes,
            detached=detached,
        )
        await reply(text=format_shell_result(result))
    except (ConfigError, DirectShellError, OSError) as exc:
        await reply(text=f"{kind}: {exc}")
    except Exception:  # noqa: BLE001 - command failures must not stop the bot loop
        await reply(text=f"{kind}: execution failed")
