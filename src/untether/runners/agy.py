"""Antigravity CLI (`agy`) runner — plain-text headless (`-p`) mode."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio

from ..backends import EngineBackend, EngineConfig
from ..config import ConfigError
from ..events import EventFactory
from ..logging import get_logger, log_pipeline
from ..model import CompletedEvent, EngineId, ResumeToken, UntetherEvent
from ..runner import BaseRunner, ResumeTokenMixin, Runner
from ..utils.paths import get_run_base_dir
from ..utils.streams import iter_bytes_lines
from ..utils.subprocess import manage_subprocess
from ..utils.transient_failures import (
    classify_transient_failure,
    format_transient_failure,
)
from ._compact_mixin import HandoffCompactMixin
from .modes import run_modes
from .run_options import get_run_options

logger = get_logger(__name__)

ENGINE: EngineId = "agy"

_TOKEN_PATTERN = r"(?P<token>[^`\s]+)"
# Native: --conversation. Alias: resume / --resume / -r (universal across agents).
# Note: -c is --continue (most-recent), NOT conversation id.
_RESUME_RE = re.compile(
    rf"(?im)^\s*`?agy\s+(?:--conversation(?:=|\s+)|(?:resume|--resume|-r)\s+)"
    rf"{_TOKEN_PATTERN}`?(?:\s|$)"
)
_RESUME_LINE_RE = re.compile(
    rf"(?im)^\s*`?agy\s+(?:--conversation(?:=|\s+)|(?:resume|--resume|-r)\s+)"
    rf"{_TOKEN_PATTERN}`?\s*$"
)

# Best-effort session id scrape from logs / exit banners.
_UUID_RE = re.compile(
    r"(?i)(?:created\s+conversation|conversation(?:\s+id)?|--conversation)\s*[=:]?\s*"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)
_BARE_UUID_RE = re.compile(
    r"(?i)\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b"
)


@dataclass(slots=True)
class AgyStreamState:
    resume: ResumeToken
    allow_id_promotion: bool = False
    factory: EventFactory = field(default_factory=lambda: EventFactory(ENGINE))
    stderr_tail: list[str] = field(default_factory=list)
    started: bool = False


def parse_conversation_id(text: str | None) -> str | None:
    if not text:
        return None
    match = _UUID_RE.search(text)
    if match:
        return match.group(1)
    # Prefer resume-line style last.
    for match in _RESUME_RE.finditer(text):
        token = match.group("token")
        if token:
            return token
    bare = list(_BARE_UUID_RE.finditer(text))
    if bare:
        return bare[-1].group(1)
    return None


@dataclass(slots=True)
class AgyRunner(HandoffCompactMixin, ResumeTokenMixin, BaseRunner):
    engine: EngineId = ENGINE
    resume_re: re.Pattern[str] = field(default=_RESUME_RE, repr=False)
    agy_cmd: str = "agy"
    model: str | None = None
    yolo: bool = True
    sandbox: bool = False
    mode: str | None = None
    extra_args: list[str] = field(default_factory=list)
    session_title: str = "agy"
    logger: Any = field(default=logger, repr=False)
    compact_handoff_note: str = "Antigravity handoff summary only; not real compaction"

    def format_resume(self, token: ResumeToken) -> str:
        if token.engine != ENGINE:
            raise RuntimeError(f"resume token is for engine {token.engine!r}")
        return f"`agy --conversation {token.value}`"

    def is_resume_line(self, line: str) -> bool:
        return bool(_RESUME_LINE_RE.match(line))

    def command(self) -> str:
        return self.agy_cmd

    def build_args(
        self,
        prompt: str,
        resume: ResumeToken | None,
    ) -> list[str]:
        run_options = get_run_options()
        plan, goal = run_modes(run_options)
        # Goal is not a native agy mode — soft-prefix the condition into the prompt.
        if goal is not None:
            body = prompt.strip()
            note = f"(autonomous goal — work until: {goal})"
            prompt = f"{note}\n\n{body}" if body else note
        args: list[str] = [*self.extra_args]

        model = self.model
        if run_options is not None and run_options.model:
            model = run_options.model
        if model is not None:
            args.extend(["--model", str(model)])

        mode = "plan" if plan else self.mode
        if mode is not None:
            args.extend(["--mode", str(mode)])

        if self.sandbox is True:
            args.append("--sandbox")

        if plan:
            # Plan mode must not auto-approve destructive tools.
            pass
        elif self.yolo is True:
            args.append("--dangerously-skip-permissions")

        if resume is not None:
            args.extend(["--conversation", resume.value])

        # `-p` / prompt last (headless adapters and agy docs agree).
        args.extend(["-p", prompt])
        return args

    def env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.setdefault("NO_COLOR", "1")
        env.setdefault("CI", "1")
        return env

    def new_state(self, prompt: str, resume: ResumeToken | None) -> AgyStreamState:
        if resume is not None:
            token = resume
            if token.engine != ENGINE:
                token = ResumeToken(engine=ENGINE, value=resume.value)
            return AgyStreamState(resume=token, allow_id_promotion=False)
        return AgyStreamState(
            resume=ResumeToken(engine=ENGINE, value=str(uuid4())),
            allow_id_promotion=True,
        )

    def _maybe_promote(self, state: AgyStreamState, candidate: str | None) -> None:
        if not candidate or not state.allow_id_promotion:
            return
        if candidate == state.resume.value:
            return
        state.resume = ResumeToken(engine=ENGINE, value=candidate)
        state.allow_id_promotion = False

    async def _collect_stdout(self, stdout: Any) -> str:
        chunks: list[bytes] = []
        async for line in iter_bytes_lines(stdout):
            chunks.append(line)
            if not line.endswith(b"\n"):
                chunks.append(b"\n")
        # Also try trailing incomplete read handled by iter_bytes_lines
        raw = b"".join(chunks)
        return raw.decode("utf-8", errors="replace").strip()

    async def _drain_stderr_capture(
        self,
        stream: Any,
        state: AgyStreamState,
        tag: str,
    ) -> None:
        # Positional-only args: anyio.TaskGroup.start_soon does not accept kwargs.
        try:
            async for line in iter_bytes_lines(stream):
                text = line.decode("utf-8", errors="replace")
                state.stderr_tail.append(text.rstrip("\n"))
                if len(state.stderr_tail) > 200:
                    state.stderr_tail = state.stderr_tail[-200:]
                log_pipeline(
                    self.logger,
                    "subprocess.stderr",
                    tag=tag,
                    line=text,
                )
                scraped = parse_conversation_id(text)
                if scraped:
                    self._maybe_promote(state, scraped)
        except Exception as exc:  # noqa: BLE001
            log_pipeline(
                self.logger,
                "subprocess.stderr.error",
                tag=tag,
                error=str(exc),
            )

    async def run_impl(
        self, prompt: str, resume: ResumeToken | None
    ) -> AsyncIterator[UntetherEvent]:
        state = self.new_state(prompt, resume)
        # Emit Started early for session lock (BaseRunner acquires on StartedEvent).
        yield state.factory.started(
            state.resume,
            title=self.session_title,
            meta={"cwd": os.getcwd(), **({"model": self.model} if self.model else {})},
        )
        state.started = True

        cmd = [self.command(), *self.build_args(prompt, resume)]
        cwd = get_run_base_dir()
        env = self.env()

        self.logger.info(
            "runner.start",
            engine=self.engine,
            resume=resume.value if resume else None,
            prompt=prompt,
            prompt_len=len(prompt),
        )

        async with manage_subprocess(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=cwd,
        ) as proc:
            if proc.stdout is None or proc.stderr is None:
                raise RuntimeError("agy subprocess missing stdout/stderr pipes")
            if proc.stdin is not None:
                await proc.stdin.aclose()

            self.logger.info(
                "subprocess.spawn",
                cmd=cmd[0] if cmd else None,
                args=cmd[1:],
                pid=proc.pid,
            )

            answer = ""
            async with anyio.create_task_group() as tg:
                tg.start_soon(
                    self._drain_stderr_capture,
                    proc.stderr,
                    state,
                    str(self.engine),
                )
                answer = await self._collect_stdout(proc.stdout)
                rc = await proc.wait()

            self.logger.info("subprocess.exit", pid=proc.pid, rc=rc)

            # Scrape id from combined stderr + stdout (resume banners).
            combined = "\n".join(state.stderr_tail) + "\n" + answer
            self._maybe_promote(state, parse_conversation_id(combined))

            ok = rc == 0
            error = None if ok else f"agy failed (rc={rc})."
            if not ok and not answer and state.stderr_tail:
                answer = "\n".join(state.stderr_tail[-20:])

            # Classify transient upstream failures from stderr for a clean
            # user-facing message. Agy emits StartedEvent early (session lock),
            # so it is intentionally never auto-retried: the side-effect
            # boundary was already crossed.
            if not ok:
                stderr_text = "\n".join(state.stderr_tail)
                failure = classify_transient_failure(stderr_text)
                if failure is not None:
                    error = format_transient_failure(ENGINE, failure)
                    # Do not surface the raw stderr blob as the answer.
                    if not answer.strip():
                        answer = ""

            completed = CompletedEvent(
                engine=ENGINE,
                ok=ok,
                answer=answer,
                resume=state.resume,
                error=error,
            )
            yield completed


def _default_agy_cmd() -> str:
    env_cmd = os.environ.get("AGY_CLI_BIN") or os.environ.get("ANTIGRAVITY_CLI_BIN")
    if env_cmd:
        return env_cmd
    found = shutil.which("agy")
    if found:
        return found
    # Official Windows installer default location (often missing from PATH
    # until the shell is restarted).
    local_app = os.environ.get("LOCALAPPDATA")
    if local_app:
        candidate = Path(local_app) / "agy" / "bin" / "agy.exe"
        if candidate.is_file():
            return str(candidate)
    return "agy"


def build_runner(config: EngineConfig, config_path: Path) -> Runner:
    agy_cmd = _default_agy_cmd()

    model = config.get("model")
    if model is not None and not isinstance(model, str):
        raise ConfigError(f"Invalid `agy.model` in {config_path}; expected a string.")

    yolo = True if "yolo" not in config else config.get("yolo") is True
    if "dangerously_skip_permissions" in config:
        yolo = config.get("dangerously_skip_permissions") is True

    sandbox = config.get("sandbox") is True

    mode = config.get("mode")
    if mode is not None and not isinstance(mode, str):
        raise ConfigError(f"Invalid `agy.mode` in {config_path}; expected a string.")

    extra_args_value = config.get("extra_args")
    if extra_args_value is None:
        extra_args: list[str] = []
    elif isinstance(extra_args_value, list) and all(
        isinstance(x, str) for x in extra_args_value
    ):
        extra_args = list(extra_args_value)
    else:
        raise ConfigError(
            f"Invalid `agy.extra_args` in {config_path}; expected a list of strings."
        )

    cmd_override = config.get("cmd")
    if isinstance(cmd_override, str) and cmd_override.strip():
        agy_cmd = cmd_override.strip()

    title = str(model) if model is not None else "agy"

    return AgyRunner(
        agy_cmd=agy_cmd,
        model=model,
        yolo=yolo,
        sandbox=sandbox,
        mode=mode,
        extra_args=extra_args,
        session_title=title,
    )


BACKEND = EngineBackend(
    id="agy",
    build_runner=build_runner,
    cli_cmd="agy",
    install_cmd="irm https://antigravity.google/cli/install.ps1 | iex",
)
