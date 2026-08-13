"""Optional, capability-gated ACP client facilities."""

from __future__ import annotations

import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio

from .interactions import InteractionBroker, PendingInteraction


class RootFilesystem:
    def __init__(self, roots: list[str | Path]) -> None:
        self.roots = tuple(Path(root).resolve() for root in roots)

    def _path(self, value: str) -> Path:
        path = Path(value)
        if not path.is_absolute() and self.roots:
            path = self.roots[0] / path
        resolved = path.resolve()
        if not any(resolved == root or root in resolved.parents for root in self.roots):
            raise PermissionError("path is outside configured ACP roots")
        return resolved

    def read_text(self, path: str) -> str:
        return self._path(path).read_text(encoding="utf-8")

    def write_text(self, path: str, text: str) -> None:
        target = self._path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(temporary)
            raise


@dataclass(slots=True)
class TerminalResult:
    output: str
    returncode: int


class TerminalExecutor:
    def __init__(self, roots: list[str | Path], *, max_output: int = 64_000) -> None:
        self.roots = tuple(Path(root).resolve() for root in roots)
        self.max_output = max_output

    def _cwd(self, cwd: str | Path) -> Path:
        path = Path(cwd)
        if not path.is_absolute() and self.roots:
            path = self.roots[0] / path
        resolved = path.resolve()
        if not any(resolved == root or root in resolved.parents for root in self.roots):
            raise PermissionError("cwd is outside configured ACP roots")
        return resolved

    async def run(self, argv: list[str], *, cwd: str | Path) -> TerminalResult:
        if not isinstance(argv, list) or not all(
            isinstance(item, str) for item in argv
        ):
            raise TypeError("terminal argv must be a list of strings")
        if not argv:
            raise ValueError("terminal argv cannot be empty")
        process = await anyio.open_process(
            argv, cwd=self._cwd(cwd), stdout=-1, stderr=-2
        )
        stdout = (
            await process.stdout.receive(self.max_output + 1) if process.stdout else b""
        )
        await process.wait()
        return TerminalResult(
            stdout[: self.max_output].decode(errors="replace"), process.returncode or 0
        )


@dataclass(slots=True)
class AcpClientFacilities:
    filesystem: RootFilesystem | None = None
    terminal: TerminalExecutor | None = None
    broker: InteractionBroker | None = None
    elicitation: bool = False

    def capabilities(self, protocol_version: int) -> dict[str, Any]:
        if protocol_version != 1:
            return {}
        result: dict[str, Any] = {}
        if self.filesystem is not None:
            result["fs"] = {"readTextFile": True, "writeTextFile": True}
        if self.terminal is not None:
            result["terminal"] = True
        if self.elicitation and self.broker is not None:
            result["elicitation"] = {"form": True, "url": True}
        return result

    async def elicit(self, owner: str, payload: dict[str, Any]) -> PendingInteraction:
        if not self.elicitation or self.broker is None:
            raise RuntimeError("ACP elicitation is disabled")
        return await self.broker.open(owner, "elicitation", payload)
