from __future__ import annotations

from pathlib import Path

from ...backends import EngineBackend, EngineConfig
from .runner import AcpRunner


class _BackendRunner(AcpRunner):
    def compact(self, resume, instructions=None):
        return self.run(instructions or "compact", resume)


def build_acp_runner(config: EngineConfig, project_dir: Path) -> _BackendRunner:
    command = str(config.get("command", "acp-agent"))
    args = [str(value) for value in config.get("args", [])]
    return _BackendRunner(
        engine=str(config.get("engine", "acp")),
        command=command,
        args=args,
        cwd=str(project_dir),
        env=config.get("env"),
        protocol=str(config.get("protocol", "auto")),
        turn_timeout_s=float(config.get("turn_timeout_s", 1800.0)),
        request_timeout_s=float(config.get("request_timeout_s", 60.0)),
    )


def acp_backend(engine: str, config: EngineConfig) -> EngineBackend:
    merged = dict(config)
    merged["engine"] = engine
    return EngineBackend(
        id=engine,
        build_runner=build_acp_runner,
        cli_cmd=str(config.get("command", "acp-agent")),
    )


BACKEND = acp_backend("acp", {})
