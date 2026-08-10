from __future__ import annotations

from contextvars import ContextVar, Token
from pathlib import Path

_run_base_dir: ContextVar[Path | None] = ContextVar(
    "untether_run_base_dir", default=None
)
_run_channel_id: ContextVar[int | None] = ContextVar(
    "untether_run_channel_id", default=None
)


def get_run_base_dir() -> Path | None:
    return _run_base_dir.get()


def set_run_base_dir(base_dir: Path | None) -> Token[Path | None]:
    return _run_base_dir.set(base_dir)


def reset_run_base_dir(token: Token[Path | None]) -> None:
    _run_base_dir.reset(token)


def get_run_channel_id() -> int | None:
    return _run_channel_id.get()


def set_run_channel_id(channel_id: int | None) -> Token[int | None]:
    return _run_channel_id.set(channel_id)


def reset_run_channel_id(token: Token[int | None]) -> None:
    _run_channel_id.reset(token)


def relativize_path(value: str, *, base_dir: Path | None = None) -> str:
    if not value:
        return value
    base = get_run_base_dir() if base_dir is None else base_dir
    if base is None:
        base = Path.cwd()
    # Compare both spellings because user-facing commands may mix POSIX and
    # native separators even on Windows.
    value_norm = value.replace("\\", "/")
    base_norm = str(base).replace("\\", "/").rstrip("/")
    if not base_norm:
        return value_norm
    if value_norm == base_norm:
        return "."
    prefix = f"{base_norm}/"
    if value_norm.startswith(prefix):
        return value_norm[len(prefix) :] or "."
    return value_norm


def relativize_command(value: str, *, base_dir: Path | None = None) -> str:
    base = get_run_base_dir() if base_dir is None else base_dir
    if base is None:
        base = Path.cwd()
    base_norm = str(base).replace("\\", "/").rstrip("/")
    if not base_norm:
        return value.replace("\\", "/")
    return value.replace("\\", "/").replace(f"{base_norm}/", "")


