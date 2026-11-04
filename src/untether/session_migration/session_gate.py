"""Cross-process session gate: one live worker per (engine_id, native_id).

Fail-closed by construction: the gate is an ``O_EXCL`` create inside an
explicit root, so a crashed holder leaves a stale gate file that refuses
every later acquire until an operator removes it. No PID liveness probing,
no wait loops, no timeout-based takeover — a busy gate raises
:class:`SourceBusy` immediately, which is also what makes the acquire safe
to call from an async event loop: it is a couple of quick syscalls with no
blocking wait.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .settings import SessionMigrationSettings


def configure_session_gate(settings: SessionMigrationSettings) -> None:
    """Apply the explicit migration gate policy before constructing runners."""
    if not settings.enabled or not settings.persistence.engines:
        return
    configured = settings.gate_root
    existing = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
    if not configured and not existing:
        raise GateRefused("SOURCE_GATE_UNCONFIGURED")
    if (
        configured
        and existing
        and Path(configured).resolve() != Path(existing).resolve()
    ):
        raise GateRefused("SOURCE_GATE_ROOT_CONFLICT")
    if configured:
        os.environ["UNTETHER_SESSION_GATE_ROOT"] = configured


_GATE_PREFIX = "gate-"
_GATE_SUFFIX = ".lock"
_HELD: ContextVar[tuple] = ContextVar("migration_locks", default=())


def assert_migration_lock_owned(*, root: Path, engine_id: str, native_id: str) -> None:
    """Validate a live lock owned by this execution context, not mere file presence."""
    path = _gate_path(Path(root), engine_id, native_id).absolute()
    for owned_path, fd, identity in _HELD.get():
        if owned_path != path:
            continue
        try:
            current = path.lstat()
            opened = os.fstat(fd)
        except OSError:
            break
        if (
            not path.is_symlink()
            and (current.st_dev, current.st_ino)
            == (opened.st_dev, opened.st_ino)
            == identity
        ):
            return
    raise GateRefused("MIGRATION_LOCK_NOT_OWNED")


class SourceBusy(RuntimeError):
    """A live process (or un-reaped stale gate) already holds this session."""


class GateRefused(RuntimeError):
    """A fail-closed precondition failed (symlinked root, empty identity)."""


def _gate_path(root: Path, engine_id: str, native_id: str) -> Path:
    # Scoped hashing keeps every identity on a safe flat filename: arbitrary
    # engine_id/native_id bytes can never traverse or collide across scopes
    # (NUL separator prevents ("ab", "c") == ("a", "bc") digest aliases).
    digest = hashlib.sha256(f"{engine_id}\x00{native_id}".encode()).hexdigest()[:32]
    return root / (_GATE_PREFIX + digest + _GATE_SUFFIX)


@contextmanager
def session_gate(*, root: Path, engine_id: str, native_id: str) -> Iterator[Path]:
    """Runner admission: exclusion plus durable ownership policy."""
    with migration_lock(root=root, engine_id=engine_id, native_id=native_id) as path:
        from .source_fence import assert_source_unfenced

        assert_source_unfenced(
            Path(root) / "fences", engine=engine_id, native_id=native_id
        )
        yield path


@contextmanager
def migration_lock(*, root: Path, engine_id: str, native_id: str) -> Iterator[Path]:
    """Hold the exclusive gate for ``(engine_id, native_id)`` under ``root``.

    Non-blocking: raises :class:`SourceBusy` when the gate is taken, releases
    (closes and unlinks) only this gate's own file on exit — including when
    the body raises. A stale gate from a crashed holder stays on disk and
    keeps refusing acquires (fail-closed); it never auto-expires.
    """
    if not engine_id or not native_id:
        raise GateRefused("GATE_ID_EMPTY")
    root = Path(root)
    if root.is_symlink():
        raise GateRefused("GATE_ROOT_SYMLINK")
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():  # re-check: mkdir raced a symlink swap in
        raise GateRefused("GATE_ROOT_SYMLINK")
    path = _gate_path(root, engine_id, native_id)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise SourceBusy(
            f"session gate busy: {engine_id}/{native_id} "
            f"(stale gate file must be removed manually: {path})"
        ) from exc
    except OSError as exc:
        raise GateRefused(f"GATE_OPEN_FAILED: {exc}") from exc
    identity = os.fstat(fd)
    token = _HELD.set(
        (*_HELD.get(), (path.absolute(), fd, (identity.st_dev, identity.st_ino)))
    )
    try:
        with contextlib.suppress(OSError):
            # Diagnostics only — never used for liveness decisions.
            os.write(fd, f"pid={os.getpid()}\n".encode())
        yield path
    finally:
        _HELD.reset(token)
        os.close(fd)
        with contextlib.suppress(OSError):
            current = path.lstat()
            # Never remove a replacement gate created after an operator moved
            # our entry. The configured directory must remain owner-controlled.
            if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                os.unlink(path)
