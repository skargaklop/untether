"""Durable pending-move fence for native session sources.

Unlike the transient :mod:`session_gate` — which exists only while a worker
holds it — a fence record is durable and never removed: once a source
``(engine, native_id)`` is fenced it stays tombstoned forever, including in
the ``completed`` state, because a moved source must never be reused. No
deletion, no auto-reaping, no resume, no timers: fail-closed by construction.

Precondition (documented here, enforced later by the move integration):
fence writers must already hold the transient ``session_gate`` for the same
``(engine, native_id)``. This module cannot prove caller context, so it does
not pretend to check it.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from uuid import UUID

__all__ = [
    "ReclaimRefused",
    "SourceFence",
    "SourceFenceConflict",
    "SourceFenceCorrupt",
    "SourceFenceRefused",
    "SourceFenced",
    "activate_source_fence",
    "assert_source_unfenced",
    "complete_source_fence",
    "place_source_fence",
    "reactivate_source_fence",
    "read_source_fence",
]

_MAX_ID_LEN = 128
_HEX_DIGITS = frozenset("0123456789abcdef")
_FENCE_PREFIX = "fence-"
_FENCE_SUFFIX = ".json"
_STATES = frozenset(("pending", "completed", "active"))


class SourceFenceRefused(RuntimeError):
    """A fail-closed precondition failed (bad identity, unsafe root)."""


class ReclaimRefused(RuntimeError):
    """The completed tombstone does not authorize reclaiming this identity."""


class SourceFenceConflict(RuntimeError):
    """An existing durable fence disagrees with this call."""


class SourceFenceCorrupt(RuntimeError):
    """A stored fence record is malformed; refuse rather than trust it."""


class SourceFenced(RuntimeError):
    """The source carries a fence record and must not be moved again."""


@dataclass(frozen=True, slots=True)
class SourceFence:
    """Durable tombstone for one native source identity."""

    engine: str
    native_id: str
    operation_id: str
    package_sha256: str
    state: str
    prior_operation_id: str | None = None


def _check_id(kind: str, value: str) -> str:
    """Bound an identity; it is used only via its digest, never as a path."""
    if not isinstance(value, str):
        raise SourceFenceRefused(f"FENCE_ID_INVALID: {kind} must be str")
    if not value or len(value) > _MAX_ID_LEN:
        raise SourceFenceRefused(f"FENCE_ID_INVALID: {kind} unbounded")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise SourceFenceRefused(f"FENCE_ID_INVALID: {kind} has control chars")
    return value


def _check_operation_id(value: str) -> str:
    _check_id("operation_id", value)
    try:
        if str(UUID(value)) != value:
            raise ValueError(value)
    except ValueError as exc:
        raise SourceFenceRefused("FENCE_ID_INVALID: operation_id not UUID") from exc
    return value


def _check_sha256(value: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or not set(value) <= _HEX_DIGITS:
        raise SourceFenceRefused("FENCE_ID_INVALID: package_sha256")
    return value


def _check_identity(engine: str, native_id: str) -> tuple[str, str]:
    return _check_id("engine", engine), _check_id("native_id", native_id)


def _fence_path(root: Path, engine: str, native_id: str) -> Path:
    # Flat digest filename: arbitrary identity bytes can never traverse or
    # collide across scopes (NUL separator blocks ("ab","c") == ("a","bc")).
    digest = hashlib.sha256(f"{engine}\x00{native_id}".encode()).hexdigest()
    return root / (_FENCE_PREFIX + digest + _FENCE_SUFFIX)


def _prepare_root(root: Path) -> Path:
    if root.is_symlink():
        raise SourceFenceRefused("FENCE_ROOT_SYMLINK")
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():  # re-check: mkdir raced a swap
        raise SourceFenceRefused("FENCE_ROOT_SYMLINK")
    return root


def _validate_record(record: SourceFence) -> SourceFence:
    if record.state not in _STATES:
        raise SourceFenceRefused("FENCE_STATE_INVALID")
    _check_identity(record.engine, record.native_id)
    _check_operation_id(record.operation_id)
    _check_sha256(record.package_sha256)
    if record.prior_operation_id is not None:
        _check_operation_id(record.prior_operation_id)
    if record.state == "active" and record.prior_operation_id is None:
        raise SourceFenceRefused("FENCE_ACTIVE_WITHOUT_PRIOR")
    return record


def _parse(raw: str) -> SourceFence:
    """Strictly decode one stored record; anything off-spec fails closed."""
    try:
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) not in (
            set(SourceFence.__dataclass_fields__),
            set(SourceFence.__dataclass_fields__) - {"prior_operation_id"},
        ):
            raise ValueError("record shape mismatch")
        return _validate_record(
            SourceFence(
                engine=data["engine"],
                native_id=data["native_id"],
                operation_id=data["operation_id"],
                package_sha256=data["package_sha256"],
                state=data["state"],
                prior_operation_id=data.get("prior_operation_id"),
            )
        )
    except (ValueError, TypeError, SourceFenceRefused) as exc:
        raise SourceFenceCorrupt("FENCE_CORRUPT") from exc


def read_source_fence(root: Path, *, engine: str, native_id: str) -> SourceFence | None:
    """Return the durable fence record, or ``None`` when none exists."""
    _check_identity(engine, native_id)
    path = _fence_path(Path(root), engine, native_id)
    if path.is_symlink():
        raise SourceFenceCorrupt("FENCE_SYMLINK")
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SourceFenceCorrupt("FENCE_READ_FAILED") from exc
    record = _parse(raw)
    if (record.engine, record.native_id) != (engine, native_id):
        raise SourceFenceCorrupt("FENCE_IDENTITY_MISMATCH")
    return record


def _accept_existing(
    root: Path,
    *,
    engine: str,
    native_id: str,
    operation_id: str,
    package_sha256: str,
) -> SourceFence:
    existing = read_source_fence(root, engine=engine, native_id=native_id)
    if existing is None:  # vanished between O_EXCL and read: refuse, never re-create
        raise SourceFenceCorrupt("FENCE_VANISHED")
    if (
        existing.operation_id != operation_id
        or existing.package_sha256 != package_sha256
    ):
        raise SourceFenceConflict(
            f"FENCE_CONFLICT: {engine}/{native_id} already fenced by "
            f"operation {existing.operation_id}"
        )
    return existing  # exact replay is idempotent, any state (pending/completed)


def place_source_fence(
    root: Path,
    *,
    engine: str,
    native_id: str,
    operation_id: str,
    package_sha256: str,
) -> SourceFence:
    """Durably fence a source (state ``pending``) or accept an exact replay.

    Precondition: the caller already holds the transient ``session_gate`` for
    the same ``(engine, native_id)``; this module cannot prove that context.

    Creation is an ``O_EXCL`` create inside the explicit ``root``. An existing
    record with the same ``operation_id`` and ``package_sha256`` is returned
    unchanged (idempotent); any other existing record raises
    :class:`SourceFenceConflict`. A failed mid-write leaves a refusing corrupt
    record behind — by design, never a silently reusable source.
    """
    _check_identity(engine, native_id)
    _check_operation_id(operation_id)
    _check_sha256(package_sha256)
    root = _prepare_root(Path(root))
    path = _fence_path(root, engine, native_id)
    if path.is_symlink():
        raise SourceFenceCorrupt("FENCE_SYMLINK")
    record = SourceFence(
        engine=engine,
        native_id=native_id,
        operation_id=operation_id,
        package_sha256=package_sha256,
        state="pending",
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError:
        current = read_source_fence(root, engine=engine, native_id=native_id)
        if current is not None and current.state == "active":
            return _replace_fence(
                root, replace(record, prior_operation_id=current.operation_id)
            )
        return _accept_existing(
            root,
            engine=engine,
            native_id=native_id,
            operation_id=operation_id,
            package_sha256=package_sha256,
        )
    except OSError as exc:
        raise SourceFenceRefused(f"FENCE_OPEN_FAILED: {exc}") from exc
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        out.write(json.dumps(asdict(record), sort_keys=True))
        out.flush()
        os.fsync(out.fileno())
    return record


def complete_source_fence(
    root: Path,
    *,
    engine: str,
    native_id: str,
    operation_id: str,
    package_sha256: str,
) -> SourceFence:
    """Flip the pending fence to ``completed``; the tombstone remains.

    Requires an existing ``pending`` record with exactly matching
    ``operation_id`` and ``package_sha256``; anything else is refused.
    """
    _check_identity(engine, native_id)
    _check_operation_id(operation_id)
    _check_sha256(package_sha256)
    root = _prepare_root(Path(root))
    path = _fence_path(root, engine, native_id)
    if path.is_symlink():
        raise SourceFenceCorrupt("FENCE_SYMLINK")
    current = read_source_fence(root, engine=engine, native_id=native_id)
    if current is None:
        raise SourceFenceRefused("FENCE_MISSING")
    if current.operation_id != operation_id or current.package_sha256 != package_sha256:
        raise SourceFenceConflict("FENCE_CONFLICT: record belongs to another operation")
    if current.state != "pending":
        raise SourceFenceConflict("FENCE_CONFLICT: already completed (no resume)")
    completed = replace(current, state="completed")
    fd, tmp_name = tempfile.mkstemp(dir=root, prefix=".fence-", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            out.write(json.dumps(asdict(completed), sort_keys=True))
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return completed


def reactivate_source_fence(
    root: Path,
    *,
    engine: str,
    native_id: str,
    prior_operation_id: str,
    operation_id: str,
    package_sha256: str,
    prior_record: object,
    prior_peer: str | None = None,
    prior_project: str | None = None,
    prior_lineage: str | None = None,
) -> SourceFence:
    """Rotate one ``completed`` tombstone to ``pending`` for a new inbound op.

    Reclaim is authorized only when every fact ties the old move to this
    inbound transfer: the fence is ``completed`` and owned by
    ``prior_operation_id`` with its recorded digest; the prior transfer
    record is a completed outbound move (``direction == "to"``) for the same
    engine/native identity and the same peer/project; the inbound operation
    is a new canonical UUID; the new package digest is exactly 64 hex.
    Anything else refuses without touching the stored record. Replaying the
    same inbound operation is idempotent; a different operation conflicts.
    """
    _check_identity(engine, native_id)
    _check_operation_id(prior_operation_id)
    _check_operation_id(operation_id)
    _check_sha256(package_sha256)
    root = _prepare_root(Path(root))
    path = _fence_path(root, engine, native_id)
    if path.is_symlink():
        raise SourceFenceCorrupt("FENCE_SYMLINK")
    current = read_source_fence(root, engine=engine, native_id=native_id)
    if current is None:
        raise ReclaimRefused("FENCE_MISSING")
    record = prior_record
    if record is None:
        raise ReclaimRefused("RECLAIM_REFUSED: missing prior transfer record")
    for attr in ("engine", "session_id", "direction", "peer", "project", "state"):
        if not hasattr(record, attr):
            raise ReclaimRefused("RECLAIM_REFUSED: prior record malformed")

    def _str(candidate: object, attr: str) -> str:
        value = getattr(candidate, attr, None)
        return value if isinstance(value, str) else ""

    def _prior_identifies_move(candidate: object) -> bool:
        return (
            _str(candidate, "engine") == engine
            and _str(candidate, "session_id") == native_id
            and _str(candidate, "direction") == "to"
            and _str(candidate, "state") == "completed"
        )

    if getattr(record, "operation_id", None) != prior_operation_id:
        raise ReclaimRefused("RECLAIM_REFUSED: prior operation mismatch")
    if not _prior_identifies_move(record):
        raise ReclaimRefused("RECLAIM_REFUSED: prior record is not this move")
    if prior_peer is not None and _str(record, "peer") != prior_peer:
        raise ReclaimRefused("RECLAIM_REFUSED: foreign peer")
    if prior_project is not None and _str(record, "project") != prior_project:
        raise ReclaimRefused("RECLAIM_REFUSED: foreign project")
    if prior_lineage is not None and _str(record, "lineage") != prior_lineage:
        raise ReclaimRefused("DIVERGENT_LINEAGE")
    if current.state == "pending":
        if (
            current.operation_id == operation_id
            and current.package_sha256 == package_sha256
            and current.prior_operation_id == prior_operation_id
        ):
            return current  # idempotent replay of the same inbound reclaim
        if _prior_identifies_move(record):
            # Prior record still identifies this exact completed move, so the
            # pending fence belongs to an earlier rotate of THIS reclaim path:
            # a different inbound operation must conflict, never steal it.
            raise SourceFenceConflict(
                "FENCE_CONFLICT: reclaim pending under another inbound operation"
            )
        raise ReclaimRefused(
            "FENCE_PENDING: pending fence is never reclaimed by another op"
        )
    if current.state != "completed":
        raise ReclaimRefused(f"FENCE_{current.state.upper()}: only completed reclaims")
    if current.operation_id != prior_operation_id:
        raise ReclaimRefused("RECLAIM_REFUSED: fence owned by another operation")
    rebound = replace(
        current,
        operation_id=operation_id,
        package_sha256=package_sha256,
        state="pending",
        prior_operation_id=prior_operation_id,
    )
    if current.package_sha256 != getattr(
        record, "receipt_sha256", current.package_sha256
    ):
        raise ReclaimRefused("RECLAIM_REFUSED: completed digest mismatch")
    fd, tmp_name = tempfile.mkstemp(dir=root, prefix=".fence-", suffix=".tmp")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            out.write(json.dumps(asdict(rebound), sort_keys=True))
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return rebound


def _replace_fence(root: Path, record: SourceFence) -> SourceFence:
    path = _fence_path(root, record.engine, record.native_id)
    fd, name = tempfile.mkstemp(dir=root, prefix=".fence-", suffix=".tmp")
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(record), sort_keys=True))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return record


def activate_source_fence(
    root: Path, *, engine: str, native_id: str, operation_id: str, package_sha256: str
) -> SourceFence:
    """Publish verified local ownership, retaining the inbound/prior evidence."""
    current = _accept_existing(
        root,
        engine=engine,
        native_id=native_id,
        operation_id=operation_id,
        package_sha256=package_sha256,
    )
    if current.state not in ("pending", "active") or not current.prior_operation_id:
        raise SourceFenceConflict("FENCE_NOT_RECLAIM")
    return _replace_fence(root, replace(current, state="active"))


def assert_source_unfenced(root: Path, *, engine: str, native_id: str) -> None:
    """Admit only absent fences or explicitly verified active local ownership.

    Pending and completed outbound evidence continues to block runners.
    """
    existing = read_source_fence(root, engine=engine, native_id=native_id)
    if existing is not None and existing.state != "active":
        raise SourceFenced(
            f"source fenced: {engine}/{native_id} "
            f"(operation {existing.operation_id}, state {existing.state})"
        )
