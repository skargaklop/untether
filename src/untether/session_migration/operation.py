"""Persistent explicit ownership intent; no silent active-owner switch.

Actual runner admission and Telegram chat binding MUST call these guards,
which are not yet connected in the published runtime.
"""
from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from collections.abc import Iterator
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from uuid import UUID


class OperationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class MigrationOperation:
    operation_id: str
    source_id: str
    project: str
    mode: str
    state: str = "prepared"
    package_hash: str | None = None
    destination_id: str | None = None
    destination_binding: str | None = None


_ALLOWED = {
    "prepared": frozenset(("uploaded", "refused", "needs_reconcile")),
    "uploaded": frozenset(("imported", "refused", "needs_reconcile")),
    "imported": frozenset(("verified", "needs_reconcile")),
    "verified": frozenset(("activated", "needs_reconcile")),
    "activated": frozenset(),
    "needs_reconcile": frozenset(),
    "refused": frozenset(),
}


class OperationStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, operation_id: str) -> Path:
        try:
            if str(UUID(operation_id)) != operation_id:
                raise ValueError
        except ValueError as exc:
            raise OperationError("OPERATION_ID_INVALID") from exc
        return self.root / (operation_id + ".json")

    def read(self, operation_id: str) -> MigrationOperation | None:
        path = self._path(operation_id)
        if path.is_symlink():
            raise OperationError("OPERATION_SYMLINK")
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            value = MigrationOperation(**data)
        except (OSError, ValueError, TypeError) as exc:
            raise OperationError("OPERATION_CORRUPT") from exc
        if value.operation_id != operation_id or value.state not in _ALLOWED:
            raise OperationError("OPERATION_CORRUPT")
        return value

    def _write(self, operation: MigrationOperation, *, create: bool) -> None:
        if self.root.is_symlink():
            raise OperationError("OPERATION_DIR_SYMLINK")
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        path = self._path(operation.operation_id)
        if path.is_symlink():
            raise OperationError("OPERATION_SYMLINK")
        with tempfile.NamedTemporaryFile(
            dir=self.root, mode="w", encoding="utf-8", delete=False,
            prefix=".operation-",
        ) as out:
            tmp = Path(out.name)
            os.chmod(tmp, 0o600)
            json.dump(asdict(operation), out, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        try:
            if create:
                os.link(tmp, path)
            else:
                os.replace(tmp, path)
        except FileExistsError as exc:
            raise OperationError("OPERATION_ALREADY_EXISTS") from exc
        finally:
            tmp.unlink(missing_ok=True)

    def create(self, operation: MigrationOperation) -> MigrationOperation:
        if (operation.mode not in ("context", "native")
            or operation.state != "prepared"
            or not operation.source_id or not operation.project):
            raise OperationError("OPERATION_INVALID")
        self._write(operation, create=True)
        return operation

    @contextmanager
    def _exclusive(self, operation_id: str) -> Iterator[None]:
        if self.root.is_symlink() or not self.root.is_dir():
            raise OperationError("OPERATION_DIR_UNSAFE")
        lock = self._path(operation_id).with_suffix(".lock")
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(lock, flags, 0o600)
        except FileExistsError as exc:
            raise OperationError("NEEDS_RECONCILE: operation lock exists") from exc
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(b"locked\\n")
                out.flush()
                os.fsync(out.fileno())
            yield
        finally:
            # An abrupt process crash leaves the lock in place: do not
            # automatically clear it on retry without explicit reconciliation.
            lock.unlink(missing_ok=True)

    def transition(
        self, operation_id: str, state: str, *,
        package_hash: str | None = None,
        destination_id: str | None = None,
        destination_binding: str | None = None,
    ) -> MigrationOperation:
        with self._exclusive(operation_id):
            previous = self.read(operation_id)
            if previous is None:
                raise OperationError("OPERATION_MISSING")
            if state not in _ALLOWED[previous.state]:
                raise OperationError("ILLEGAL_TRANSITION")
            if state in ("verified", "activated"):
                if not (destination_id or previous.destination_id):
                    raise OperationError("RECEIPT_REQUIRED")
                if not (package_hash or previous.package_hash):
                    raise OperationError("PACKAGE_HASH_REQUIRED")
            if state == "activated" and not (destination_binding or previous.destination_binding):
                raise OperationError("DESTINATION_ENROLMENT_REQUIRED")
            updated = replace(
                previous, state=state,
                package_hash=package_hash or previous.package_hash,
                destination_id=destination_id or previous.destination_id,
                destination_binding=destination_binding or previous.destination_binding,
            )
            self._write(updated, create=False)
            return updated

    def source_blocked(self, operation_id: str) -> bool:
        current = self.read(operation_id)
        return current is not None and (
            current.mode == "native" and current.state not in ("refused",)
        )
