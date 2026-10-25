"""Write-once operation intent guards against lost receipts and duplicate imports."""
from __future__ import annotations

import json
import os
from pathlib import Path

from .receipt import ReceiptError, ReceiptStore


class OperationConflict(ReceiptError):
    pass


class OperationIntentStore:
    def __init__(self, root: Path):
        self.root = root
        self._receipts = ReceiptStore(root)

    def begin(self, operation_id: str, package_sha256: str) -> None:
        """Atomically reserve before irreversible import. Never override ambiguous state."""
        safe_id = self._receipts._path(operation_id).stem
        if len(package_sha256) != 64 or any(c not in "0123456789abcdef" for c in package_sha256):
            raise OperationConflict("PACKAGE_HASH_INVALID")
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.root.is_symlink():
            raise OperationConflict("INTENT_ROOT_SYMLINK")
        path = self.root / (safe_id + ".intent")
        payload = json.dumps(
            {"operation_id": safe_id, "sha256": package_sha256, "state": "importing"},
            sort_keys=True,
        ).encode()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(path, flags, 0o600)
        except FileExistsError as exc:
            existing = self.inspect(operation_id)
            if existing and existing.get("sha256") != package_sha256:
                raise OperationConflict("OPERATION_CONFLICT") from exc
            raise OperationConflict("NEEDS_RECONCILE: previous import may have completed") from exc
        with os.fdopen(fd, "wb") as out:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())
        if os.name == "posix":
            directory = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def inspect(self, operation_id: str) -> dict[str, str] | None:
        path = self.root / (self._receipts._path(operation_id).stem + ".intent")
        if path.is_symlink():
            raise OperationConflict("INTENT_SYMLINK")
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("state") != "importing":
            raise OperationConflict("INTENT_INVALID")
        return data
