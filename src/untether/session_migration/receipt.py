"""Crash-safe durable receipts for imported operations, independent of deployment mode."""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import UUID

OP_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f-]{27,}$")


class ReceiptError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Receipt:
    operation_id: str
    package_sha256: str
    importer_version: str
    native_id: str
    status: str  # "verified" only after independent resume probe


class ReceiptStore:
    """Write-once receipts. Callers must hold a per-operation exclusive lock."""

    def __init__(self, root: Path):
        self.root = root

    def _path(self, operation_id: str) -> Path:
        try:
            if str(UUID(operation_id)) != operation_id or not OP_ID.fullmatch(
                operation_id
            ):
                raise ValueError
        except ValueError as exc:
            raise ReceiptError("OPERATION_ID_INVALID") from exc
        return self.root / (operation_id + ".json")

    def read(self, operation_id: str) -> Receipt | None:
        path = self._path(operation_id)
        if not path.exists():
            return None
        if path.is_symlink():
            raise ReceiptError("RECEIPT_SYMLINK")
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            raise ReceiptError("RECEIPT_MALFORMED")
        return Receipt(**obj)

    def record(self, receipt: Receipt) -> Receipt:
        if receipt.status != "verified" or not receipt.native_id:
            raise ReceiptError("UNVERIFIED_RECEIPT")
        path = self._path(receipt.operation_id)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.is_symlink():
            raise ReceiptError("RECEIPT_DIRECTORY_SYMLINK")
        previous = self.read(receipt.operation_id)
        if previous:
            if previous != receipt:
                raise ReceiptError("OPERATION_CONFLICT")
            return previous
        encoded = json.dumps(asdict(receipt), sort_keys=True).encode()
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=self.root, prefix=".receipt-", delete=False
            ) as out:
                tmp = Path(out.name)
                os.chmod(tmp, 0o600)
                out.write(encoded)
                out.flush()
                os.fsync(out.fileno())
            try:
                os.link(tmp, path)
            except FileExistsError:
                previous = self.read(receipt.operation_id)
                if previous != receipt:
                    raise ReceiptError("OPERATION_CONFLICT") from None
            finally:
                tmp.unlink(missing_ok=True)
            if os.name == "posix":
                dirfd = os.open(self.root, os.O_RDONLY)
                try:
                    os.fsync(dirfd)
                finally:
                    os.close(dirfd)
        finally:
            if tmp:
                tmp.unlink(missing_ok=True)
        return receipt
