"""Fail-closed receiver of exact native snapshots (no ACP transcript seeding)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID

from .native_adapter import assert_exact_restore, require_persistence_adapter
from .native_archive import FidelityError, decode_native
from .receipt import Receipt, ReceiptStore


class NativeReceiverError(RuntimeError):
    pass


class NativeReceiver:
    def __init__(
        self,
        root: Path,
        *,
        max_archive: int,
        max_expanded: int,
        importer_version: str = "native-v1",
    ):
        self.root = root
        self.receipts = ReceiptStore(root / "receipts")
        self.max_archive = max_archive
        self.max_expanded = max_expanded
        self.importer_version = importer_version

    def receive(self, operation_id: str, content: bytes, *, engine: str) -> Receipt:
        try:
            if str(UUID(operation_id)) != operation_id:
                raise ValueError
        except ValueError as exc:
            raise NativeReceiverError("OPERATION_ID_INVALID") from exc
        digest = hashlib.sha256(content).hexdigest()
        previous = self.receipts.read(operation_id)
        if previous is not None:
            if previous.package_sha256 != digest:
                raise NativeReceiverError("OPERATION_CONFLICT")
            return previous
        try:
            archive = decode_native(
                content,
                max_archive=self.max_archive,
                max_expanded=self.max_expanded,
            )
        except FidelityError as exc:
            raise NativeReceiverError("ARCHIVE_UNSAFE") from exc
        if archive.engine != engine:
            raise NativeReceiverError("ENGINE_MISMATCH")
        # The concrete native adapter MUST be installed, bound to this engine,
        # and implement external persistence with restart/resume proof.
        adapter = require_persistence_adapter(engine)
        # Do not publish any success receipt until ALL content is verifiably restored.
        proof = adapter.import_exact(archive)
        native_id = assert_exact_restore(archive, proof)
        return self.receipts.record(
            Receipt(operation_id, digest, self.importer_version, native_id, "verified")
        )
