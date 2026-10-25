"""Bounded receive contract, same for direct Linux and container deployments."""
from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

from .intent import OperationIntentStore
from .package import ContextPackage, PackageError, unpack_context
from .receipt import Receipt, ReceiptStore


class ReceiverRefusal(RuntimeError):
    pass


class StagingReceiver:
    """No native overwrite; publish only after a proven restart/resume probe."""

    def __init__(
        self, root: Path, *, max_archive: int, max_expanded: int,
        min_free_bytes: int, importer_version: str = "1",
    ):
        self.root = root
        self.max_archive = max_archive
        self.max_expanded = max_expanded
        self.min_free_bytes = min_free_bytes
        self.importer_version = importer_version
        self.receipts = ReceiptStore(root / "receipts")
        self.intents = OperationIntentStore(root / "intents")

    def receive(
        self, operation_id: str, archive: bytes, *,
        expected_project: str,
        import_and_probe: Callable[[ContextPackage], str] | None = None,
    ) -> Receipt:
        try:
            if str(UUID(operation_id)) != operation_id:
                raise ValueError
        except ValueError as exc:
            raise ReceiverRefusal("OPERATION_ID_INVALID") from exc
        if len(archive) > self.max_archive:
            raise ReceiverRefusal("ARCHIVE_TOO_LARGE")
        digest = hashlib.sha256(archive).hexdigest()
        previous = self.receipts.read(operation_id)
        if previous:
            if previous.package_sha256 != digest:
                raise ReceiverRefusal("OPERATION_CONFLICT")
            return previous
        if import_and_probe is None:
            raise ReceiverRefusal("ADAPTER_UNAVAILABLE")
        try:
            package = unpack_context(
                archive, max_archive=self.max_archive,
                max_expanded=self.max_expanded,
            )
        except PackageError as exc:
            raise ReceiverRefusal(str(exc)) from exc
        if package.project != expected_project:
            raise ReceiverRefusal("PROJECT_MISMATCH")
        if self.root.is_symlink():
            raise ReceiverRefusal("ROOT_SYMLINK")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.stat().st_mode & 0o077 and os.name == "posix":
            raise ReceiverRefusal("ROOT_PERMISSIONS")
        if shutil.disk_usage(self.root).free < len(archive) + self.min_free_bytes:
            raise ReceiverRefusal("DISK_HEADROOM")
        stage = self.root / "staging"
        stage.mkdir(mode=0o700, exist_ok=True)
        if stage.is_symlink():
            raise ReceiverRefusal("STAGING_SYMLINK")
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=stage, prefix=".stage-", delete=False,
        ) as out:
            staged = Path(out.name)
            os.chmod(staged, 0o600)
            out.write(archive)
            out.flush()
            os.fsync(out.fileno())
        try:
            # Persist ambiguous state BEFORE calling a potentially irreversible adapter.
            self.intents.begin(operation_id, digest)
            native_id = import_and_probe(package)
            if not isinstance(native_id, str) or not native_id:
                raise ReceiverRefusal("RESUME_PROBE_FAILED")
            return self.receipts.record(
                Receipt(operation_id, digest, self.importer_version, native_id, "verified")
            )
        finally:
            staged.unlink(missing_ok=True)
