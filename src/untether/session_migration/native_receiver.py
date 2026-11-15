"""Fail-closed receiver of exact native snapshots (no ACP transcript seeding)."""

from __future__ import annotations

import hashlib
import os
from contextlib import ExitStack, nullcontext
from pathlib import Path
from uuid import UUID

from .intent import OperationConflict, OperationIntentStore
from .native_adapter import (
    CapabilityUnavailable,
    assert_exact_restore,
    require_persistence_adapter,
)
from .native_archive import FidelityError, decode_native, encode_native
from .receipt import Receipt, ReceiptStore
from .session_gate import migration_lock, session_gate
from .source_fence import (
    complete_source_fence,
    place_source_fence,
    read_source_fence,
)


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
        self.intents = OperationIntentStore(root / "intents")
        self.max_archive = max_archive
        self.max_expanded = max_expanded
        self.importer_version = importer_version

    def status_receipt(self, operation_id: str) -> Receipt | None:
        receipt = self.receipts.read(operation_id)
        if receipt is None:
            return None
        from . import transfer

        record = transfer._read(self.root / "operations", operation_id)
        if record is not None and record.direction == "from":
            gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
            if not gate_root:
                return None
            fence = read_source_fence(
                Path(gate_root) / "fences",
                engine=record.engine,
                native_id=receipt.native_id,
            )
            if (
                fence is None
                or fence.state != "active"
                or fence.operation_id != operation_id
                or fence.package_sha256 != receipt.package_sha256
            ):
                return None
        return receipt

    def receive(
        self,
        operation_id: str,
        content: bytes,
        *,
        engine: str,
        peer_name: str | None = None,
        project: str | None = None,
    ) -> Receipt:
        try:
            if str(UUID(operation_id)) != operation_id:
                raise ValueError
        except ValueError as exc:
            raise NativeReceiverError("OPERATION_ID_INVALID") from exc
        digest = hashlib.sha256(content).hexdigest()
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
        from . import transfer

        gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        lock = (
            migration_lock(
                root=Path(gate_root), engine_id=engine, native_id=archive.native_id
            )
            if gate_root
            else nullcontext()
        )
        with lock:
            ops = self.root / "operations"
            record = transfer._read(ops, operation_id)
            if record is not None and (
                record.peer != peer_name
                or record.project != project
                or record.identity_sha256 != transfer._identity_sha256(archive)
            ):
                raise NativeReceiverError("OPERATION_CONFLICT")
            previous = self.receipts.read(operation_id)
            if previous is not None:
                if previous.package_sha256 != digest:
                    raise NativeReceiverError("OPERATION_CONFLICT")
                if record is not None:
                    transfer._finalize_reclaim(archive, record, digest)
                return previous
            try:
                self.intents.begin(operation_id, digest)
            except OperationConflict as exc:
                fence = (
                    read_source_fence(
                        Path(gate_root) / "fences",
                        engine=engine,
                        native_id=archive.native_id,
                    )
                    if gate_root
                    else None
                )
                intent = self.intents.inspect(operation_id)
                if not (
                    peer_name
                    and project
                    and fence
                    and fence.state == "pending"
                    and fence.operation_id == operation_id
                    and fence.package_sha256 == digest
                    and fence.prior_operation_id
                    and intent
                    and intent.get("sha256") == digest
                ):
                    raise NativeReceiverError(str(exc)) from exc
            try:
                proof = adapter.import_exact(archive)
                native_id = assert_exact_restore(archive, proof)
            except CapabilityUnavailable as exc:
                if str(exc) != "SESSION_ID_COLLISION":
                    raise
                if not peer_name or not project:
                    raise NativeReceiverError("RECLAIM_CONTEXT_REQUIRED") from exc
                native_id, _ = transfer._reclaim_import(
                    importer=adapter,
                    archive=archive,
                    op_id=operation_id,
                    digest=digest,
                    peer_name=peer_name,
                    project=project,
                    ops=ops,
                )
                create = record is None
                record = transfer.TransferRecord(
                    operation_id=operation_id,
                    engine=engine,
                    session_id=archive.native_id,
                    direction="from",
                    peer=peer_name,
                    project=project,
                    state="verified",
                    identity_sha256=transfer._identity_sha256(archive),
                    native_id=native_id,
                    generation=archive.generation,
                    lineage=archive.lineage,
                    receipt_sha256=digest,
                    destination_id=native_id,
                )
                transfer._write(ops, record, create=create)
            receipt = self.receipts.record(
                Receipt(
                    operation_id, digest, self.importer_version, native_id, "verified"
                )
            )
            if record is not None:
                transfer._finalize_reclaim(archive, record, digest)
            return receipt

    def export_exact(
        self,
        session_id: str,
        *,
        engine: str,
        operation_id: str | None = None,
        peer_name: str | None = None,
        project: str | None = None,
    ) -> bytes:
        if not session_id or len(session_id) > 128:
            raise NativeReceiverError("SESSION_ID_INVALID")
        if operation_id is None:
            adapter = require_persistence_adapter(engine)
            return self._encode_export(
                adapter.export_exact(session_id), session_id, engine
            )
        try:
            if str(UUID(operation_id)) != operation_id:
                raise ValueError
        except ValueError as exc:
            raise NativeReceiverError("OPERATION_ID_INVALID") from exc
        gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not gate_root:
            raise NativeReceiverError("SOURCE_GATE_UNCONFIGURED")
        fences = Path(gate_root) / "fences"
        cache = self.root / "exports" / f"{operation_id}.bundle"
        fence = read_source_fence(fences, engine=engine, native_id=session_id)
        if fence is not None and fence.state != "active":
            if fence.operation_id != operation_id:
                raise NativeReceiverError("SOURCE_FENCE_CONFLICT")
            if cache.is_symlink():
                raise NativeReceiverError("EXPORT_CACHE_UNSAFE")
            with cache.open("rb") as stream:
                package = stream.read(self.max_archive + 1)
            if (
                len(package) > self.max_archive
                or hashlib.sha256(package).hexdigest() != fence.package_sha256
            ):
                raise NativeReceiverError("EXPORT_CACHE_CORRUPT")
            archive = decode_native(
                package, max_archive=self.max_archive, max_expanded=self.max_expanded
            )
            if archive.engine != engine or archive.logical_id != session_id:
                raise NativeReceiverError("EXPORT_CACHE_IDENTITY_MISMATCH")
            native_fence = read_source_fence(
                fences, engine=engine, native_id=archive.native_id
            )
            if (
                native_fence is None
                or native_fence.operation_id != operation_id
                or native_fence.package_sha256 != fence.package_sha256
            ):
                raise NativeReceiverError("EXPORT_CACHE_FENCE_MISMATCH")
            self._export_evidence(archive, package, operation_id, peer_name, project)
            return package
        adapter = require_persistence_adapter(engine)
        snapshot = getattr(adapter, "export_snapshot", None)
        if not callable(snapshot):
            raise NativeReceiverError("SOURCE_SNAPSHOT_UNAVAILABLE")
        with ExitStack() as stack:
            archive = stack.enter_context(snapshot(session_id))
            if archive.native_id != session_id:
                stack.enter_context(
                    session_gate(
                        root=Path(gate_root),
                        engine_id=engine,
                        native_id=archive.native_id,
                    )
                )
            package = self._encode_export(archive, session_id, engine)
            self._export_evidence(archive, package, operation_id, peer_name, project)
            cache.parent.mkdir(parents=True, exist_ok=True)
            # Exclusive cache creation refuses an ambiguous crash before fencing.
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            fd = os.open(cache, flags, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(package)
                stream.flush()
                os.fsync(stream.fileno())
            if os.name == "posix":
                directory_fd = os.open(cache.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            for native_id in dict.fromkeys((session_id, archive.native_id)):
                place_source_fence(
                    fences,
                    engine=engine,
                    native_id=native_id,
                    operation_id=operation_id,
                    package_sha256=hashlib.sha256(package).hexdigest(),
                )
            return package

    def complete_export(
        self,
        operation_id: str,
        *,
        engine: str,
        session_id: str,
        package_sha256: str,
        peer_name: str | None = None,
        project: str | None = None,
    ) -> dict[str, str]:
        """Flip BOTH durable export fences to ``completed`` (tombstones stay).

        Requires the fenced cached bundle: canonical UUID operation id, a
        bounded non-symlink cache read that decodes with matching engine and
        logical session identity, a digest exactly equal to the claimed
        ``package_sha256``, and BOTH the logical and actual-native fences
        matching this operation and digest. Completing an already-completed
        exact replay is idempotent; every refusal mutates nothing.
        """
        if not session_id or len(session_id) > 128:
            raise NativeReceiverError("SESSION_ID_INVALID")
        try:
            if str(UUID(operation_id)) != operation_id:
                raise ValueError
        except ValueError as exc:
            raise NativeReceiverError("OPERATION_ID_INVALID") from exc
        gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not gate_root:
            raise NativeReceiverError("SOURCE_GATE_UNCONFIGURED")
        fences = Path(gate_root) / "fences"
        cache = self.root / "exports" / f"{operation_id}.bundle"
        if cache.is_symlink():
            raise NativeReceiverError("EXPORT_CACHE_UNSAFE")
        if not cache.exists():
            raise NativeReceiverError("EXPORT_CACHE_MISSING")
        with cache.open("rb") as stream:
            package = stream.read(self.max_archive + 1)
        if (
            len(package) > self.max_archive
            or hashlib.sha256(package).hexdigest() != package_sha256
        ):
            raise NativeReceiverError("EXPORT_CACHE_CORRUPT")
        try:
            archive = decode_native(
                package, max_archive=self.max_archive, max_expanded=self.max_expanded
            )
        except FidelityError as exc:
            raise NativeReceiverError("EXPORT_CACHE_CORRUPT") from exc
        if archive.engine != engine or archive.logical_id != session_id:
            raise NativeReceiverError("EXPORT_CACHE_IDENTITY_MISMATCH")
        pending = []
        for native_id in dict.fromkeys((session_id, archive.native_id)):
            fence = read_source_fence(fences, engine=engine, native_id=native_id)
            if (
                fence is None
                or fence.operation_id != operation_id
                or fence.package_sha256 != package_sha256
                or fence.state not in ("pending", "completed")
            ):
                raise NativeReceiverError("EXPORT_FENCE_MISMATCH")
            if fence.state == "pending":
                pending.append(fence)
        record = self._export_evidence(
            archive, package, operation_id, peer_name, project
        )
        for fence in pending:
            complete_source_fence(
                fences,
                engine=fence.engine,
                native_id=fence.native_id,
                operation_id=operation_id,
                package_sha256=package_sha256,
            )
        if record is not None and record.state != "completed":
            from . import transfer

            transfer._write(
                self.root / "operations",
                record.model_copy(update={"state": "completed"}),
                create=False,
            )
        return {
            "operation_id": operation_id,
            "engine": engine,
            "session_id": session_id,
            "package_sha256": package_sha256,
            "status": "export_completed",
        }

    def _export_evidence(self, archive, package, operation_id, peer_name, project):
        """Only operator-bound context can create outbound reclaim authority."""
        from . import transfer

        ops = self.root / "operations"
        existing = transfer._read(ops, operation_id)
        if not peer_name or not project:
            if existing is not None:
                raise NativeReceiverError("RECLAIM_CONTEXT_REQUIRED")
            return None
        digest = hashlib.sha256(package).hexdigest()
        record = transfer.TransferRecord(
            operation_id=operation_id,
            engine=archive.engine,
            session_id=archive.native_id,
            direction="to",
            peer=peer_name,
            project=project,
            state="prepared",
            identity_sha256=transfer._identity_sha256(archive),
            native_id=archive.native_id,
            generation=archive.generation,
            lineage=archive.lineage,
            receipt_sha256=digest,
            destination_id=archive.native_id,
        )
        if existing is not None:
            if existing.model_copy(update={"state": "prepared"}) != record:
                raise NativeReceiverError("EXPORT_EVIDENCE_CONFLICT")
            transfer._cached_package(ops, operation_id, digest, self.max_archive)
            return existing
        transfer._cache_package(ops, operation_id, digest, package, self.max_archive)
        transfer._write(ops, record, create=True)
        return record

    def _encode_export(self, archive, session_id: str, engine: str) -> bytes:
        if archive.engine != engine:
            raise NativeReceiverError("ENGINE_MISMATCH")
        if not (archive.logical_id and archive.native_id and archive.lineage):
            raise NativeReceiverError("IDENTITY_INVALID")
        if archive.logical_id != session_id:
            raise NativeReceiverError("SESSION_IDENTITY_MISMATCH")
        return encode_native(
            engine=archive.engine,
            logical_id=archive.logical_id,
            native_id=archive.native_id,
            generation=archive.generation,
            lineage=archive.lineage,
            records=archive.records,
            max_bytes=self.max_archive,
        )
