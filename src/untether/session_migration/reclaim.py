"""Reclaim helpers for descendant pulls; coordinator dependencies stay explicit."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from .native_adapter import PersistenceAdapter, require_persistence_adapter
from .native_archive import FidelityError, NativeArchive, decode_native
from .source_fence import (
    ReclaimRefused,
    SourceFenceConflict,
    reactivate_source_fence,
    read_source_fence,
)

if TYPE_CHECKING:
    from .transfer import TransferRecord


def _prior_pushed_records(
    ops: Path,
    prior_op_id: str,
    archive: NativeArchive,
    *,
    read_record: Callable[[Path, str], TransferRecord | None],
    cached_package: Callable[[Path, str, str, int], bytes],
) -> dict[str, bytes] | None:
    """Exact prior records from the push operation's durable package cache."""
    record = read_record(ops, prior_op_id)
    if record is None:
        return None
    cached = cached_package(ops, prior_op_id, record.receipt_sha256 or "", 1 << 24)
    if cached is None:
        return None
    try:
        prior_archive = decode_native(cached, max_archive=1 << 24, max_expanded=1 << 24)
    except FidelityError:
        return None
    if prior_archive.native_id != archive.native_id:
        return None
    return prior_archive.records


def _read_imported(importer: PersistenceAdapter, native_id: str) -> NativeArchive:
    """File stores validate caller lock ownership; executable stores keep their API."""
    read = getattr(importer, "export_locked", None)
    return read(native_id) if callable(read) else importer.export_exact(native_id)


def _reclaim_import(
    *,
    importer: PersistenceAdapter,
    archive: NativeArchive,
    op_id: str,
    digest: str,
    peer_name: str,
    project: str,
    ops: Path,
    read_record: Callable[[Path, str], TransferRecord | None],
    cached_package: Callable[[Path, str, str, int], bytes],
    identity_sha256: Callable[[NativeArchive], str],
    refusal: type[Exception],
) -> tuple[str, NativeArchive]:
    """Reclaim a previously pushed local identity for a descendant pull.

    When the local store already holds this session because it was pushed to
    this peer earlier (``SESSION_ID_COLLISION``), a verified descendant inbound
    archive may reclaim it: the completed source fence owned by the prior
    outbound move is rotated to this inbound operation, the stored record is
    atomically replaced under that admission, and the fence completes again.
    Anything short of the full prior-move evidence chain refuses closed.
    """
    gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
    if not gate_root:
        raise refusal("RECLAIM_GATE_UNCONFIGURED")
    fences = Path(gate_root) / "fences"
    native_id = archive.native_id
    fence = read_source_fence(fences, engine=archive.engine, native_id=native_id)
    if fence is None or fence.state not in ("completed", "pending"):
        raise refusal("RECLAIM_REFUSED: no completed fence for this identity")
    prior_op = fence.operation_id
    if fence.state == "pending":
        if fence.operation_id != op_id or fence.package_sha256 != digest:
            raise refusal("RECLAIM_REFUSED: foreign pending owner")
        prior_op = fence.prior_operation_id
        if prior_op is None:
            raise refusal("RECLAIM_REFUSED: missing prior linkage")
    prior_record = read_record(ops, prior_op)
    if prior_record is None or prior_record.direction != "to":
        raise refusal("RECLAIM_REFUSED: fence not owned by an outbound move")
    if prior_record.engine != archive.engine or prior_record.session_id != native_id:
        raise refusal("RECLAIM_REFUSED: prior record identity mismatch")
    if prior_record.peer != peer_name or prior_record.project != project:
        raise refusal("RECLAIM_REFUSED: foreign peer or project")
    if (
        archive.lineage != prior_record.lineage
        or (prior_record.generation is None)
        or archive.generation < prior_record.generation
    ):
        raise refusal("DIVERGENT_LINEAGE: not a descendant of the moved session")
    overwrite = getattr(importer, "import_overwrite", None)
    if not callable(overwrite):
        raise refusal("RECLAIM_ADAPTER_UNSUPPORTED")
    prior_records = _prior_pushed_records(
        ops, prior_op, archive, read_record=read_record, cached_package=cached_package
    )
    if prior_records is None:
        raise refusal("RECLAIM_REFUSED: pushed package unavailable")
    try:
        reactivate_source_fence(
            fences,
            engine=archive.engine,
            native_id=native_id,
            prior_operation_id=prior_op,
            operation_id=op_id,
            package_sha256=digest,
            prior_record=prior_record,
            prior_peer=prior_record.peer,
            prior_project=prior_record.project,
            prior_lineage=prior_record.lineage,
        )
        current = _read_imported(importer, native_id)
        file_retry = callable(getattr(importer, "export_locked", None))
        if file_retry or identity_sha256(current) != identity_sha256(archive):
            if (
                identity_sha256(current) != prior_record.identity_sha256
                and identity_sha256(current) != identity_sha256(archive)
                and not (
                    file_retry
                    and current.records == archive.records
                    and current.lineage == archive.lineage
                    and current.generation == prior_record.generation
                )
            ):
                raise refusal("RECLAIM_REFUSED: divergent local identity")
            overwrite(
                engine=archive.engine,
                native_id=native_id,
                records=archive.records,
                expected_prior_records=prior_records,
                generation=archive.generation,
                lineage=archive.lineage,
            )
    except (ReclaimRefused, SourceFenceConflict) as exc:
        raise refusal(f"RECLAIM_REFUSED: {exc}") from exc
    reloaded = _read_imported(require_persistence_adapter(archive.engine), native_id)
    if identity_sha256(reloaded) != identity_sha256(archive):
        raise refusal("RECLAIM_OVERWRITE_UNVERIFIED")
    return native_id, reloaded


def _finalize_reclaim(
    archive: NativeArchive,
    record: TransferRecord,
    digest: str,
    *,
    identity_sha256: Callable[[NativeArchive], str],
    refusal: type[Exception],
    activate_fence: Callable[..., object],
) -> None:
    gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
    if not gate_root:
        return
    fences = Path(gate_root) / "fences"
    fence = read_source_fence(
        fences, engine=archive.engine, native_id=archive.native_id
    )
    if fence is None or fence.state == "active":
        return
    if record.state not in ("verified", "bound") or record.receipt_sha256 != digest:
        raise refusal("RECLAIM_NOT_VERIFIED")
    if fence.operation_id != record.operation_id or fence.package_sha256 != digest:
        raise refusal("RECLAIM_OWNER_MISMATCH")
    reloaded = _read_imported(
        require_persistence_adapter(archive.engine), archive.native_id
    )
    if identity_sha256(reloaded) != identity_sha256(archive):
        raise refusal("RESTORE_IDENTITY_MISMATCH")
    activate_fence(
        fences,
        engine=archive.engine,
        native_id=archive.native_id,
        operation_id=record.operation_id,
        package_sha256=digest,
    )
