"""Reclaim helpers for descendant pulls; coordinator dependencies stay explicit."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from .native_adapter import (
    CapabilityUnavailable,
    PersistenceAdapter,
    assert_restored,
    require_persistence_adapter,
)
from .native_archive import FidelityError, NativeArchive, decode_native
from .source_fence import (
    ReclaimRefused,
    SourceFenceConflict,
    reactivate_source_fence,
    read_source_fence,
)

if TYPE_CHECKING:
    from .transfer import TransferRecord


def _prior_pushed_archive(
    ops: Path,
    prior_op_id: str,
    archive: NativeArchive,
    *,
    read_record: Callable[[Path, str], TransferRecord | None],
    cached_package: Callable[[Path, str, str, int], bytes],
) -> NativeArchive | None:
    """Exact prior pushed archive from the push operation's durable cache."""
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
    return prior_archive


def _restore_matches(
    adapter: PersistenceAdapter, archive: NativeArchive, stored: NativeArchive
) -> bool:
    """Exact-or-adapter-verified store comparison shared with the coordinator."""
    try:
        assert_restored(adapter, archive, stored)
    except CapabilityUnavailable:
        return False
    return True


def _preoverwrite_matches(
    importer: PersistenceAdapter,
    archive: NativeArchive,
    prior_archive: NativeArchive,
    current: NativeArchive,
) -> bool:
    """May the pending reclaim overwrite ``current``?

    Three states are admissible: the untouched prior pushed state, the fully
    restored incoming state, or a partially applied incoming restore whose
    journal landed before its identity metadata did (a crashed publication the
    pending coordinator fence still keeps closed). The last case pins the
    prior generation exactly; an adapter may additionally declare an
    engine-specific recovery hook that reconstructs the expected transitional
    archive from prior metadata plus the incoming journal. Without such an
    explicit declaration nothing is relaxed.
    """
    if _restore_matches(importer, prior_archive, current):
        return True
    if _restore_matches(importer, archive, current):
        return True
    if current.generation != prior_archive.generation:
        return False
    stale = replace(archive, generation=prior_archive.generation)
    if _restore_matches(importer, stale, current):
        return True
    recoverable = getattr(importer, "recoverable_restore", None)
    if callable(recoverable):
        return bool(recoverable(archive, prior_archive, current))
    return False


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
    prior_archive = _prior_pushed_archive(
        ops, prior_op, archive, read_record=read_record, cached_package=cached_package
    )
    if prior_archive is None:
        raise refusal("RECLAIM_REFUSED: pushed package unavailable")
    if (
        prior_archive.engine != prior_record.engine
        or prior_archive.logical_id != archive.logical_id
        or prior_archive.generation != prior_record.generation
        or prior_archive.lineage != prior_record.lineage
    ):
        raise refusal("RECLAIM_REFUSED: pushed package identity mismatch")
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
        matches_incoming = _restore_matches(importer, archive, current)
        if file_retry or not matches_incoming:
            if not _preoverwrite_matches(importer, archive, prior_archive, current):
                raise refusal("RECLAIM_REFUSED: divergent local identity")
            overwrite(
                engine=archive.engine,
                native_id=native_id,
                records=archive.records,
                expected_prior_records=prior_archive.records,
                generation=archive.generation,
                lineage=archive.lineage,
            )
    except (ReclaimRefused, SourceFenceConflict) as exc:
        raise refusal(f"RECLAIM_REFUSED: {exc}") from exc
    adapter = require_persistence_adapter(archive.engine)
    reloaded = _read_imported(adapter, native_id)
    if not _restore_matches(adapter, archive, reloaded):
        raise refusal("RECLAIM_OVERWRITE_UNVERIFIED")
    return native_id, reloaded


def _finalize_reclaim(
    archive: NativeArchive,
    record: TransferRecord,
    digest: str,
    *,
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
    adapter = require_persistence_adapter(archive.engine)
    reloaded = _read_imported(adapter, archive.native_id)
    if not _restore_matches(adapter, archive, reloaded):
        raise refusal("RESTORE_IDENTITY_MISMATCH")
    activate_fence(
        fences,
        engine=archive.engine,
        native_id=archive.native_id,
        operation_id=record.operation_id,
        package_sha256=digest,
    )
