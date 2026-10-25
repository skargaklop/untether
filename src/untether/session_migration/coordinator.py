"""Guarded non-destructive context-copy coordinator; NEVER native migration."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

from .journal import ObservedJournal
from .operation import MigrationOperation, OperationStore
from .package import pack_context
from .remote import receiver_request
from .settings import MigrationPeer, SessionMigrationSettings


class CoordinatorRefusal(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CopyResult:
    operation_id: str
    destination_id: str
    state: str
    mode: str = "context"
    source_unchanged: bool = True


def copy_observed_context(
    *,
    settings: SessionMigrationSettings,
    peer: MigrationPeer,
    source_session_id: str,
    project: str,
    direction: str,
    journal: ObservedJournal,
    operations: OperationStore,
    source_quiescent_and_complete: Callable[[], bool] | None,
    receiver: Callable[..., dict] = receiver_request,
) -> CopyResult:
    """One bounded context seed, no native store copy or Telegram takeover.

    The caller must attest complete **observed** history, no running tools,
    no pending permissions and an immutable snapshot generation. The proof
    has no default; mere availability of in-memory /export is insufficient.
    """
    if not settings.enabled:
        raise CoordinatorRefusal("MIGRATION_DISABLED")
    if project not in peer.projects:
        raise CoordinatorRefusal("PROJECT_UNMAPPED")
    if direction not in ("to", "from"):
        raise CoordinatorRefusal("DIRECTION_INVALID")
    if source_quiescent_and_complete is None or source_quiescent_and_complete() is not True:
        raise CoordinatorRefusal("SOURCE_NOT_QUIESCENT_OR_COMPLETE")
    payload, _digest = journal.snapshot(source_session_id)
    archive = pack_context(
        engine="acp", project=project, content=payload,
        max_bytes=settings.max_archive_bytes,
    )
    operation_id = str(uuid4())
    operations.create(
        MigrationOperation(operation_id, source_session_id, project, "context")
    )
    operations.transition(operation_id, "uploaded")
    try:
        reply = receiver(
            peer,
            project=project,
            direction=direction,
            action="receive",
            operation_id=operation_id,
            package=archive,
            max_package_bytes=settings.max_archive_bytes,
            timeout_s=settings.transfer_timeout_s,
        )
        if reply.get("status") != "verified" or not isinstance(
            reply.get("native_id"), str
        ) or not reply["native_id"]:
            raise CoordinatorRefusal("RECEIVER_UNVERIFIED")
        from hashlib import sha256

        digest = sha256(archive).hexdigest()
        if reply.get("package_sha256") != digest:
            raise CoordinatorRefusal("PACKAGE_HASH_MISMATCH")
        operations.transition(operation_id, "imported")
        operations.transition(
            operation_id, "verified",
            destination_id=reply["native_id"], package_hash=digest,
        )
        return CopyResult(operation_id, reply["native_id"], "verified")
    except Exception:
        # It is never safe to silently retry after a lost receiver response.
        # A context copy leaves the source session usable regardless.
        operations.transition(operation_id, "needs_reconcile")
        raise
