"""Conservative source-ownership FSM; no runtime admission integration claimed."""

from __future__ import annotations

from uuid import uuid4

import pytest

from untether.session_migration.operation import (
    MigrationOperation,
    OperationError,
    OperationStore,
)


def test_native_move_requires_receipt_and_enrolled_binding(tmp_path):
    store = OperationStore(tmp_path / "state")
    op = str(uuid4())
    store.create(MigrationOperation(op, "source", "demo", "native"))
    assert store.source_blocked(op)
    store.transition(op, "uploaded", package_hash="a" * 64)
    store.transition(op, "imported")
    with pytest.raises(OperationError, match="RECEIPT_REQUIRED"):
        store.transition(op, "verified")
    store.transition(op, "verified", destination_id="new-native")
    with pytest.raises(OperationError, match="DESTINATION_ENROLMENT_REQUIRED"):
        store.transition(op, "activated")
    store.transition(op, "activated", destination_binding="enrolled")
    current = store.read(op)
    assert current is not None
    assert current.state == "activated"
    assert store.source_blocked(op)


def test_context_copy_keeps_source_usable(tmp_path):
    store = OperationStore(tmp_path)
    op = str(uuid4())
    store.create(MigrationOperation(op, "source", "demo", "context"))
    assert not store.source_blocked(op)


def test_ambiguous_native_move_never_unfences_implicitly(tmp_path):
    store = OperationStore(tmp_path)
    op = str(uuid4())
    store.create(MigrationOperation(op, "source", "demo", "native"))
    store.transition(op, "needs_reconcile")
    with pytest.raises(OperationError, match="ILLEGAL_TRANSITION"):
        store.transition(op, "activated", destination_id="unproven")
    assert store.source_blocked(op)


def test_path_traversal_refused(tmp_path):
    store = OperationStore(tmp_path)
    with pytest.raises(OperationError, match="OPERATION_ID_INVALID"):
        store.create(MigrationOperation("../../fake", "src", "demo", "native"))


def test_orphaned_lock_requires_manual_reconciliation(tmp_path):
    store = OperationStore(tmp_path)
    op = str(uuid4())
    store.create(MigrationOperation(op, "source", "demo", "native"))
    (tmp_path / (op + ".lock")).write_bytes(b"interrupted transaction")
    with pytest.raises(OperationError, match="NEEDS_RECONCILE"):
        store.transition(op, "uploaded")
    current = store.read(op)
    assert current is not None
    assert current.state == "prepared"
    assert store.source_blocked(op)
