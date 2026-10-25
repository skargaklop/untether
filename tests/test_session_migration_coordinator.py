"""Coordinator isolates all data locally and does not contact SSH hosts."""

from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from untether.session_migration.coordinator import (
    CoordinatorRefusal,
    copy_observed_context,
)
from untether.session_migration.journal import ObservedJournal
from untether.session_migration.operation import OperationStore
from untether.session_migration.settings import (
    MigrationPeer,
    SessionMigrationSettings,
)

PIN = "SHA256:" + "A" * 43


def peer():
    return MigrationPeer(
        host="test.invalid",
        user="operator",
        identity_dir="fake",
        identity_file="selected",
        known_hosts_file="known",
        host_key_fingerprint=PIN,
        receiver_path="/bin/receiver",
        expected_endpoint_id="test",
        destination_binding="enrolled",
        projects={"demo": "/srv/demo"},
    )


@pytest.mark.parametrize("direction", ["to", "from"])
def test_context_copy_only_after_verified_receipt(tmp_path, direction):
    journal = ObservedJournal(tmp_path / "journal")
    ops = OperationStore(tmp_path / "operations")
    sid = str(uuid4())
    journal.append(sid, role="user", text="history\r\nline")
    journal.append(sid, role="assistant", text="reply")
    calls = []

    def receiver(_profile, **kwargs):
        calls.append(kwargs)
        return {
            "status": "verified",
            "native_id": "new-native-id",
            "package_sha256": hashlib.sha256(kwargs["package"]).hexdigest(),
        }

    result = copy_observed_context(
        settings=SessionMigrationSettings(enabled=True, peers={"remote": peer()}),
        peer=peer(),
        source_session_id=sid,
        project="demo",
        direction=direction,
        journal=journal,
        operations=ops,
        source_quiescent_and_complete=lambda: True,
        receiver=receiver,
    )
    assert result.state == "verified"
    assert result.source_unchanged
    assert result.mode == "context"
    assert calls[0]["direction"] == direction
    assert ops.read(result.operation_id).state == "verified"
    assert not ops.source_blocked(result.operation_id)


def test_rejects_unproven_source_before_any_write(tmp_path):
    journal = ObservedJournal(tmp_path / "journal")
    ops = OperationStore(tmp_path / "ops")
    sid = str(uuid4())
    with pytest.raises(CoordinatorRefusal, match="SOURCE_NOT_QUIESCENT"):
        copy_observed_context(
            settings=SessionMigrationSettings(enabled=True, peers={"remote": peer()}),
            peer=peer(),
            source_session_id=sid,
            project="demo",
            direction="to",
            journal=journal,
            operations=ops,
            source_quiescent_and_complete=None,
        )
    assert not ops.root.exists()


def test_lost_receipt_needs_manual_reconcile_and_preserves_source(tmp_path):
    journal = ObservedJournal(tmp_path / "journal")
    ops = OperationStore(tmp_path / "ops")
    sid = str(uuid4())
    journal.append(sid, role="user", text="history")
    observed = []

    def lost_response(_profile, **kwargs):
        observed.append(kwargs)
        raise TimeoutError("lost receiver response")

    with pytest.raises(TimeoutError):
        copy_observed_context(
            settings=SessionMigrationSettings(enabled=True, peers={"remote": peer()}),
            peer=peer(),
            source_session_id=sid,
            project="demo",
            direction="to",
            journal=journal,
            operations=ops,
            source_quiescent_and_complete=lambda: True,
            receiver=lost_response,
        )
    assert observed
    import json

    files = list(ops.root.glob("*.json"))
    assert len(files) == 1
    assert json.loads(files[0].read_text())["state"] == "needs_reconcile"
    assert journal.snapshot(sid)[0]
