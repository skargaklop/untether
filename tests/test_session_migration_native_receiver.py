from __future__ import annotations

from uuid import uuid4

import pytest

from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.native_archive import encode_native
from untether.session_migration.native_receiver import NativeReceiver


def test_unregistered_engine_refuses_native_import_without_receipt(tmp_path):
    root = tmp_path / "state"
    receiver = NativeReceiver(root, max_archive=4096, max_expanded=4096)
    archive = encode_native(
        engine="unregistered-unknown-engine", logical_id=str(uuid4()),
        native_id="native", generation=1, lineage="lineage",
        records={"events.ndjson": b'{"type":"tool"}\n'},
        max_bytes=4096,
    )
    with pytest.raises(CapabilityUnavailable, match="ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE"):
        receiver.receive(str(uuid4()), archive, engine="unregistered-unknown-engine")
    assert not (root / "receipts").exists()
