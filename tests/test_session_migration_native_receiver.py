from __future__ import annotations

import hashlib
from contextlib import contextmanager
from uuid import uuid4

import pytest

from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    register_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_archive import NativeArchive, encode_native
from untether.session_migration.native_receiver import (
    NativeReceiver,
    NativeReceiverError,
)


@pytest.mark.parametrize("actual_native_id", ["sid", "actual-native"])
def test_fenced_export_replays_lost_reply_and_refuses_reuse(
    tmp_path, monkeypatch, actual_native_id
):
    from untether.session_migration.session_gate import session_gate
    from untether.session_migration.source_fence import SourceFenced, read_source_fence

    gates = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gates))
    calls = []

    class Adapter:
        engine = "fixture-export"
        data = b"original"

        def export_exact(self, session_id):
            raise AssertionError("unguarded export")

        def import_exact(self, archive):
            raise NotImplementedError

        @contextmanager
        def export_snapshot(self, sid):
            with session_gate(root=gates, engine_id=self.engine, native_id=sid):
                calls.append(sid)
                yield NativeArchive(
                    engine=self.engine,
                    logical_id=sid,
                    native_id=actual_native_id,
                    generation=1,
                    lineage="fixture",
                    records={"data": self.data},
                    digest="unused",
                )

    register_persistence_adapter(Adapter.engine, Adapter)
    try:
        receiver = NativeReceiver(
            tmp_path / "state", max_archive=8192, max_expanded=8192
        )
        op = str(uuid4())
        package = receiver.export_exact("sid", engine=Adapter.engine, operation_id=op)
        Adapter.data = b"changed"
        retry = NativeReceiver(receiver.root, max_archive=8192, max_expanded=8192)
        assert (
            retry.export_exact("sid", engine=Adapter.engine, operation_id=op) == package
        )
        assert calls == ["sid"]
        fence = read_source_fence(
            gates / "fences", engine=Adapter.engine, native_id="sid"
        )
        assert fence is not None and fence.state == "pending"
        with pytest.raises(NativeReceiverError, match="CONFLICT"):
            retry.export_exact("sid", engine=Adapter.engine, operation_id=str(uuid4()))
        with (
            pytest.raises(SourceFenced),
            session_gate(root=gates, engine_id=Adapter.engine, native_id="sid"),
        ):
            pytest.fail("fenced source admitted")
        with (
            pytest.raises(SourceFenced),
            session_gate(
                root=gates, engine_id=Adapter.engine, native_id=actual_native_id
            ),
        ):
            pytest.fail("actual native source admitted")
        if actual_native_id != "sid":
            from untether.session_migration.source_fence import _fence_path

            native_fence_path = _fence_path(
                gates / "fences", Adapter.engine, actual_native_id
            )
            saved_fence = native_fence_path.read_bytes()
            native_fence_path.unlink()
            with pytest.raises(NativeReceiverError, match="CACHE_FENCE_MISMATCH"):
                retry.export_exact("sid", engine=Adapter.engine, operation_id=op)
            native_fence_path.write_bytes(saved_fence)
        (receiver.root / "exports" / f"{op}.bundle").write_bytes(b"corrupt")
        with pytest.raises(NativeReceiverError, match="CACHE_CORRUPT"):
            retry.export_exact("sid", engine=Adapter.engine, operation_id=op)
        assert calls == ["sid"]
    finally:
        unregister_persistence_adapter(Adapter.engine)


def test_operation_export_requires_gate_configuration(tmp_path, monkeypatch):
    monkeypatch.delenv("UNTETHER_SESSION_GATE_ROOT", raising=False)
    receiver = NativeReceiver(tmp_path, max_archive=8192, max_expanded=8192)
    with pytest.raises(NativeReceiverError, match="SOURCE_GATE_UNCONFIGURED"):
        receiver.export_exact("sid", engine="missing", operation_id=str(uuid4()))
    with pytest.raises(NativeReceiverError, match="OPERATION_ID_INVALID"):
        receiver.export_exact("sid", engine="missing", operation_id="../unsafe")


class _CrashImportAdapter:
    """Records each import call, then simulates a crash before any receipt."""

    engine = "fixture-crash"

    def __init__(self, calls: list):
        self._calls = calls

    def export_exact(self, session_id: str):
        raise NotImplementedError

    def import_exact(self, archive):
        self._calls.append(archive)
        raise RuntimeError("simulated crash before receipt")


def test_unregistered_engine_refuses_native_import_without_receipt(tmp_path):
    root = tmp_path / "state"
    receiver = NativeReceiver(root, max_archive=4096, max_expanded=4096)
    archive = encode_native(
        engine="unregistered-unknown-engine",
        logical_id=str(uuid4()),
        native_id="native",
        generation=1,
        lineage="lineage",
        records={"events.ndjson": b'{"type":"tool"}\n'},
        max_bytes=4096,
    )
    with pytest.raises(
        CapabilityUnavailable, match="ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE"
    ):
        receiver.receive(str(uuid4()), archive, engine="unregistered-unknown-engine")
    assert not (root / "receipts").exists()


def test_crash_before_receipt_retry_same_package_needs_reconcile_no_reimport(tmp_path):
    calls: list = []
    register_persistence_adapter("fixture-crash", lambda: _CrashImportAdapter(calls))
    try:
        root = tmp_path / "state"
        receiver = NativeReceiver(root, max_archive=4096, max_expanded=4096)
        operation_id = str(uuid4())
        content = encode_native(
            engine="fixture-crash",
            logical_id=str(uuid4()),
            native_id="native",
            generation=1,
            lineage="lineage",
            records={"events.ndjson": b'{"type":"tool"}\n'},
            max_bytes=4096,
        )
        with pytest.raises(RuntimeError, match="simulated crash"):
            receiver.receive(operation_id, content, engine="fixture-crash")
        assert len(calls) == 1
        assert not (root / "receipts").exists()
        with pytest.raises(NativeReceiverError, match="NEEDS_RECONCILE"):
            receiver.receive(operation_id, content, engine="fixture-crash")
        assert len(calls) == 1
    finally:
        unregister_persistence_adapter("fixture-crash")


def test_crash_retry_same_operation_altered_package_conflict_no_import(tmp_path):
    calls: list = []
    register_persistence_adapter("fixture-crash", lambda: _CrashImportAdapter(calls))
    try:
        receiver = NativeReceiver(
            tmp_path / "state", max_archive=4096, max_expanded=4096
        )
        operation_id = str(uuid4())

        def archive(events: bytes) -> bytes:
            return encode_native(
                engine="fixture-crash",
                logical_id=str(uuid4()),
                native_id="native",
                generation=1,
                lineage="lineage",
                records={"events.ndjson": events},
                max_bytes=4096,
            )

        with pytest.raises(RuntimeError, match="simulated crash"):
            receiver.receive(
                operation_id, archive(b'{"type":"tool"}\n'), engine="fixture-crash"
            )
        assert len(calls) == 1
        with pytest.raises(NativeReceiverError, match="OPERATION_CONFLICT"):
            receiver.receive(
                operation_id,
                archive(b'{"type":"tool","altered":true}\n'),
                engine="fixture-crash",
            )
        assert len(calls) == 1
    finally:
        unregister_persistence_adapter("fixture-crash")


class _CompleteExportAdapter:
    """Snapshot factory for complete_export tests (never imports)."""

    engine = "fixture-complete"

    def __init__(self, gates, actual_native_id: str, calls: list):
        self._gates = gates
        self._native_id = actual_native_id
        self._calls = calls

    def export_exact(self, session_id: str):
        raise AssertionError("unguarded export")

    def import_exact(self, archive):
        raise NotImplementedError

    @contextmanager
    def export_snapshot(self, sid: str):
        from untether.session_migration.session_gate import session_gate

        with session_gate(root=self._gates, engine_id=self.engine, native_id=sid):
            self._calls.append(sid)
            yield NativeArchive(
                engine=self.engine,
                logical_id=sid,
                native_id=self._native_id,
                generation=1,
                lineage="fixture",
                records={"data": b"original"},
                digest="unused",
            )


@pytest.mark.parametrize("actual_native_id", ["sid", "actual-native"])
def test_complete_export_flips_both_fences_pending_to_completed(
    tmp_path, monkeypatch, actual_native_id
):
    from untether.session_migration.source_fence import read_source_fence

    gates = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gates))
    calls: list = []
    register_persistence_adapter(
        _CompleteExportAdapter.engine,
        lambda: _CompleteExportAdapter(gates, actual_native_id, calls),
    )
    try:
        receiver = NativeReceiver(
            tmp_path / "state", max_archive=8192, max_expanded=8192
        )
        op = str(uuid4())
        package = receiver.export_exact(
            "sid", engine=_CompleteExportAdapter.engine, operation_id=op
        )
        digest = hashlib.sha256(package).hexdigest()
        for native_id in dict.fromkeys(("sid", actual_native_id)):
            fence = read_source_fence(
                gates / "fences",
                engine=_CompleteExportAdapter.engine,
                native_id=native_id,
            )
            assert fence is not None and fence.state == "pending"
        result = receiver.complete_export(
            op,
            engine=_CompleteExportAdapter.engine,
            session_id="sid",
            package_sha256=digest,
        )
        assert result == {
            "operation_id": op,
            "engine": _CompleteExportAdapter.engine,
            "session_id": "sid",
            "package_sha256": digest,
            "status": "export_completed",
        }
        for native_id in dict.fromkeys(("sid", actual_native_id)):
            fence = read_source_fence(
                gates / "fences",
                engine=_CompleteExportAdapter.engine,
                native_id=native_id,
            )
            assert fence is not None and fence.state == "completed"
            assert fence.operation_id == op
            assert fence.package_sha256 == digest
        assert calls == ["sid"]
    finally:
        unregister_persistence_adapter(_CompleteExportAdapter.engine)


def test_complete_export_repeated_completion_is_idempotent(tmp_path, monkeypatch):
    from untether.session_migration.source_fence import read_source_fence

    gates = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gates))
    calls: list = []
    register_persistence_adapter(
        _CompleteExportAdapter.engine,
        lambda: _CompleteExportAdapter(gates, "actual-native", calls),
    )
    try:
        receiver = NativeReceiver(
            tmp_path / "state", max_archive=8192, max_expanded=8192
        )
        op = str(uuid4())
        package = receiver.export_exact(
            "sid", engine=_CompleteExportAdapter.engine, operation_id=op
        )
        digest = hashlib.sha256(package).hexdigest()
        first = receiver.complete_export(
            op,
            engine=_CompleteExportAdapter.engine,
            session_id="sid",
            package_sha256=digest,
        )
        again = receiver.complete_export(
            op,
            engine=_CompleteExportAdapter.engine,
            session_id="sid",
            package_sha256=digest,
        )
        assert first == again
        for native_id in ("sid", "actual-native"):
            fence = read_source_fence(
                gates / "fences",
                engine=_CompleteExportAdapter.engine,
                native_id=native_id,
            )
            assert fence is not None and fence.state == "completed"
    finally:
        unregister_persistence_adapter(_CompleteExportAdapter.engine)


def test_complete_export_refuses_foreign_digest_and_unknown_op_without_fence_change(
    tmp_path, monkeypatch
):
    from untether.session_migration.source_fence import read_source_fence

    gates = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gates))
    calls: list = []
    register_persistence_adapter(
        _CompleteExportAdapter.engine,
        lambda: _CompleteExportAdapter(gates, "actual-native", calls),
    )
    try:
        receiver = NativeReceiver(
            tmp_path / "state", max_archive=8192, max_expanded=8192
        )
        op = str(uuid4())
        package = receiver.export_exact(
            "sid", engine=_CompleteExportAdapter.engine, operation_id=op
        )
        digest = hashlib.sha256(package).hexdigest()
        wrong = hashlib.sha256(package + b"tampered").hexdigest()
        with pytest.raises(NativeReceiverError, match="EXPORT_CACHE_CORRUPT"):
            receiver.complete_export(
                op,
                engine=_CompleteExportAdapter.engine,
                session_id="sid",
                package_sha256=wrong,
            )
        with pytest.raises(NativeReceiverError, match="EXPORT_CACHE_MISSING"):
            receiver.complete_export(
                str(uuid4()),
                engine=_CompleteExportAdapter.engine,
                session_id="sid",
                package_sha256=digest,
            )
        for native_id in ("sid", "actual-native"):
            fence = read_source_fence(
                gates / "fences",
                engine=_CompleteExportAdapter.engine,
                native_id=native_id,
            )
            assert fence is not None and fence.state == "pending"
        assert calls == ["sid"]
    finally:
        unregister_persistence_adapter(_CompleteExportAdapter.engine)


def test_rejected_completion_does_not_create_export_evidence(tmp_path, monkeypatch):
    from untether.session_migration.source_fence import _fence_path

    gates = tmp_path / "gates"
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gates))
    register_persistence_adapter(
        _CompleteExportAdapter.engine,
        lambda: _CompleteExportAdapter(gates, "actual-native", []),
    )
    try:
        receiver = NativeReceiver(
            tmp_path / "state", max_archive=8192, max_expanded=8192
        )
        op = str(uuid4())
        package = receiver.export_exact(
            "sid", engine=_CompleteExportAdapter.engine, operation_id=op
        )
        fence = _fence_path(
            gates / "fences", _CompleteExportAdapter.engine, "actual-native"
        )
        fence.rename(fence.with_suffix(".held"))
        assert not (receiver.root / "operations").exists()
        with pytest.raises(NativeReceiverError, match="EXPORT_FENCE_MISMATCH"):
            receiver.complete_export(
                op,
                engine=_CompleteExportAdapter.engine,
                session_id="sid",
                package_sha256=hashlib.sha256(package).hexdigest(),
                peer_name="trusted-local",
                project="demo",
            )
        assert not (receiver.root / "operations").exists()
    finally:
        unregister_persistence_adapter(_CompleteExportAdapter.engine)
