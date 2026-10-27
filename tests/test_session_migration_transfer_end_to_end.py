"""Packet focus: two different sessions, one shared endpoint config, real processes.

Every branch drives the actual subprocess engine adapter fixture; only the SSH
byte transport is replaced by an in-process fixed receiver running the real
NativeReceiver, protocol handler, codec, and write-once receipts.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio
import pytest

from untether.session_migration import transfer
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    register_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.receiver_protocol import ReceiverEndpoint
from untether.session_migration.settings import MigrationPeer, SessionMigrationSettings
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS
from untether.session_migration.subprocess_adapter import ExecutablePersistenceAdapter
from untether.session_migration.telegram_enrollment import EnrollmentStore
from untether.telegram.topic_state import TopicStateStore, resolve_state_path

FIXTURE = Path(__file__).parent / "fixtures" / "native_persistence_engine.py"
ENGINE = "fixture-engine"
PIN = "SHA256:" + "A" * 43


@pytest.fixture(autouse=True)
def _gate_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))


def _peer() -> MigrationPeer:
    return MigrationPeer(
        host="example.invalid",
        user="operator",
        identity_dir="keys",
        identity_file="selected",
        known_hosts_file="known",
        host_key_fingerprint=PIN,
        receiver_path="/usr/local/bin/untether-migration-receiver",
        execution_mode="native",
        expected_endpoint_id="endpoint-one",
        destination_binding="enrolled-one",
        projects={"demo": "/srv/demo"},
    )


def _seed(store: Path, sid: str, *, engine: str = ENGINE, generation: int = 2) -> None:
    payload = {
        "engine": engine,
        "logical_id": sid,
        "native_id": sid,
        "generation": generation,
        "lineage": sid,
        "records": {
            "history/tool.ndjson": base64.b64encode(b"tool\r\nraw").decode(),
            "assets/a.bin": base64.b64encode(b"\x00\xff\xfe").decode(),
        },
    }
    (store / (sid + ".json")).write_text(json.dumps(payload), encoding="utf-8")


def _records(sid: str) -> dict[str, bytes]:
    return {
        "history/tool.ndjson": b"tool\r\nraw",
        "assets/a.bin": b"\x00\xff\xfe",
    }


def _receiver_call(store: Path, tmp_path: Path, *, authorized_peer="local"):
    """In-process stand-in for the SSH RPC: real protocol + receiver + adapter.

    A real remote receiver runs in its own process with its own adapter
    registry pointing at the destination store; the swap below mirrors that
    boundary inside this single process.
    """
    import base64
    import hashlib

    from untether.session_migration import native_adapter
    from untether.session_migration.receiver_protocol import handle_receiver

    remote_adapter = _adapter(store)
    exported = {}
    completed = set()

    def call(
        operation_id,
        package=b"",
        *,
        peer,
        project,
        engine,
        action="receive",
        session_id=None,
    ):
        if action == "export-complete":
            digest = json.loads(package)["package_sha256"]
            assert exported[operation_id] == (engine, session_id, digest)
            completed.add(operation_id)
            return {
                "status": "export_completed",
                "operation_id": operation_id,
                "engine": engine,
                "session_id": session_id,
                "package_sha256": digest,
                "endpoint_id": peer.expected_endpoint_id,
            }
        native = NativeReceiver(
            tmp_path / "native-state", max_archive=65536, max_expanded=65536
        )
        checks = dict.fromkeys(_REQUIRED_CHECKS, True)
        endpoint = ReceiverEndpoint(
            endpoint_id=peer.expected_endpoint_id,
            execution_mode="native",
            destination_binding=peer.destination_binding,
            project=project,
            deployment="direct-linux",
            arch="x86_64",
            probe_health=lambda: checks,
            native_receiver=native,
            authorized_peer=authorized_peer,
        )
        previous = native_adapter._ADAPTERS.get(ENGINE)
        native_adapter._ADAPTERS[ENGINE] = lambda: remote_adapter
        try:
            fields = {
                "protocol": 2,
                "action": action,
                "operation_id": operation_id,
                "project": project,
                "size": len(package),
                "sha256": hashlib.sha256(package).hexdigest(),
                "engine": engine,
            }
            if session_id:
                fields["session_id"] = session_id
            frame = json.dumps(fields).encode() + b"\n" + package
            saved_gate = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
            os.environ["UNTETHER_SESSION_GATE_ROOT"] = str(tmp_path / "receiver-gate")
            try:
                raw = handle_receiver(endpoint, verb=action, payload=frame)
            finally:
                if saved_gate is None:
                    os.environ.pop("UNTETHER_SESSION_GATE_ROOT", None)
                else:
                    os.environ["UNTETHER_SESSION_GATE_ROOT"] = saved_gate
            reply = json.loads(raw)
            if action == "export" and "package_base64" in reply:
                reply["package"] = base64.b64decode(reply["package_base64"])
                exported[operation_id] = (
                    engine,
                    session_id,
                    hashlib.sha256(reply["package"]).hexdigest(),
                )
            return reply
        finally:
            if previous is None:
                native_adapter._ADAPTERS.pop(ENGINE, None)
            else:
                native_adapter._ADAPTERS[ENGINE] = previous

    return call


def _adapter(store: Path) -> ExecutablePersistenceAdapter:
    return ExecutablePersistenceAdapter(
        ENGINE, FIXTURE, store, interpreter=sys.executable
    )


def test_push_two_sessions_same_endpoint_config_real_subprocess(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    register_persistence_adapter(ENGINE, lambda: _adapter(source))
    try:
        cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
        results = []
        for sid in (str(uuid4()), str(uuid4())):
            _seed(source, sid)
            results.append(
                transfer.perform(
                    cfg,
                    peer_name="vps",
                    project="demo",
                    session_id=sid,
                    engine=ENGINE,
                    direction="to",
                    root=tmp_path / "ops",
                    receiver_call=_receiver_call(destination, tmp_path),
                )
            )
        assert results[0].operation_id != results[1].operation_id
        for result, sid in zip(
            results, (results[0].session_id, results[1].session_id), strict=True
        ):
            assert result.state == "completed"
            assert result.native_id == sid
            assert result.engine == ENGINE
            assert len(result.receipt_sha256) == 64
        # Both sessions now live in the shared destination store, source intact.
        resumed = ExecutablePersistenceAdapter(
            ENGINE, FIXTURE, destination, interpreter=sys.executable
        )
        for result in results:
            assert (source / (result.session_id + ".json")).exists()
            reloaded = resumed.export_exact(result.session_id)
            assert reloaded.records == _records(result.session_id)
            assert reloaded.logical_id == result.session_id
            assert reloaded.lineage == result.session_id
        # Write-once receipts: exactly one file per operation.
        receipts = list((tmp_path / "native-state" / "receipts").glob("*.json"))
        assert len(receipts) == 2
    finally:
        unregister_persistence_adapter(ENGINE)


def test_pull_retry_with_destination_binds_already_verified_operation(tmp_path):
    remote_store, local_store = tmp_path / "remote", tmp_path / "local"
    remote_store.mkdir()
    local_store.mkdir()
    sid = str(uuid4())
    _seed(remote_store, sid)
    register_persistence_adapter(ENGINE, lambda: _adapter(local_store))
    try:
        cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
        package = transfer.build_inbound_package(
            cfg,
            ExecutablePersistenceAdapter(
                ENGINE, FIXTURE, remote_store, interpreter=sys.executable
            ).export_exact(sid),
        )
        common: dict[str, Any] = {
            "peer_name": "vps",
            "project": "demo",
            "session_id": sid,
            "engine": ENGINE,
            "direction": "from",
            "root": tmp_path / "ops",
            "incoming_package": package,
        }
        # First pull verifies the restore but requests no destination yet.
        first = transfer.perform(
            cfg,
            destination_chat=None,
            destination_topic=None,
            destination_bot_id=None,
            source_bot_id=None,
            **common,
        )
        assert first.state == "verified"
        # Retry of the same operation WITH a destination must bind, not stall.
        second = transfer.perform(
            cfg,
            destination_chat=-100123,
            destination_topic=42,
            destination_bot_id=22,
            source_bot_id=11,
            **common,
        )
        assert second.state == "bound"
        assert second.receipt_sha256 == first.receipt_sha256
        # No duplicate import: exactly one write-once receipt.
        receipts = list((tmp_path / "ops" / "native" / "receipts").glob("*.json"))
        assert len(receipts) == 1
        token = anyio.run(
            TopicStateStore(
                resolve_state_path(tmp_path / "untether.toml")
            ).get_session_resume,
            -100123,
            42,
            ENGINE,
        )
        assert token is not None
        assert token.value == sid
        enrollment = EnrollmentStore(tmp_path / "ops" / "enrollment.json")
        assert (
            enrollment.require_binding(
                bot_id=22,
                chat_id=-100123,
                topic_id=42,
                peer_name="vps",
                endpoint_id="endpoint-one",
            ).confirmed
            is True
        )
    finally:
        unregister_persistence_adapter(ENGINE)


def test_pull_binds_destination_chat_only_after_verified_restore(tmp_path):
    remote_store, local_store = tmp_path / "remote", tmp_path / "local"
    remote_store.mkdir()
    local_store.mkdir()
    sid = str(uuid4())
    _seed(remote_store, sid)
    # Export from the remote store with a real fresh subprocess adapter.
    remote_adapter = ExecutablePersistenceAdapter(
        ENGINE, FIXTURE, remote_store, interpreter=sys.executable
    )
    register_persistence_adapter(ENGINE, lambda: _adapter(local_store))
    try:
        cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
        package = transfer.build_inbound_package(cfg, remote_adapter.export_exact(sid))
        result = transfer.perform(
            cfg,
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="from",
            root=tmp_path / "ops",
            incoming_package=package,
            destination_chat=-100123,
            destination_topic=42,
            destination_bot_id=22,
            source_bot_id=11,
        )
        assert result.state == "bound"
        assert result.native_id == sid
        enrollment = EnrollmentStore(tmp_path / "ops" / "enrollment.json")
        assert (
            enrollment.require_binding(
                bot_id=22,
                chat_id=-100123,
                topic_id=42,
                peer_name="vps",
                endpoint_id="endpoint-one",
            ).confirmed
            is True
        )
        # Local store resumed by a fresh process holds identical records.
        reloaded = _adapter(local_store).export_exact(sid)
        assert reloaded.records == _records(sid)

        # Actual TopicStateStore updated using existing public seam
        topic_store = TopicStateStore(resolve_state_path(tmp_path / "untether.toml"))
        token = anyio.run(topic_store.get_session_resume, -100123, 42, ENGINE)
        assert token is not None
        assert token.value == sid
        assert token.engine == ENGINE
    finally:
        unregister_persistence_adapter(ENGINE)


def test_pull_two_sessions_same_endpoint_config_real_subprocess(tmp_path):
    remote_store, local_store = tmp_path / "remote", tmp_path / "local"
    remote_store.mkdir()
    local_store.mkdir()
    sids = [str(uuid4()), str(uuid4())]
    for sid in sids:
        _seed(remote_store, sid)

    register_persistence_adapter(ENGINE, lambda: _adapter(local_store))
    try:
        cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
        results = []
        # Pull two different sessions over the same endpoint configuration.
        # Coordinator pull fetches itself over SSH receiver RPC with NO incoming_package.
        for i, sid in enumerate(sids):
            result = transfer.perform(
                cfg,
                peer_name="vps",
                project="demo",
                session_id=sid,
                engine=ENGINE,
                direction="from",
                root=tmp_path / "ops",
                config_path=tmp_path / "untether.toml",
                receiver_call=_receiver_call(remote_store, tmp_path),
                destination_chat=-100123,
                destination_topic=100 + i,
            )
            results.append(result)

        assert results[0].operation_id != results[1].operation_id
        for result, sid in zip(results, sids, strict=True):
            assert result.state == "bound"
            assert result.native_id == sid
            assert result.engine == ENGINE
            assert len(result.receipt_sha256) == 64

        topic_store = TopicStateStore(resolve_state_path(tmp_path / "untether.toml"))
        for i, sid in enumerate(sids):
            token = anyio.run(topic_store.get_session_resume, -100123, 100 + i, ENGINE)
            assert token is not None
            assert token.value == sid
            assert token.engine == ENGINE

        resumed = _adapter(local_store)
        for sid in sids:
            reloaded = resumed.export_exact(sid)
            assert reloaded.records == _records(sid)
            assert reloaded.logical_id == sid
            assert reloaded.lineage == sid
    finally:
        unregister_persistence_adapter(ENGINE)


def test_pull_without_verified_restore_never_binds_chat(tmp_path):
    remote_store, local_store = tmp_path / "remote", tmp_path / "local"
    remote_store.mkdir()
    local_store.mkdir()
    sid = str(uuid4())
    other = str(uuid4())
    _seed(remote_store, sid)
    _seed(remote_store, other)
    # Engine adapter missing for the destination engine: import cannot verify.
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    remote_archive = transfer.build_inbound_package(
        cfg,
        ExecutablePersistenceAdapter(
            ENGINE, FIXTURE, remote_store, interpreter=sys.executable
        ).export_exact(sid),
    )
    # A package naming a different session must not pass as this transfer.
    mismatched = transfer.build_inbound_package(
        cfg,
        ExecutablePersistenceAdapter(
            ENGINE, FIXTURE, remote_store, interpreter=sys.executable
        ).export_exact(other),
    )
    with pytest.raises(transfer.TransferRefusal, match="SESSION_IDENTITY_MISMATCH"):
        transfer.perform(
            cfg,
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="from",
            root=tmp_path / "ops",
            incoming_package=mismatched,
            destination_chat=-100123,
            destination_topic=None,
            destination_bot_id=22,
            source_bot_id=11,
        )
    mid = len(remote_archive) // 2
    tampered = (
        remote_archive[:mid]
        + bytes([remote_archive[mid] ^ 0xFF])
        + remote_archive[mid + 1 :]
    )
    with pytest.raises(transfer.TransferRefusal, match="ENGINE_NATIVE_PERSISTENCE"):
        transfer.perform(
            cfg,
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="from",
            root=tmp_path / "ops",
            incoming_package=remote_archive,
            destination_chat=-100123,
            destination_topic=None,
            destination_bot_id=22,
            source_bot_id=11,
        )
    assert not (tmp_path / "ops" / "enrollment.json").exists()
    # Tampered package is refused before any import or binding.
    register_persistence_adapter(ENGINE, lambda: _adapter(local_store))
    try:
        with pytest.raises(transfer.TransferRefusal, match="ARCHIVE"):
            transfer.perform(
                cfg,
                peer_name="vps",
                project="demo",
                session_id=sid,
                engine=ENGINE,
                direction="from",
                root=tmp_path / "ops",
                incoming_package=tampered,
                destination_chat=-100123,
                destination_topic=None,
                destination_bot_id=22,
                source_bot_id=11,
            )
        assert not (tmp_path / "ops" / "enrollment.json").exists()
    finally:
        unregister_persistence_adapter(ENGINE)


@pytest.mark.parametrize("damage", [False, True])
def test_pending_push_replays_cached_snapshot(tmp_path, monkeypatch, damage):
    from untether.session_migration.session_gate import session_gate
    from untether.session_migration.source_fence import SourceFenced, read_source_fence

    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    sid = str(uuid4())
    _seed(source, sid)
    register_persistence_adapter(ENGINE, lambda: _adapter(source))
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    common: dict[str, Any] = {
        "peer_name": "vps",
        "project": "demo",
        "session_id": sid,
        "engine": ENGINE,
        "direction": "to",
        "root": tmp_path / "ops",
    }
    actual = _receiver_call(destination, tmp_path)
    sent = []

    def call(op, package, **kwargs):
        with (
            pytest.raises(SourceFenced),
            session_gate(
                root=Path(os.environ["UNTETHER_SESSION_GATE_ROOT"]),
                engine_id=ENGINE,
                native_id=sid,
            ),
        ):
            pytest.fail("pending source admitted")
        sent.append((op, package))
        reply = actual(op, package, **kwargs)
        if len(sent) == 1:
            raise TimeoutError("lost ACK")
        return reply

    try:
        with pytest.raises(TimeoutError, match="lost ACK"):
            transfer.perform(cfg, receiver_call=call, **common)
        fence = read_source_fence(
            tmp_path / "gate" / "fences", engine=ENGINE, native_id=sid
        )
        assert fence is not None and fence.state == "pending"
        _seed(source, sid, generation=99)

        def forbidden(*args):
            pytest.fail("retry exported drifted source")

        monkeypatch.setattr(ExecutablePersistenceAdapter, "export_snapshot", forbidden)
        other = _peer().model_copy(update={"expected_endpoint_id": "other"})
        with pytest.raises(transfer.TransferRefusal, match="OPERATION_CONFLICT"):
            transfer.perform(
                SessionMigrationSettings(enabled=True, peers={"vps": other}),
                receiver_call=call,
                **common,
            )
        if damage:
            cache = next((tmp_path / "ops" / "operations").glob("*.package"))
            cache.write_bytes(b"corrupt")
            with pytest.raises(transfer.TransferRefusal, match="PACKAGE_CACHE_CORRUPT"):
                transfer.perform(cfg, receiver_call=call, **common)
            assert len(sent) == 1
        else:
            result = transfer.perform(cfg, receiver_call=call, **common)
            assert result.state == "completed"
            assert sent[0] == sent[1]
            assert transfer.perform(cfg, receiver_call=call, **common) == result
            assert len(sent) == 2
        assert len(list((tmp_path / "native-state" / "receipts").glob("*.json"))) == 1
    finally:
        unregister_persistence_adapter(ENGINE)


def test_push_requires_snapshot_and_durable_cache(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    sid = str(uuid4())
    _seed(source, sid)
    register_persistence_adapter(ENGINE, lambda: _adapter(source))
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    common: dict[str, Any] = {
        "peer_name": "vps",
        "project": "demo",
        "session_id": sid,
        "engine": ENGINE,
        "direction": "to",
        "root": tmp_path / "ops",
    }

    def forbidden(*args, **kwargs):
        pytest.fail("unsafe send")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(ExecutablePersistenceAdapter, "export_snapshot", None)
            with pytest.raises(
                transfer.TransferRefusal, match="SNAPSHOT_ADMISSION_UNAVAILABLE"
            ):
                transfer.perform(cfg, receiver_call=forbidden, **common)

        def broken_link(*args):
            raise OSError("disk failure")

        monkeypatch.setattr(transfer.os, "link", broken_link)
        with pytest.raises(
            transfer.TransferRefusal, match="PACKAGE_CACHE_WRITE_FAILED"
        ):
            transfer.perform(cfg, receiver_call=forbidden, **common)
        assert not list((tmp_path / "gate" / "fences").glob("*.json"))
    finally:
        unregister_persistence_adapter(ENGINE)


@pytest.mark.parametrize("destination_topic", [None, 42])
@pytest.mark.parametrize(
    "wrong_field",
    [
        None,
        "status",
        "operation_id",
        "engine",
        "session_id",
        "package_sha256",
        "endpoint_id",
    ],
)
def test_pull_completion_retry_without_duplicate_import(
    tmp_path, monkeypatch, destination_topic, wrong_field
):
    import hashlib

    remote, local = tmp_path / "remote", tmp_path / "local"
    remote.mkdir()
    local.mkdir()
    sid = str(uuid4())
    _seed(remote, sid)
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    adapter = _adapter(local)
    imports = []
    original_import = ExecutablePersistenceAdapter.import_exact

    def counted_import(self, archive):
        imports.append(archive.native_id)
        return original_import(self, archive)

    monkeypatch.setattr(ExecutablePersistenceAdapter, "import_exact", counted_import)
    register_persistence_adapter(ENGINE, lambda: adapter)
    exports, completions = [], []
    cached = None
    completed = False

    def call(op, package=b"", *, peer, project, engine, action, session_id):
        nonlocal cached, completed
        assert engine == ENGINE and session_id == sid and project == "demo"
        if action == "export":
            exports.append(op)
            if cached is None:
                cached = transfer.build_inbound_package(
                    cfg, _adapter(remote).export_exact(sid)
                )
            return {"status": "exported", "package": cached}
        assert action == "export-complete"
        assert cached is not None
        digest = hashlib.sha256(cached).hexdigest()
        assert json.loads(package) == {"package_sha256": digest}
        assert op == exports[0]
        record = json.loads(
            next((tmp_path / "ops" / "operations").glob("*.json")).read_text()
        )
        assert record["state"] == ("verified" if destination_topic is None else "bound")
        if destination_topic is not None:
            token = anyio.run(
                TopicStateStore(
                    resolve_state_path(tmp_path / "untether.toml")
                ).get_session_resume,
                -100123,
                destination_topic,
                ENGINE,
            )
            assert token is not None and token.value == sid
        completions.append(op)
        completed = True
        reply = {
            "status": "export_completed",
            "operation_id": op,
            "engine": engine,
            "session_id": session_id,
            "package_sha256": digest,
            "endpoint_id": peer.expected_endpoint_id,
        }
        if len(completions) == 1:
            if wrong_field is None:
                raise TimeoutError("lost completion ACK")
            reply[wrong_field] = "wrong"
        return reply

    common: dict[str, Any] = {
        "peer_name": "vps",
        "project": "demo",
        "session_id": sid,
        "engine": ENGINE,
        "direction": "from",
        "root": tmp_path / "ops",
        "receiver_call": call,
        "destination_topic": destination_topic,
        "destination_chat": None if destination_topic is None else -100123,
    }
    try:
        if wrong_field is None:
            with pytest.raises(TimeoutError, match="lost completion ACK"):
                transfer.perform(cfg, **common)
        else:
            with pytest.raises(transfer.TransferRefusal, match="COMPLETION_MISMATCH"):
                transfer.perform(cfg, **common)
        assert completed and imports == [sid]
        _seed(remote, sid, generation=99)
        result = transfer.perform(cfg, **common)
        assert transfer.perform(cfg, **common) == result
        assert imports == [sid]
        assert len(exports) == len(completions) == 3
        assert len(set(exports)) == 1
        # Injected packages never own a remote export, including record retries.
        injected: dict[str, Any] = {**common, "incoming_package": cached}
        transfer.perform(cfg, **injected)
        assert len(completions) == 3
    finally:
        unregister_persistence_adapter(ENGINE)


@pytest.mark.parametrize("crash", [None, "overwrite", "activation"])
def test_round_trip_reclaim_pushed_session_with_evolved_content(
    tmp_path, monkeypatch, crash
):
    """Local -> remote -> local: an evolved descendant pull reclaims the pushed
    identity instead of colliding, and the second identical pull is a no-op."""
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    # The remote receiver runs on another machine with its own gate root; the
    # in-process harness must swap the env inside each receiver call to keep
    # that boundary honest rather than letting both sides share one root.
    remote_gate = tmp_path / "remote-gate"
    base_call = _receiver_call(remote, tmp_path)

    def bounded_call(*args, **kwargs):
        saved = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        os.environ["UNTETHER_SESSION_GATE_ROOT"] = str(remote_gate)
        try:
            return base_call(*args, **kwargs)
        finally:
            if saved is None:
                os.environ.pop("UNTETHER_SESSION_GATE_ROOT", None)
            else:
                os.environ["UNTETHER_SESSION_GATE_ROOT"] = saved

    sid = str(uuid4())
    _seed(local, sid)
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    register_persistence_adapter(ENGINE, lambda: _adapter(local))
    overwrites = []
    original_overwrite = ExecutablePersistenceAdapter.import_overwrite

    def counted_overwrite(self, **kwargs):
        overwrites.append(kwargs["native_id"])
        return original_overwrite(self, **kwargs)

    monkeypatch.setattr(
        ExecutablePersistenceAdapter, "import_overwrite", counted_overwrite
    )
    try:
        # Outbound move: local session lands in the remote store, local source
        # receives a completed durable fence under the push operation.
        pushed = transfer.perform(
            cfg,
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="to",
            root=tmp_path / "ops",
            receiver_call=bounded_call,
        )
        assert pushed.state == "completed"
        assert (local / (sid + ".json")).exists()

        # The session evolves on the remote side.
        _seed(remote, sid, generation=99)
        remote_path = remote / (sid + ".json")
        evolved = json.loads(remote_path.read_text())
        evolved_bytes = b"tool\r\nraw\nfollow-up tool result"
        evolved["records"]["history/tool.ndjson"] = base64.b64encode(
            evolved_bytes
        ).decode()
        remote_path.write_text(json.dumps(evolved))

        # Return pull: the local store still holds the old bytes, but the
        # verified descendant archive reclaims the identity through the
        # completed fence instead of failing with SESSION_ID_COLLISION.
        common: dict[str, Any] = {
            "peer_name": "vps",
            "project": "demo",
            "session_id": sid,
            "engine": ENGINE,
            "direction": "from",
            "root": tmp_path / "ops",
            "receiver_call": bounded_call,
        }
        if crash:
            with monkeypatch.context() as patch:
                if crash == "activation":

                    def interrupted(*args, **kwargs):
                        raise RuntimeError("crash before activation")

                    patch.setattr(transfer, "activate_source_fence", interrupted)
                else:

                    def interrupted_overwrite(self, **kwargs):
                        counted_overwrite(self, **kwargs)
                        raise RuntimeError("crash after overwrite")

                    patch.setattr(
                        ExecutablePersistenceAdapter,
                        "import_overwrite",
                        interrupted_overwrite,
                    )
                with pytest.raises(RuntimeError, match="crash"):
                    transfer.perform(cfg, **common)
        result = transfer.perform(cfg, **common)
        assert result.state == "verified"
        assert overwrites == [sid]

        # The local store now holds the evolved bytes, byte-identical.
        reloaded = _adapter(local).export_exact(sid)
        assert reloaded.records == {
            **_records(sid),
            "history/tool.ndjson": evolved_bytes,
        }
        assert reloaded.generation == 99

        # A second identical pull replays the verified operation: no
        # reimport, no second overwrite, idempotent result.
        again = transfer.perform(
            cfg,
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="from",
            root=tmp_path / "ops",
            receiver_call=bounded_call,
        )
        assert again.operation_id == result.operation_id
        assert overwrites == [sid]
        from untether.session_migration.session_gate import session_gate

        # A returned identity must be usable, not merely copied successfully.
        with session_gate(root=tmp_path / "gate", engine_id=ENGINE, native_id=sid):
            pass
        # Active ownership is a new source, not a replay of the old push.
        replies = []

        def verified_second_move(op, package, **kwargs):
            import hashlib

            replies.append(op)
            return {
                "protocol": 1,
                "endpoint_id": _peer().expected_endpoint_id,
                "operation_id": op,
                "package_sha256": hashlib.sha256(package).hexdigest(),
                "importer_version": "test",
                "native_id": sid,
                "status": "verified",
                "engine": ENGINE,
            }

        second_move = transfer.perform(
            cfg,
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="to",
            root=tmp_path / "ops",
            receiver_call=verified_second_move,
        )
        assert second_move.operation_id != pushed.operation_id
        assert replies == [second_move.operation_id]
        from untether.session_migration.source_fence import SourceFenced

        with (
            pytest.raises(SourceFenced),
            session_gate(root=tmp_path / "gate", engine_id=ENGINE, native_id=sid),
        ):
            pass
    finally:
        unregister_persistence_adapter(ENGINE)


def test_second_push_after_return_pull_reclaims_real_receiver(tmp_path, monkeypatch):
    """A -> B -> A -> B must transfer ownership, not just return a fake ACK."""
    from untether.session_migration.session_gate import session_gate
    from untether.session_migration.source_fence import SourceFenced

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    sid = str(uuid4())
    _seed(local, sid)
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    base_call = _receiver_call(remote, tmp_path)
    receiver = NativeReceiver(
        tmp_path / "native-state", max_archive=65536, max_expanded=65536
    )
    imports, overwrites = [], []
    original_import = ExecutablePersistenceAdapter.import_exact
    original_overwrite = ExecutablePersistenceAdapter.import_overwrite

    def counted_import(self, archive):
        imports.append((self.store, archive.native_id))
        return original_import(self, archive)

    def counted_overwrite(self, **kwargs):
        overwrites.append((self.store, kwargs["native_id"]))
        return original_overwrite(self, **kwargs)

    monkeypatch.setattr(ExecutablePersistenceAdapter, "import_exact", counted_import)
    monkeypatch.setattr(
        ExecutablePersistenceAdapter, "import_overwrite", counted_overwrite
    )

    def call(op, package=b"", **kwargs):
        if kwargs.get("action") != "export-complete":
            return base_call(op, package, **kwargs)
        # Unlike the older fixture helper, complete the real remote fences.
        with monkeypatch.context() as patch:
            patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "receiver-gate"))
            result = receiver.complete_export(
                op,
                engine=kwargs["engine"],
                session_id=kwargs["session_id"],
                package_sha256=json.loads(package)["package_sha256"],
                peer_name="local",
                project="demo",
            )
        return {**result, "endpoint_id": kwargs["peer"].expected_endpoint_id}

    common = {
        "peer_name": "vps",
        "project": "demo",
        "session_id": sid,
        "engine": ENGINE,
        "root": tmp_path / "ops",
        "receiver_call": call,
    }
    register_persistence_adapter(ENGINE, lambda: _adapter(local))
    try:
        first = transfer.perform(cfg, direction="to", **common)
        _seed(remote, sid, generation=3)
        returned = transfer.perform(cfg, direction="from", **common)
        assert returned.state == "verified"
        with session_gate(root=tmp_path / "gate", engine_id=ENGINE, native_id=sid):
            pass
        with (
            pytest.raises(SourceFenced),
            session_gate(
                root=tmp_path / "receiver-gate", engine_id=ENGINE, native_id=sid
            ),
        ):
            pytest.fail("returned remote source admitted")

        # Evolve on A after return, including opaque bytes, not just metadata.
        path = local / (sid + ".json")
        data = json.loads(path.read_text())
        latest = b"latest tool result\x00\xff\r\n"
        data["generation"] = 4
        data["records"]["history/tool.ndjson"] = base64.b64encode(latest).decode()
        path.write_text(json.dumps(data))
        try:
            second = transfer.perform(cfg, direction="to", **common)
        except Exception:
            # Preserve the original failure, but first prove it fails closed:
            # no success receipt, no ownership release, no second import retry.
            counts = (len(imports), len(overwrites))
            receipts = list((receiver.root / "receipts").glob("*.json"))
            assert len(receipts) == 1
            assert _adapter(remote).export_exact(sid).generation == 3
            from untether.session_migration.receiver_protocol import (
                ReceiverProtocolError,
            )

            with pytest.raises(ReceiverProtocolError, match="NEEDS_RECONCILE"):
                transfer.perform(cfg, direction="to", **common)
            assert (len(imports), len(overwrites)) == counts
            for gate in ("gate", "receiver-gate"):
                with (
                    pytest.raises(SourceFenced),
                    session_gate(root=tmp_path / gate, engine_id=ENGINE, native_id=sid),
                ):
                    pytest.fail("failed transfer released ownership")
            raise
        assert second.state == "completed"
        assert second.operation_id != first.operation_id
        restored = _adapter(remote).export_exact(sid)
        assert restored.records == {**_records(sid), "history/tool.ndjson": latest}
        assert restored.generation == 4 and restored.lineage == sid
        with session_gate(
            root=tmp_path / "receiver-gate", engine_id=ENGINE, native_id=sid
        ):
            pass
        with (
            pytest.raises(SourceFenced),
            session_gate(root=tmp_path / "gate", engine_id=ENGINE, native_id=sid),
        ):
            pytest.fail("second push source admitted")
        counts = (len(imports), len(overwrites))
        assert transfer.perform(cfg, direction="to", **common) == second
        assert (len(imports), len(overwrites)) == counts
    finally:
        unregister_persistence_adapter(ENGINE)


@pytest.mark.parametrize(
    "boundary", ["missing-peer", "peer", "project", "lineage", "activation"]
)
def test_receiver_reclaim_authority_and_receipt_replay(tmp_path, monkeypatch, boundary):
    import hashlib

    from untether.session_migration.receiver_protocol import ReceiverProtocolError
    from untether.session_migration.session_gate import session_gate
    from untether.session_migration.source_fence import SourceFenced, read_source_fence

    store = tmp_path / "destination"
    store.mkdir()
    sid = str(uuid4())
    _seed(store, sid)
    original = (store / (sid + ".json")).read_bytes()
    receiver = NativeReceiver(
        tmp_path / "native-state", max_archive=65536, max_expanded=65536
    )
    register_persistence_adapter(ENGINE, lambda: _adapter(store))
    try:
        # Real outbound evidence and completed fence authorize a later return.
        with monkeypatch.context() as patch:
            patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "receiver-gate"))
            outbound = str(uuid4())
            package = receiver.export_exact(
                sid,
                engine=ENGINE,
                operation_id=outbound,
                peer_name="local",
                project="demo",
            )
            receiver.complete_export(
                outbound,
                engine=ENGINE,
                session_id=sid,
                package_sha256=hashlib.sha256(package).hexdigest(),
                peer_name="local",
                project="demo",
            )
        _seed(store, sid, generation=3)
        if boundary == "lineage":
            path = store / (sid + ".json")
            data = json.loads(path.read_text())
            data["lineage"] = str(uuid4())
            path.write_text(json.dumps(data))
        cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
        incoming = transfer.build_inbound_package(
            cfg, _adapter(store).export_exact(sid)
        )
        (store / (sid + ".json")).write_bytes(original)
        peer = (
            None
            if boundary == "missing-peer"
            else ("foreign" if boundary == "peer" else "local")
        )
        call = _receiver_call(store, tmp_path, authorized_peer=peer)
        op = str(uuid4())
        kwargs = {
            "peer": _peer(),
            "project": "foreign" if boundary == "project" else "demo",
            "engine": ENGINE,
        }
        if boundary != "activation":
            reason = {
                "missing-peer": "RECLAIM_CONTEXT_REQUIRED",
                "peer": "RECLAIM_REFUSED: foreign peer or project",
                "project": "RECLAIM_REFUSED: foreign peer or project",
                "lineage": "DIVERGENT_LINEAGE",
            }[boundary]
            with pytest.raises(ReceiverProtocolError, match=reason):
                call(op, incoming, **kwargs)
            assert (store / (sid + ".json")).read_bytes() == original
            assert receiver.status_receipt(op) is None
            fence = read_source_fence(
                tmp_path / "receiver-gate" / "fences", engine=ENGINE, native_id=sid
            )
            assert fence is not None and fence.state == "completed"
            assert fence.operation_id == outbound
        else:

            def interrupted(*args, **kwargs):
                raise RuntimeError("injected activation interruption")

            with monkeypatch.context() as patch:
                patch.setattr(transfer, "activate_source_fence", interrupted)
                with pytest.raises(ReceiverProtocolError, match="injected activation"):
                    call(op, incoming, **kwargs)
            receipt = receiver.status_receipt(op)
            assert receipt is not None
            receipt_path = receiver.root / "receipts" / (op + ".json")
            receipt_bytes = receipt_path.read_bytes()
            restored = (store / (sid + ".json")).read_bytes()
            with (
                pytest.raises(SourceFenced),
                session_gate(
                    root=tmp_path / "receiver-gate", engine_id=ENGINE, native_id=sid
                ),
            ):
                pytest.fail("interrupted activation admitted the session")

            def forbidden(*args, **kwargs):
                pytest.fail("receipt replay attempted another import")

            monkeypatch.setattr(ExecutablePersistenceAdapter, "import_exact", forbidden)
            monkeypatch.setattr(
                ExecutablePersistenceAdapter, "import_overwrite", forbidden
            )
            reply = call(op, incoming, **kwargs)
            assert reply["package_sha256"] == receipt.package_sha256
            assert reply["status"] == "verified" and reply["native_id"] == sid
            assert receiver.status_receipt(op) == receipt
            assert receipt_path.read_bytes() == receipt_bytes
            assert (store / (sid + ".json")).read_bytes() == restored
            with session_gate(
                root=tmp_path / "receiver-gate", engine_id=ENGINE, native_id=sid
            ):
                pass
    finally:
        unregister_persistence_adapter(ENGINE)


def test_engine_and_session_identity_are_strict(tmp_path):
    store = tmp_path / "store"
    store.mkdir()
    sid = str(uuid4())
    _seed(store, sid, engine="other-engine")
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    register_persistence_adapter(ENGINE, lambda: _adapter(store))
    try:
        with pytest.raises(CapabilityUnavailable, match="ENGINE_MISMATCH"):
            transfer.perform(
                cfg,
                peer_name="vps",
                project="demo",
                session_id=sid,
                engine=ENGINE,
                direction="to",
                root=tmp_path / "ops",
                receiver_call=_receiver_call(tmp_path / "unused", tmp_path),
            )
        assert not (tmp_path / "ops" / "operations").exists()
        with pytest.raises(transfer.TransferRefusal, match="SESSION_ID_INVALID"):
            transfer.perform(
                cfg,
                peer_name="vps",
                project="demo",
                session_id="../../etc/passwd",
                engine=ENGINE,
                direction="to",
                root=tmp_path / "ops",
                receiver_call=_receiver_call(tmp_path / "unused", tmp_path),
            )
    finally:
        unregister_persistence_adapter(ENGINE)
