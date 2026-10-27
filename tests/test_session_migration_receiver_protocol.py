"""Receiver protocol over framed bytes, with no root, Docker or SSH access."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from uuid import uuid4

import pytest

from untether.session_migration.package import pack_context
from untether.session_migration.receiver import StagingReceiver
from untether.session_migration.receiver_protocol import (
    MAX_HEADER,
    ReceiverEndpoint,
    ReceiverProtocolError,
    handle_receiver,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS


def make_endpoint(tmp_path, mode):
    receiver = StagingReceiver(
        tmp_path / "state",
        max_archive=8192,
        max_expanded=16384,
        min_free_bytes=1,
    )
    checks = dict.fromkeys(_REQUIRED_CHECKS, True)
    checks.update(container_ready=True, container_uid=True, mount_mapping=True)
    endpoint = ReceiverEndpoint(
        endpoint_id="receiver-1",
        execution_mode=mode,
        destination_binding="enrolled",
        project="demo",
        deployment="direct-linux" if mode == "native" else "container",
        arch="x86_64",
        receiver=receiver,
        probe_health=lambda: checks,
        import_and_probe=lambda _pkg: "new-session",
    )
    return endpoint, checks


def frame(operation_id, archive, action):
    return (
        json.dumps(
            {
                "protocol": 1,
                "action": action,
                "operation_id": operation_id,
                "project": "demo",
                "size": len(archive),
                "sha256": hashlib.sha256(archive).hexdigest(),
            }
        ).encode()
        + b"\n"
        + archive
    )


@pytest.mark.parametrize("mode", ["native", "container-wrapper"])
@pytest.mark.parametrize("direction", ["to", "from"])
def test_receiver_same_protocol_for_portable_deployments(tmp_path, mode, direction):
    endpoint, _checks = make_endpoint(tmp_path / direction, mode)
    health = json.loads(handle_receiver(endpoint, verb="inspect"))
    assert health["execution_mode"] == mode
    archive = pack_context(engine="acp", project="demo", content=b"x", max_bytes=8192)
    op = str(uuid4())
    received = json.loads(
        handle_receiver(
            endpoint,
            verb="receive",
            payload=frame(op, archive, "receive"),
        )
    )
    assert received["status"] == "verified"
    assert received["native_id"] == "new-session"
    status = json.loads(
        handle_receiver(
            endpoint,
            verb="status",
            payload=frame(op, b"", "status"),
        )
    )
    assert status["status"] == "verified"


def test_fail_closed_if_provider_not_healthy(tmp_path):
    endpoint, checks = make_endpoint(tmp_path, "native")
    checks["provider_connectivity"] = False
    with pytest.raises(ReceiverProtocolError, match="provider_connectivity"):
        handle_receiver(endpoint, verb="inspect")


def test_reject_archive_tampering(tmp_path):
    endpoint, _checks = make_endpoint(tmp_path, "native")
    op = str(uuid4())
    archive = pack_context(engine="acp", project="demo", content=b"x", max_bytes=8192)
    data = frame(op, archive, "receive")
    with pytest.raises(ReceiverProtocolError, match="FRAME_HASH_OR_SIZE_MISMATCH"):
        handle_receiver(endpoint, verb="receive", payload=data[:-1] + b"z")


def test_refuses_when_no_proven_importer(tmp_path):
    endpoint, _checks = make_endpoint(tmp_path, "native")
    endpoint = ReceiverEndpoint(
        endpoint_id=endpoint.endpoint_id,
        execution_mode=endpoint.execution_mode,
        destination_binding=endpoint.destination_binding,
        project=endpoint.project,
        deployment=endpoint.deployment,
        arch=endpoint.arch,
        receiver=endpoint.receiver,
        probe_health=endpoint.probe_health,
        import_and_probe=None,
    )
    blob = pack_context(engine="acp", project="demo", content=b"x", max_bytes=8192)
    with pytest.raises(ReceiverProtocolError, match="ADAPTER_UNAVAILABLE"):
        handle_receiver(
            endpoint, verb="receive", payload=frame(str(uuid4()), blob, "receive")
        )


def test_receiver_protocol_export(tmp_path, monkeypatch):
    import base64
    from contextlib import contextmanager

    from untether.session_migration.native_adapter import (
        register_persistence_adapter,
        unregister_persistence_adapter,
    )
    from untether.session_migration.native_receiver import NativeReceiver
    from untether.session_migration.session_gate import session_gate

    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gates"))

    class DummyAdapter:
        engine = "mock"

        @contextmanager
        def export_snapshot(self, sid):
            with session_gate(
                root=tmp_path / "gates", engine_id=self.engine, native_id=sid
            ):
                yield self.export_exact(sid)

        def export_exact(self, session_id):
            from untether.session_migration.native_archive import NativeArchive

            return NativeArchive(
                engine="mock",
                logical_id=session_id,
                native_id=session_id,
                generation=1,
                lineage="lineage-1",
                records={"data.json": b'{"a":1}'},
                digest="abc",
            )

        def import_exact(self, archive):
            raise NotImplementedError

    register_persistence_adapter("mock", DummyAdapter)
    try:
        native = NativeReceiver(
            tmp_path / "native", max_archive=8192, max_expanded=8192
        )
        checks = dict.fromkeys(_REQUIRED_CHECKS, True)
        endpoint = ReceiverEndpoint(
            endpoint_id="receiver-1",
            execution_mode="native",
            destination_binding="enrolled",
            project="demo",
            deployment="direct-linux",
            arch="x86_64",
            probe_health=lambda: checks,
            native_receiver=native,
        )
        op = str(uuid4())
        export_frame = (
            json.dumps(
                {
                    "protocol": 2,
                    "action": "export",
                    "operation_id": op,
                    "project": "demo",
                    "size": 0,
                    "sha256": hashlib.sha256(b"").hexdigest(),
                    "engine": "mock",
                    "session_id": "sess-export-1",
                }
            ).encode()
            + b"\n"
        )
        resp = json.loads(
            handle_receiver(endpoint, verb="export", payload=export_frame)
        )
        assert resp["status"] == "exported"
        assert resp["session_id"] == "sess-export-1"
        assert resp["engine"] == "mock"
        pkg = base64.b64decode(resp["package_base64"])
        assert hashlib.sha256(pkg).hexdigest() == resp["package_sha256"]
    finally:
        unregister_persistence_adapter("mock")


def inspect_frame(**changes):
    info = {
        "protocol": 1,
        "action": "inspect",
        "operation_id": str(uuid4()),
        "project": "demo",
        "session_id": "sess-1",
    }
    info.update(changes)
    return json.dumps(info).encode() + b"\n"


@pytest.fixture
def scoped_endpoint(tmp_path, monkeypatch):
    from pathlib import PurePosixPath, PureWindowsPath

    from untether.session_migration import receiver_protocol

    endpoint, checks = make_endpoint(tmp_path, "native")
    calls = []

    class Adapter:
        def lookup_for_project(self, sid, root):
            calls.append((sid, root))
            assert isinstance(root, (PurePosixPath, PureWindowsPath))
            if sid not in ("sess-1", "sess-2"):
                raise ValueError("private /path and prompt contents")
            return sid

        def export_exact(self, sid):
            raise AssertionError("inspect must not export")

        def lookup_exact(self, sid):
            raise AssertionError("inspect must not use unscoped lookup")

    adapter = Adapter()

    def require(engine):
        assert engine == "configured"
        return adapter

    monkeypatch.setattr(receiver_protocol, "require_persistence_adapter", require)
    endpoint = dataclasses.replace(
        endpoint,
        engine="configured",
        project_root="/work/demo",
        native_receiver=DummyNative(),
    )
    return endpoint, checks, calls, adapter


@pytest.mark.parametrize("sid", ["sess-1", "sess-2"])
@pytest.mark.parametrize("root", ["/work/demo", "C:\\work\\demo"])
def test_targeted_inspect_narrow_response(scoped_endpoint, sid, root):
    from pathlib import PurePosixPath, PureWindowsPath

    endpoint, _, calls, _ = scoped_endpoint
    endpoint = dataclasses.replace(endpoint, project_root=root)
    payload = inspect_frame(session_id=sid)
    result = json.loads(handle_receiver(endpoint, verb="inspect", payload=payload))
    assert result == {
        "protocol": 1,
        "endpoint_id": "receiver-1",
        "operation_id": json.loads(payload)["operation_id"],
        "project": "demo",
        "session_id": sid,
        "engine": "configured",
        "status": "found",
    }
    path_type = PureWindowsPath if root.startswith("C:") else PurePosixPath
    assert calls == [(sid, path_type(root))]
    assert type(calls[0][1]) is path_type


@pytest.mark.parametrize(
    "changes",
    [
        {"project": "foreign"},
        {"session_id": "foreign"},
        {"engine": "foreign"},
        {"extra": True},
        {"protocol": True},
        {"protocol": 2},
        {"action": "export"},
        {"operation_id": "bad"},
        {"operation_id": str(uuid4()).upper()},
        {"session_id": ""},
        {"session_id": "a" * 129},
        {"session_id": "../secret"},
        {"session_id": "a\\b"},
        {"session_id": "C:secret"},
        {"session_id": "a\u007f"},
        {"session_id": 1},
    ],
)
def test_targeted_inspect_refuses_metadata(scoped_endpoint, changes):
    endpoint, _, _, _ = scoped_endpoint
    with pytest.raises(ReceiverProtocolError, match=r"^SESSION_PROJECT_REFUSED$"):
        handle_receiver(endpoint, verb="inspect", payload=inspect_frame(**changes))


@pytest.mark.parametrize(
    "payload",
    [
        b"{}\n",
        b"[]\n",
        b"null\n",
        b"not json\n",
        b"\xff\n",
        b"{}",
        b"{}\nbody",
        b"{}\n\n",
        b" " * MAX_HEADER + b"\n",
        b'{"protocol":1,"protocol":1}\n',
    ],
)
def test_targeted_inspect_refuses_frames(scoped_endpoint, payload):
    endpoint, _, calls, _ = scoped_endpoint
    with pytest.raises(ReceiverProtocolError, match=r"^SESSION_PROJECT_REFUSED$"):
        handle_receiver(endpoint, verb="inspect", payload=payload)
    assert calls == []


@pytest.mark.parametrize(
    "changes",
    [
        {"project_root": None},
        {"project_root": ""},
        {"engine": ""},
        {"native_receiver": None},
    ],
)
def test_targeted_inspect_requires_configuration(scoped_endpoint, changes):
    endpoint, _, calls, _ = scoped_endpoint
    with pytest.raises(ReceiverProtocolError, match=r"^SESSION_PROJECT_REFUSED$"):
        handle_receiver(
            dataclasses.replace(endpoint, **changes),
            verb="inspect",
            payload=inspect_frame(),
        )
    assert calls == []


def test_targeted_inspect_no_capability_or_health_bypass(scoped_endpoint, monkeypatch):
    endpoint, checks, calls, adapter = scoped_endpoint
    monkeypatch.setattr(adapter, "lookup_for_project", None)
    with pytest.raises(ReceiverProtocolError, match=r"^SESSION_PROJECT_REFUSED$"):
        handle_receiver(endpoint, verb="inspect", payload=inspect_frame())
    checks["provider_connectivity"] = False
    with pytest.raises(ReceiverProtocolError, match="HEALTH_FAILED"):
        handle_receiver(endpoint, verb="inspect", payload=inspect_frame())
    assert calls == []


class DummyNative:
    """Bound-guard witness: any call here means the guard fired too late."""

    max_archive = 8192

    def receive(self, operation_id, content, *, engine):
        raise AssertionError("adapter called despite ENGINE_ENDPOINT_MISMATCH")

    def export_exact(self, session_id, *, engine, operation_id):
        raise AssertionError("adapter called despite ENGINE_ENDPOINT_MISMATCH")


def _frame2(operation_id, body, action, *, engine, session_id=None):
    info = {
        "protocol": 2,
        "action": action,
        "operation_id": operation_id,
        "project": "demo",
        "size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "engine": engine,
    }
    if session_id is not None:
        info["session_id"] = session_id
    return json.dumps(info).encode() + b"\n" + body


@pytest.mark.parametrize("verb", ["export", "receive"])
def test_engine_endpoint_mismatch_fails_before_adapter(tmp_path, verb):
    endpoint, _checks = make_endpoint(tmp_path / verb, "native")
    endpoint = dataclasses.replace(
        endpoint, engine="expected", native_receiver=DummyNative()
    )
    if verb == "export":
        payload = _frame2(
            str(uuid4()), b"", "export", engine="foreign", session_id="sess-1"
        )
    else:
        payload = _frame2(str(uuid4()), b"foreign-blob", "receive", engine="foreign")
    with pytest.raises(ReceiverProtocolError, match="ENGINE_ENDPOINT_MISMATCH"):
        handle_receiver(endpoint, verb=verb, payload=payload)
