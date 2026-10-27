"""SSH receiver RPC uses a fixed verb and rejects unverified replies."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from uuid import uuid4

import pytest

from untether.session_migration import receiver_protocol
from untether.session_migration import remote as remote_module
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    ReceiverProtocolError,
    handle_receiver,
)
from untether.session_migration.remote import inspect_session, receiver_request
from untether.session_migration.settings import MigrationPeer
from untether.session_migration.ssh_transport import (
    _REQUIRED_CHECKS,
    PreflightError,
)

PIN = "SHA256:" + "A" * 43
CHECKS = [
    "installed_receiver",
    "runtime_ready",
    "agent_installed",
    "agent_version_compatible",
    "adapter_capable",
    "provider_auth",
    "provider_connectivity",
    "staging_writable",
    "disk_headroom",
    "inodes_available",
    "workspace_git_parity",
    "quiescent",
    "telegram_enrolled",
    "distinct_bot",
    "destination_binding",
    "resume_probe_available",
    "container_ready",
    "container_uid",
    "mount_mapping",
]


def fixture_peer(tmp_path, mode):
    keys = tmp_path / "keys"
    keys.mkdir()
    (keys / "selected").write_text("test-only")
    known = tmp_path / "known"
    known.write_text("key")
    return MigrationPeer(
        host="example.invalid",
        user="operator",
        identity_dir=str(keys),
        identity_file="selected",
        known_hosts_file=str(known),
        host_key_fingerprint=PIN,
        receiver_path="/usr/local/bin/fixed-receiver",
        execution_mode=mode,
        expected_endpoint_id="endpoint-one",
        destination_binding="enrolled",
        projects={"demo": "/srv/demo"},
    )


@pytest.mark.parametrize("direction", ["to", "from"])
@pytest.mark.parametrize("mode", ["native", "container-wrapper"])
def test_binary_receiver_both_directions(tmp_path, mode, direction):
    peer = fixture_peer(tmp_path, mode)
    op = str(uuid4())
    archive = b"opaque context zip bytes"
    digest = hashlib.sha256(archive).hexdigest()
    transport_calls = []

    def fake_preflight(argv, **_kwargs):
        if argv[0] == "ssh-keygen":
            return subprocess.CompletedProcess(argv, 0, PIN, "")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "protocol": 1,
                    "endpoint_id": "endpoint-one",
                    "execution_mode": mode,
                    "destination_binding": "enrolled",
                    "os": "linux",
                    "arch": "x86_64",
                    "project": "demo",
                    "checks": dict.fromkeys(CHECKS, True),
                }
            ),
            "",
        )

    def fake_transport(argv, **kwargs):
        transport_calls.append((argv, kwargs["input"]))
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "protocol": 1,
                    "operation_id": op,
                    "endpoint_id": "endpoint-one",
                    "package_sha256": digest,
                    "status": "verified",
                    "native_id": "fresh-destination-id",
                    "importer_version": "1",
                }
            ).encode(),
            b"",
        )

    result = receiver_request(
        peer,
        direction=direction,
        project="demo",
        action="receive",
        operation_id=op,
        package=archive,
        run=fake_transport,
        preflight_run=fake_preflight,
    )
    assert result["native_id"] == "fresh-destination-id"
    args, payload = transport_calls[0]
    assert args[-1] == "/usr/local/bin/fixed-receiver receive"
    header, _, rest = payload.partition(b"\n")
    assert json.loads(header)["sha256"] == digest
    assert rest == archive
    assert "BatchMode=yes" in args
    assert "StrictHostKeyChecking=yes" in args


def test_reject_unverified_receipt(tmp_path):
    peer = fixture_peer(tmp_path, "native")
    with pytest.raises(PreflightError, match="RECEIVER_ACTION_FORBIDDEN"):
        receiver_request(
            peer,
            project="demo",
            direction="to",
            action="sh -c whoami",
            operation_id=str(uuid4()),
        )


def test_binary_receiver_export(tmp_path):
    import base64

    peer = fixture_peer(tmp_path, "native")
    op = str(uuid4())
    sid = "sess-remote-1"
    archive = b"exported binary package"
    digest = hashlib.sha256(archive).hexdigest()
    transport_calls = []

    def fake_preflight(argv, **_kwargs):
        if argv[0] == "ssh-keygen":
            return subprocess.CompletedProcess(argv, 0, PIN, "")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "protocol": 1,
                    "endpoint_id": "endpoint-one",
                    "execution_mode": "native",
                    "destination_binding": "enrolled",
                    "os": "linux",
                    "arch": "x86_64",
                    "project": "demo",
                    "checks": dict.fromkeys(CHECKS, True),
                }
            ),
            "",
        )

    def fake_transport(argv, **kwargs):
        transport_calls.append((argv, kwargs["input"]))
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "protocol": 1,
                    "operation_id": op,
                    "endpoint_id": "endpoint-one",
                    "session_id": sid,
                    "engine": "fixture-engine",
                    "package_sha256": digest,
                    "package_base64": base64.b64encode(archive).decode(),
                    "status": "exported",
                }
            ).encode(),
            b"",
        )

    result = receiver_request(
        peer,
        direction="from",
        project="demo",
        action="export",
        operation_id=op,
        session_id=sid,
        engine="fixture-engine",
        run=fake_transport,
        preflight_run=fake_preflight,
    )
    assert result["status"] == "exported"
    assert result["package"] == archive
    args, payload = transport_calls[0]
    assert args[-1] == "/usr/local/bin/fixed-receiver export"
    header, _, rest = payload.partition(b"\n")
    info = json.loads(header)
    assert info["action"] == "export"
    assert info["session_id"] == sid
    assert info["engine"] == "fixture-engine"
    assert rest == b""


class _StubNativeReceiver(NativeReceiver):
    def __init__(self, max_archive):
        self.max_archive = max_archive
        self.exported = None

    def export_exact(
        self, session_id, *, engine, operation_id=None, peer_name=None, project=None
    ):
        self.exported = (session_id, engine, operation_id)
        return b"x" * (self.max_archive + 1)


def _fake_preflight(argv, **_kwargs):
    if argv[0] == "ssh-keygen":
        return subprocess.CompletedProcess(argv, 0, PIN, "")
    return subprocess.CompletedProcess(
        argv,
        0,
        json.dumps(
            {
                "protocol": 1,
                "endpoint_id": "endpoint-one",
                "execution_mode": "native",
                "destination_binding": "enrolled",
                "os": "linux",
                "arch": "x86_64",
                "project": "demo",
                "checks": dict.fromkeys(CHECKS, True),
            }
        ),
        "",
    )


def test_inspect_session_exact_frame_and_pinned_transport(tmp_path):
    peer = fixture_peer(tmp_path, "native")
    calls = []

    def transport(argv, **kwargs):
        frame = kwargs["input"]
        header = json.loads(frame)
        from uuid import UUID

        assert str(UUID(header["operation_id"])) == header["operation_id"]
        assert header == {
            "protocol": 1,
            "action": "inspect",
            "project": "demo",
            "session_id": "sid-1",
            "operation_id": header["operation_id"],
        }
        assert frame.endswith(b"\n") and frame.count(b"\n") == 1
        assert argv[-1] == "/usr/local/bin/fixed-receiver inspect"
        assert "StrictHostKeyChecking=yes" in argv
        assert kwargs["timeout"] == 7
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "protocol": 1,
                    "endpoint_id": "endpoint-one",
                    "operation_id": header["operation_id"],
                    "project": "demo",
                    "session_id": "sid-1",
                    "engine": "omp",
                    "status": "found",
                }
            ).encode(),
            b"",
        )

    assert (
        inspect_session(
            peer,
            "demo",
            "sid-1",
            timeout_s=7,
            run=transport,
            preflight_run=_fake_preflight,
        )
        == "omp"
    )
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("protocol", True),
        ("protocol", 1.0),
        ("protocol", 2),
        ("endpoint_id", "foreign"),
        ("operation_id", "foreign"),
        ("project", "foreign"),
        ("session_id", "foreign"),
        ("engine", "OMP"),
        ("engine", ""),
        ("engine", "a" * 65),
        ("engine", None),
        ("engine", "../omp"),
        ("status", "missing"),
        ("path", "/private/session"),
    ],
)
def test_inspect_rejects_wrong_or_extra_reply_fields(tmp_path, field, value):
    peer = fixture_peer(tmp_path, "native")

    def transport(argv, **kwargs):
        header = json.loads(kwargs["input"])
        reply = {
            "protocol": 1,
            "endpoint_id": "endpoint-one",
            "operation_id": header["operation_id"],
            "project": "demo",
            "session_id": "sid-1",
            "engine": "omp",
            "status": "found",
        }
        reply[field] = value
        return subprocess.CompletedProcess(argv, 0, json.dumps(reply).encode(), b"")

    with pytest.raises(PreflightError):
        inspect_session(
            peer, "demo", "sid-1", run=transport, preflight_run=_fake_preflight
        )


@pytest.mark.parametrize(
    "payload",
    [
        b'{"protocol":1,"protocol":1}',
        b"{}",
        b"[]",
        b"x" * 16385,
    ],
)
def test_inspect_rejects_duplicate_missing_and_oversize_reply(tmp_path, payload):
    peer = fixture_peer(tmp_path, "native")

    def transport(argv, **_kwargs):
        return subprocess.CompletedProcess(argv, 0, payload, b"")

    with pytest.raises(PreflightError):
        inspect_session(
            peer, "demo", "sid-1", run=transport, preflight_run=_fake_preflight
        )


@pytest.mark.parametrize(
    "extra",
    [
        {"session_id": None},
        {"session_id": ""},
        {"engine": "omp"},
        {"package": b"unexpected"},
    ],
)
def test_inspect_invalid_request_never_reaches_preflight(tmp_path, extra):
    peer = fixture_peer(tmp_path, "native")
    kwargs = {"session_id": "sid-1", **extra}

    def forbidden(*_args, **_kwargs):
        pytest.fail("invalid request reached transport")

    with pytest.raises(PreflightError):
        receiver_request(
            peer,
            project="demo",
            direction="from",
            action="inspect",
            operation_id=str(uuid4()),
            run=forbidden,
            preflight_run=forbidden,
            session_id=kwargs.get("session_id"),
            engine=extra.get("engine"),
            package=extra.get("package", b""),
        )


@pytest.mark.parametrize(
    "field",
    [
        "protocol",
        "endpoint_id",
        "operation_id",
        "project",
        "session_id",
        "engine",
        "status",
    ],
)
@pytest.mark.parametrize("mutation", ["missing", "wrong-type", "duplicate"])
def test_inspect_every_reply_field_is_required_unique_and_typed(
    tmp_path, field, mutation
):
    peer = fixture_peer(tmp_path, "native")

    def transport(argv, **kwargs):
        header = json.loads(kwargs["input"])
        reply = {
            "protocol": 1,
            "endpoint_id": "endpoint-one",
            "operation_id": header["operation_id"],
            "project": "demo",
            "session_id": "sid-1",
            "engine": "omp",
            "status": "found",
        }
        if mutation == "missing":
            del reply[field]
        elif mutation == "wrong-type":
            reply[field] = []
        payload = json.dumps(reply).encode()
        if mutation == "duplicate":
            payload = (
                payload[:-1]
                + b","
                + json.dumps(field).encode()
                + b":"
                + json.dumps(reply[field]).encode()
                + b"}"
            )
        return subprocess.CompletedProcess(argv, 0, payload, b"")

    with pytest.raises(PreflightError):
        inspect_session(
            peer, "demo", "sid-1", run=transport, preflight_run=_fake_preflight
        )


def test_inspect_timeout_is_fail_closed(tmp_path):
    peer = fixture_peer(tmp_path, "native")

    def transport(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    with pytest.raises(PreflightError, match="RECEIVER_UNREACHABLE"):
        inspect_session(
            peer, "demo", "sid-1", run=transport, preflight_run=_fake_preflight
        )


def _patch_child_process(monkeypatch, child_code):
    real_popen = subprocess.Popen

    def patched(argv, **kwargs):
        return real_popen([sys.executable, "-c", child_code], **kwargs)

    monkeypatch.setattr(remote_module.subprocess, "Popen", patched)


def _export_frame(operation_id, engine, session_id):
    header = {
        "protocol": 2,
        "action": "export",
        "operation_id": operation_id,
        "project": "demo",
        "size": 0,
        "sha256": hashlib.sha256(b"").hexdigest(),
        "engine": engine,
        "session_id": session_id,
    }
    return json.dumps(header).encode() + b"\n"


def test_oversize_export_refused_before_base64_encode(monkeypatch):
    stub = _StubNativeReceiver(max_archive=1024)
    op = str(uuid4())
    endpoint = ReceiverEndpoint(
        endpoint_id="endpoint-one",
        execution_mode="native",
        destination_binding="enrolled",
        project="demo",
        deployment="direct-linux",
        arch="x86_64",
        probe_health=lambda: dict.fromkeys(_REQUIRED_CHECKS, True),
        native_receiver=stub,
    )

    def _forbidden_encode(*_args, **_kwargs):
        raise AssertionError("base64 encode ran before the size precheck")

    monkeypatch.setattr(receiver_protocol.base64, "b64encode", _forbidden_encode)
    with pytest.raises(ReceiverProtocolError, match="EXPORT_PACKAGE_TOO_LARGE"):
        handle_receiver(
            endpoint,
            verb="export",
            payload=_export_frame(op, "fixture-engine", "sid-1"),
        )
    assert stub.exported == ("sid-1", "fixture-engine", op)


@pytest.mark.parametrize(
    ("extra", "expected"),
    [(0, "RECEIVER_REPLY_MALFORMED"), (1, "RECEIVER_REPLY_OVERSIZE")],
)
def test_export_reply_cap_is_max_times_four_thirds_plus_16k(
    tmp_path, monkeypatch, extra, expected
):
    peer = fixture_peer(tmp_path, "native")
    cap = 12 * 4 // 3 + 16384  # 16400
    _patch_child_process(
        monkeypatch, f"import sys; sys.stdout.buffer.write(b'a' * {cap + extra})"
    )
    with pytest.raises(PreflightError, match=expected):
        receiver_request(
            peer,
            direction="from",
            project="demo",
            action="export",
            operation_id=str(uuid4()),
            session_id="s",
            engine="e",
            max_package_bytes=12,
            timeout_s=20,
            preflight_run=_fake_preflight,
        )


def test_malicious_flood_then_sleep_child_killed_before_completion(
    tmp_path, monkeypatch
):
    peer = fixture_peer(tmp_path, "native")
    marker = tmp_path / "completed.marker"
    child = (
        "import sys, time\n"
        "sys.stdout.buffer.write(b'x' * (4 * 1024 * 1024))\n"
        "sys.stdout.buffer.flush()\n"
        f"open(r'{marker}', 'w').write('done')\n"
        "time.sleep(60)\n"
    )
    _patch_child_process(monkeypatch, child)
    started = time.monotonic()
    with pytest.raises(PreflightError, match="RECEIVER_REPLY_OVERSIZE"):
        receiver_request(
            peer,
            direction="to",
            project="demo",
            action="status",
            operation_id=str(uuid4()),
            timeout_s=20,
            preflight_run=_fake_preflight,
        )
    assert time.monotonic() - started < 15
    assert not marker.exists()


def test_silent_sleeping_child_killed_at_deadline(tmp_path, monkeypatch):
    peer = fixture_peer(tmp_path, "native")
    _patch_child_process(monkeypatch, "import time; time.sleep(60)")
    started = time.monotonic()
    with pytest.raises(PreflightError, match="RECEIVER_UNREACHABLE"):
        receiver_request(
            peer,
            direction="to",
            project="demo",
            action="status",
            operation_id=str(uuid4()),
            timeout_s=3,
            preflight_run=_fake_preflight,
        )
    assert time.monotonic() - started < 13


def test_child_closes_stdout_before_exit_zero_succeeds(tmp_path, monkeypatch):
    peer = fixture_peer(tmp_path, "native")
    op = str(uuid4())
    zeros = "0" * 64
    child = (
        "import json, sys, time\n"
        "time.sleep(0.1)\n"
        f"reply = json.dumps({{'protocol': 1, 'operation_id': {op!r},"
        f" 'endpoint_id': 'endpoint-one', 'package_sha256': {zeros!r}}}).encode()\n"
        "sys.stdout.buffer.write(reply)\n"
        "sys.stdout.buffer.flush()\n"
        "sys.stdout.close()\n"
        "time.sleep(0.1)\n"
    )
    _patch_child_process(monkeypatch, child)
    result = receiver_request(
        peer,
        direction="to",
        project="demo",
        action="status",
        operation_id=op,
        timeout_s=5,
        preflight_run=_fake_preflight,
    )
    assert result["operation_id"] == op


def test_child_closes_stdout_then_hang_times_out_bounded(tmp_path, monkeypatch):
    peer = fixture_peer(tmp_path, "native")
    _patch_child_process(
        monkeypatch,
        "import sys, time\n"
        "sys.stdout.buffer.write(b'ok')\n"
        "sys.stdout.buffer.flush()\n"
        "sys.stdout.close()\n"
        "time.sleep(60)\n",
    )
    started = time.monotonic()
    with pytest.raises(PreflightError, match="RECEIVER_UNREACHABLE"):
        receiver_request(
            peer,
            direction="to",
            project="demo",
            action="status",
            operation_id=str(uuid4()),
            timeout_s=3,
            preflight_run=_fake_preflight,
        )
    assert time.monotonic() - started < 8
