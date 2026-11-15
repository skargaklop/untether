"""Two distinct source sessions through one fixed adapter, separate stores."""

from __future__ import annotations

import base64
import json
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest

from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    assert_exact_restore,
)
from untether.session_migration.subprocess_adapter import ExecutablePersistenceAdapter

FIXTURE = Path(__file__).parent / "fixtures" / "native_persistence_engine.py"


def test_two_sessions_independent_processes(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    exporter = ExecutablePersistenceAdapter(
        "fixture", FIXTURE, source, interpreter=sys.executable
    )
    importer = ExecutablePersistenceAdapter(
        "fixture", FIXTURE, destination, interpreter=sys.executable
    )
    for _ in range(2):
        sid = str(uuid4())
        original = {
            "engine": "fixture",
            "logical_id": sid,
            "native_id": sid,
            "generation": 2,
            "lineage": sid,
            "records": {
                "history/tool.ndjson": base64.b64encode(b"tool\r\nraw").decode(),
                "assets/a.bin": base64.b64encode(b"\x00\xff\xfe").decode(),
            },
        }
        (source / (sid + ".json")).write_text(json.dumps(original))
        exported = exporter.export_exact(sid)
        proof = importer.import_exact(exported)
        assert assert_exact_restore(exported, proof) == sid
        independently_loaded = importer.export_exact(sid)
        assert independently_loaded.records == exported.records
        assert independently_loaded.logical_id == sid
        assert independently_loaded.lineage == sid


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("engine", "foreign"),
        ("logical_id", "foreign"),
        ("native_id", "foreign"),
        ("generation", 99),
        ("lineage", "foreign"),
        ("restarted", False),
        ("resumed", False),
    ],
)
def test_overwrite_rejects_matching_hashes_with_invalid_restore_proof(
    tmp_path, monkeypatch, field, bad_value
):
    import hashlib

    adapter = ExecutablePersistenceAdapter(
        "fixture", FIXTURE, tmp_path, interpreter=sys.executable
    )
    reply = {
        "engine": "fixture",
        "logical_id": "session",
        "native_id": "session",
        "generation": 2,
        "lineage": "session",
        "restarted": True,
        "resumed": True,
        "record_hashes": {"record": hashlib.sha256(b"new").hexdigest()},
    }
    reply[field] = bad_value
    monkeypatch.setattr(ExecutablePersistenceAdapter, "_call", lambda *args: reply)
    with pytest.raises(CapabilityUnavailable, match="PERSISTENCE_PROOF_INVALID"):
        adapter.import_overwrite(
            engine="fixture",
            native_id="session",
            records={"record": b"new"},
            expected_prior_records={"record": b"old"},
            generation=2,
            lineage="session",
        )


@pytest.mark.parametrize(
    ("action", "code", "expected"),
    [
        ("import_and_restart_verify", 3, "SESSION_ID_COLLISION"),
        ("overwrite_and_restart_verify", 5, "OVERWRITE_PRIOR_MISMATCH"),
        ("overwrite_and_restart_verify", 6, "OVERWRITE_PRIOR_MISMATCH"),
        ("export", 3, "PERSISTENCE_ADAPTER_REFUSED"),
        ("export", 6, "PERSISTENCE_ADAPTER_REFUSED"),
        ("import_and_restart_verify", 6, "PERSISTENCE_ADAPTER_REFUSED"),
    ],
)
def test_protocol_refusal_codes_are_action_scoped(
    tmp_path, monkeypatch, action, code, expected
):
    from untether.session_migration import subprocess_adapter

    adapter = ExecutablePersistenceAdapter(
        "fixture", FIXTURE, tmp_path, interpreter=sys.executable
    )
    monkeypatch.setattr(
        subprocess_adapter, "_bounded_run", lambda *a, **kw: (code, b"")
    )
    with pytest.raises(CapabilityUnavailable, match=expected):
        adapter._call(action, {})


def test_export_snapshot_requires_gate_root(tmp_path, monkeypatch):
    monkeypatch.delenv("UNTETHER_SESSION_GATE_ROOT", raising=False)
    store = tmp_path / "store"
    store.mkdir()
    adapter = ExecutablePersistenceAdapter(
        "fixture", FIXTURE, store, interpreter=sys.executable
    )
    with (
        pytest.raises(CapabilityUnavailable) as excinfo,
        adapter.export_snapshot(str(uuid4())),
    ):
        pass
    assert excinfo.value.args[0] == "SOURCE_GATE_UNCONFIGURED"


def test_export_snapshot_yields_archive_while_gate_held(tmp_path, monkeypatch):
    gate_root = tmp_path / "gate"
    gate_root.mkdir()
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(gate_root))
    store = tmp_path / "store"
    store.mkdir()
    sid = str(uuid4())
    original = {
        "engine": "fixture",
        "logical_id": sid,
        "native_id": sid,
        "generation": 1,
        "lineage": sid,
        "records": {
            "history/tool.ndjson": base64.b64encode(b"held").decode(),
        },
    }
    (store / (sid + ".json")).write_text(json.dumps(original))
    adapter = ExecutablePersistenceAdapter(
        "fixture", FIXTURE, store, interpreter=sys.executable
    )
    with adapter.export_snapshot(sid) as archive:
        assert archive.logical_id == sid
        assert archive.engine == "fixture"
        assert archive.records == {"history/tool.ndjson": b"held"}
        assert list(gate_root.glob("gate-*.lock"))
    assert not list(gate_root.glob("gate-*.lock"))


def test_response_over_cap_kills_child_and_rejects_early(tmp_path):
    script = tmp_path / "flooder.py"
    script.write_text(
        "import sys, time\n"
        "sys.stdout.write('x' * (7 * 1024 * 1024))\n"
        "sys.stdout.flush()\n"
        "time.sleep(30)\n"
    )
    store = tmp_path / "store"
    store.mkdir()
    adapter = ExecutablePersistenceAdapter(
        "fixture",
        script,
        store,
        timeout=15.0,
        interpreter=sys.executable,
        max_response_bytes=1024 * 1024,
    )
    started = time.monotonic()
    with pytest.raises(CapabilityUnavailable) as excinfo:
        adapter.export_exact(str(uuid4()))
    assert excinfo.value.args[0] == "PERSISTENCE_RESPONSE_TOO_LARGE"
    assert time.monotonic() - started < 15.0


def test_silent_child_times_out_bounded(tmp_path):
    script = tmp_path / "silent.py"
    script.write_text("import time; time.sleep(30)\n")
    store = tmp_path / "store"
    store.mkdir()
    adapter = ExecutablePersistenceAdapter(
        "fixture",
        script,
        store,
        timeout=2.0,
        interpreter=sys.executable,
    )
    started = time.monotonic()
    with pytest.raises(CapabilityUnavailable) as excinfo:
        adapter.export_exact(str(uuid4()))
    assert excinfo.value.args[0] == "PERSISTENCE_ADAPTER_UNAVAILABLE"
    assert time.monotonic() - started < 10.0


def test_nonpositive_limits_rejected(tmp_path):
    store = tmp_path / "store"
    store.mkdir()
    for response_limit, request_limit in ((0, 1024), (1024, -1)):
        with pytest.raises(CapabilityUnavailable):
            ExecutablePersistenceAdapter(
                "fixture",
                tmp_path / "unused",
                store,
                max_response_bytes=response_limit,
                max_request_bytes=request_limit,
            )


def test_valid_adapter_within_small_cap(tmp_path):
    script = tmp_path / "valid.py"
    script.write_text(
        "import json, sys\nsys.stdout.write(json.dumps({'version': 1}))\n"
    )
    store = tmp_path / "store"
    store.mkdir()
    adapter = ExecutablePersistenceAdapter(
        "fixture",
        script,
        store,
        interpreter=sys.executable,
        max_response_bytes=64 * 1024,
    )
    assert adapter._call("status", {}) == {"version": 1}
