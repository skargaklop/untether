"""Executable serve subprocess: the receiver really restarts and resumes.

Each test drives a real `python native_receiver_serve.py` process. Importing
a package there spawns a further fresh engine subprocess per operation
(restart) and then re-exports it (resume) from the disjoint destination
store. The two sessions share one serve process and one store configuration;
the serve process is then terminated.
"""

from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from untether.session_migration.subprocess_adapter import ExecutablePersistenceAdapter

HERE = Path(__file__).parent
SERVE = HERE / "fixtures" / "native_receiver_serve.py"
ENGINE_FIXTURE = HERE / "fixtures" / "native_persistence_engine.py"
ENGINE = "fixture-engine"
PIN = "SHA256:" + "A" * 43


class ServeProcess:
    def __init__(self, store: Path):
        import os

        env = {
            key: value
            for key, value in os.environ.items()
            if key in ("SYSTEMROOT", "PATH", "COMSPEC", "TEMP", "TMP")
        }
        env["PYTHONUTF8"] = "1"
        env["PYTHONPATH"] = str(HERE.parent / "src")
        # Operation-bound export is fail-closed: the child's source adapters
        # must hold the real cross-process gate under an explicit tmp root.
        env["UNTETHER_SESSION_GATE_ROOT"] = str(store.parent / "gate")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-X",
                "utf8",
                str(SERVE),
                "--serve-protocol-v1",
                ENGINE,
                str(store),
                "--gate-root",
                env["UNTETHER_SESSION_GATE_ROOT"],
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=env,
        )

    def call(self, verb: str, payload: bytes) -> dict:
        request = json.dumps(
            {
                "verb": verb,
                "payload_base64": base64.b64encode(payload).decode(),
            }
        ).encode()
        assert self.proc.stdin is not None and self.proc.stdout is not None
        self.proc.stdin.write(str(len(request)).encode() + b"\n" + request)
        self.proc.stdin.flush()
        length = int(self.proc.stdout.readline().strip())
        return json.loads(self.proc.stdout.read(length))

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        self.proc.wait(timeout=10)


def _archive_bytes(store: Path, sid: str) -> bytes:
    from untether.session_migration.native_archive import encode_native

    adapter = ExecutablePersistenceAdapter(
        ENGINE, ENGINE_FIXTURE, store, interpreter=sys.executable
    )
    archive = adapter.export_exact(sid)
    return encode_native(
        engine=archive.engine,
        logical_id=archive.logical_id,
        native_id=archive.native_id,
        generation=archive.generation,
        lineage=archive.lineage,
        records=archive.records,
        max_bytes=1 << 20,
    )


def _frame(op: str, payload: bytes) -> bytes:
    header = json.dumps(
        {
            "protocol": 2,
            "action": "receive",
            "operation_id": op,
            "project": "demo",
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "engine": ENGINE,
        }
    ).encode()
    return header + b"\n" + payload


def _export_frame(op: str, sid: str) -> bytes:
    header = json.dumps(
        {
            "protocol": 2,
            "action": "export",
            "operation_id": op,
            "project": "demo",
            "size": 0,
            "sha256": hashlib.sha256(b"").hexdigest(),
            "engine": ENGINE,
            "session_id": sid,
        }
    ).encode()
    return header + b"\n"


def test_serve_subprocess_restarts_and_resumes_two_sessions(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    sids = [str(uuid4()), str(uuid4())]
    for sid in sids:
        payload = {
            "engine": ENGINE,
            "logical_id": sid,
            "native_id": sid,
            "generation": 3,
            "lineage": sid,
            "records": {
                "history/tool.ndjson": base64.b64encode(b"tool\r\nraw").decode(),
                "assets/a.bin": base64.b64encode(b"\x00\xff\xfe").decode(),
            },
        }
        (source / (sid + ".json")).write_text(json.dumps(payload))
    serve = ServeProcess(destination)
    try:
        for sid in sids:
            package = _archive_bytes(source, sid)
            reply = serve.call("receive", _frame(str(uuid4()), package))
            assert reply.get("status") == "verified", reply
            assert reply["native_id"] == sid
            assert reply["engine"] == ENGINE
        for sid in sids:
            exp_reply = serve.call("export", _export_frame(str(uuid4()), sid))
            assert exp_reply.get("status") == "exported", exp_reply
            assert exp_reply["session_id"] == sid
            assert exp_reply["engine"] == ENGINE
            raw_pkg = base64.b64decode(exp_reply["package_base64"])
            assert hashlib.sha256(raw_pkg).hexdigest() == exp_reply["package_sha256"]
            from untether.session_migration.native_archive import decode_native

            arch = decode_native(raw_pkg, max_archive=1 << 20, max_expanded=1 << 20)
            assert arch.records["assets/a.bin"] == b"\x00\xff\xfe"
            assert arch.records["history/tool.ndjson"] == b"tool\r\nraw"
            assert arch.logical_id == sid
        status = serve.call(
            "status",
            json.dumps(
                {
                    "protocol": 2,
                    "action": "status",
                    "operation_id": "00000000-0000-0000-0000-000000000000",
                    "project": "demo",
                    "size": 0,
                    "sha256": hashlib.sha256(b"").hexdigest(),
                    "engine": ENGINE,
                }
            ).encode(),
        )
        assert "error" in status  # status needs a real op id with a receipt
    finally:
        serve.close()
    assert serve.proc.returncode == 0
    # Independent fresh process proves the disjoint destination store resumed.
    resumed = ExecutablePersistenceAdapter(
        ENGINE, ENGINE_FIXTURE, destination, interpreter=sys.executable
    )
    for sid in sids:
        reloaded = resumed.export_exact(sid)
        assert reloaded.records["assets/a.bin"] == b"\x00\xff\xfe"
        assert reloaded.records["history/tool.ndjson"] == b"tool\r\nraw"
        assert reloaded.logical_id == sid and reloaded.lineage == sid
