import base64
import hashlib
import inspect
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from untether.cli import migration
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    RestoreProof,
    register_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_archive import decode_native, encode_native
from untether.session_migration.settings import (
    ReceiverSettings,
    SessionMigrationSettings,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS


@pytest.mark.parametrize("root", [None, "/srv/authoritative", r"C:\Projects\demo"])
def test_receiver_endpoint_uses_only_configured_root(tmp_path, root):
    cfg = SessionMigrationSettings(
        receiver=ReceiverSettings(
            endpoint_id="fixture",
            engine="fixture",
            project="demo",
            project_root=root,
            state_dir=str(tmp_path / "state"),
            destination_binding="enrolled",
            health_argv=("health",),
        )
    )
    endpoint = migration._build_receiver_endpoint(
        engine="fixture", project="demo", cfg=cfg
    )
    assert endpoint.project_root == root
    with pytest.raises(CapabilityUnavailable, match="RECEIVER_SCOPE_MISMATCH"):
        migration._build_receiver_endpoint(
            engine="fixture", project="chat-supplied", cfg=cfg
        )
    assert (
        "project_root"
        not in inspect.signature(migration._build_receiver_endpoint).parameters
    )


def test_native_receive_through_cli(monkeypatch, tmp_path):
    seen = []

    class Adapter:
        engine = "fixture"

        def export_exact(self, session_id):
            raise AssertionError("receive must not export")

        def import_exact(self, archive):
            seen.append(archive)
            return RestoreProof(
                archive.logical_id,
                archive.native_id,
                archive.generation,
                archive.lineage,
                {k: hashlib.sha256(v).hexdigest() for k, v in archive.records.items()},
                True,
                True,
            )

    cfg = SessionMigrationSettings(
        receiver=ReceiverSettings(
            endpoint_id="fixture",
            engine="fixture",
            project="demo",
            state_dir=str(tmp_path / "state"),
            destination_binding="enrolled",
            health_argv=(
                sys.executable,
                "-c",
                "print("
                + repr(json.dumps(dict.fromkeys(_REQUIRED_CHECKS, True)))
                + ")",
            ),
        )
    )
    cfg.enabled = True  # isolate receiver from initiator peer validation
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda: (SimpleNamespace(session_migration=cfg), None),
    )
    monkeypatch.setattr(
        migration, "register_from_settings", lambda cfg, *, engine: None
    )
    register_persistence_adapter("fixture", Adapter)
    try:
        for sid in ("first", "second"):
            package = encode_native(
                engine="fixture",
                logical_id=sid,
                native_id=sid,
                generation=1,
                lineage=sid,
                records={"events.jsonl": sid.encode()},
                max_bytes=8192,
            )
            frame = (
                json.dumps(
                    {
                        "protocol": 2,
                        "action": "receive",
                        "operation_id": str(uuid4()),
                        "project": "demo",
                        "size": len(package),
                        "sha256": hashlib.sha256(package).hexdigest(),
                        "engine": "fixture",
                    }
                ).encode()
                + b"\n"
                + package
            )
            result = CliRunner().invoke(
                migration.build_migrate_app(),
                ["serve", "receive", "--engine", "fixture", "--project", "demo"],
                input=frame,
            )
            assert result.exit_code == 0, result.output
            assert json.loads(result.output)["status"] == "verified"
        assert [a.native_id for a in seen] == ["first", "second"]
        assert [a.records["events.jsonl"] for a in seen] == [b"first", b"second"]
    finally:
        unregister_persistence_adapter("fixture")


def test_native_export_through_cli(monkeypatch, tmp_path):
    class Adapter:
        engine = "fixture"

        def import_exact(self, archive):
            raise AssertionError("export must not import")

        @contextmanager
        def export_snapshot(self, session_id):
            """Hold the real cross-process gate for this session, then yield."""
            from untether.session_migration.session_gate import session_gate

            with session_gate(
                root=Path(os.environ["UNTETHER_SESSION_GATE_ROOT"]),
                engine_id=self.engine,
                native_id=session_id,
            ):
                yield self.export_exact(session_id)

        def export_exact(self, session_id):
            from untether.session_migration.native_archive import NativeArchive

            return NativeArchive(
                engine="fixture",
                logical_id=session_id,
                native_id=session_id,
                generation=1,
                lineage=session_id,
                records={"events.jsonl": session_id.encode()},
                digest="fixture",
            )

    cfg = SessionMigrationSettings(
        receiver=ReceiverSettings(
            endpoint_id="fixture",
            engine="fixture",
            project="demo",
            state_dir=str(tmp_path / "state"),
            destination_binding="enrolled",
            health_argv=(
                sys.executable,
                "-c",
                "print("
                + repr(json.dumps(dict.fromkeys(_REQUIRED_CHECKS, True)))
                + ")",
            ),
        )
    )
    cfg.enabled = True
    # Operation-bound export is fail-closed: it needs an explicit gate root,
    # so point the production guard at an isolated tmp root for this test.
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    monkeypatch.setattr(
        migration,
        "load_settings",
        lambda: (SimpleNamespace(session_migration=cfg), None),
    )
    monkeypatch.setattr(
        migration, "register_from_settings", lambda cfg, *, engine: None
    )
    register_persistence_adapter("fixture", Adapter)
    try:
        for sid in ("first", "second"):
            frame = (
                json.dumps(
                    {
                        "protocol": 2,
                        "action": "export",
                        "operation_id": str(uuid4()),
                        "project": "demo",
                        "size": 0,
                        "sha256": hashlib.sha256(b"").hexdigest(),
                        "engine": "fixture",
                        "session_id": sid,
                    }
                ).encode()
                + b"\n"
            )
            result = CliRunner().invoke(
                migration.build_migrate_app(),
                ["serve", "export", "--engine", "fixture", "--project", "demo"],
                input=frame,
            )
            assert result.exit_code == 0, result.output
            reply = json.loads(result.output)
            archive = decode_native(
                base64.b64decode(reply["package_base64"], validate=True),
                max_archive=65536,
                max_expanded=65536,
            )
            assert archive.logical_id == sid
            assert archive.native_id == sid
            assert archive.engine == "fixture"
            assert archive.records["events.jsonl"] == sid.encode()
    finally:
        unregister_persistence_adapter("fixture")
