"""File-bundle descendant reclaim through the real transfer coordinator."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from untether.session_migration import transfer
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    register_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.settings import MigrationPeer, SessionMigrationSettings

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


@pytest.mark.parametrize("fail_probe", [False, True])
@pytest.mark.parametrize("metadata_crash", [False, True])
def test_file_bundle_changed_roundtrip_metadata_and_retry(
    tmp_path, monkeypatch, fail_probe, metadata_crash
):
    from dataclasses import replace

    from untether.session_migration.file_bundle_adapter import (
        FileBundlePersistenceAdapter,
    )
    from untether.session_migration.session_gate import session_gate
    from untether.session_migration.settings import EnginePersistenceProfile
    from untether.session_migration.source_fence import SourceFenced

    def adapter(root):
        root.mkdir(mode=0o700)
        profile = EnginePersistenceProfile(
            format="pi-jsonl-v3",
            sessions_dir=str(root),
            session_record="pi",
            restart_argv=(
                sys.executable,
                "-c",
                "import pathlib,sys; assert pathlib.Path(sys.argv[1]).is_file()",
                "{session_path}",
            ),
        )
        return FileBundlePersistenceAdapter("pi", profile, root)

    local = adapter(tmp_path / "local")
    remote = adapter(tmp_path / "remote")
    sid = "file-roundtrip"
    old = b'{"type":"session","version":3,"id":"file-roundtrip"}\n{}\n'
    (local.sessions_dir / f"{sid}.jsonl").write_bytes(old)
    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer()})
    register_persistence_adapter("pi", lambda: local)
    common: dict[str, Any] = {
        "peer_name": "vps",
        "project": "demo",
        "session_id": sid,
        "engine": "pi",
        "root": tmp_path / "ops",
    }

    def receive(op, package, **kwargs):
        import hashlib

        from untether.session_migration.native_archive import decode_native

        archive = decode_native(package, max_archive=1 << 20, max_expanded=1 << 20)
        remote.import_exact(archive)
        return {
            "protocol": 1,
            "endpoint_id": _peer().expected_endpoint_id,
            "operation_id": op,
            "package_sha256": hashlib.sha256(package).hexdigest(),
            "importer_version": "test",
            "native_id": sid,
            "status": "verified",
            "engine": "pi",
        }

    try:
        transfer.perform(cfg, direction="to", receiver_call=receive, **common)
        with (
            pytest.raises(SourceFenced),
            session_gate(root=tmp_path / "gate", engine_id="pi", native_id=sid),
        ):
            pass
        evolved = old + b'{"type":"message","text":"changed"}\n'
        (remote.sessions_dir / f"{sid}.jsonl").write_bytes(evolved)
        with monkeypatch.context() as patch:
            patch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "remote-gate"))
            incoming = replace(remote.export_exact(sid), generation=9)
        package = transfer.build_inbound_package(cfg, incoming)
        if metadata_crash:
            from untether.session_migration import file_bundle_adapter

            original_replace = file_bundle_adapter.os.replace

            def interrupt_metadata(source, destination):
                if Path(destination).parent.name == ".untether-migration":
                    raise OSError("injected metadata publication failure")
                return original_replace(source, destination)

            with monkeypatch.context() as patch:
                patch.setattr(file_bundle_adapter.os, "replace", interrupt_metadata)
                with pytest.raises(
                    CapabilityUnavailable, match="SESSION_METADATA_WRITE_FAILED"
                ):
                    transfer.perform(
                        cfg, direction="from", incoming_package=package, **common
                    )
            assert (local.sessions_dir / f"{sid}.jsonl").read_bytes() == evolved
            with (
                pytest.raises(SourceFenced),
                session_gate(root=tmp_path / "gate", engine_id="pi", native_id=sid),
            ):
                pass

        probes = []
        original = FileBundlePersistenceAdapter._run_restart_probe

        def probe(self, *args):
            probes.append(args)
            if fail_probe and len(probes) == 1:
                raise CapabilityUnavailable("RESTART_PROBE_FAILED")
            return original(self, *args)

        monkeypatch.setattr(FileBundlePersistenceAdapter, "_run_restart_probe", probe)
        if fail_probe:
            with pytest.raises(CapabilityUnavailable, match="RESTART_PROBE_FAILED"):
                transfer.perform(
                    cfg, direction="from", incoming_package=package, **common
                )
            with (
                pytest.raises(SourceFenced),
                session_gate(root=tmp_path / "gate", engine_id="pi", native_id=sid),
            ):
                pass
        result = transfer.perform(
            cfg, direction="from", incoming_package=package, **common
        )
        assert result.state == "verified"
        assert local.export_exact(sid) == incoming
        assert probes
        assert (
            transfer.perform(cfg, direction="from", incoming_package=package, **common)
            == result
        )
        with session_gate(root=tmp_path / "gate", engine_id="pi", native_id=sid):
            pass
    finally:
        unregister_persistence_adapter("pi")
