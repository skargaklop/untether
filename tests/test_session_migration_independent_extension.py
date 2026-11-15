"""Independent-extension acceptance (G3/G6).

Requirement under test: a NEW engine — not named anywhere in the codebase —
joins session migration purely through a declared ``executable-v1``
persistence profile (configuration + an external adapter binary). No central
dispatch edit, no hardcoded engine branch. The coordinator
(``transfer.perform``) then moves two distinct sessions for that engine in
both directions, and the return leg reclaims the pushed identities with
evolved content byte-exactly — the same semantics the in-tree round-trip
tests pin for known engines.

This is the documented extension path: existing native adapters are optional
implementations, not the product definition
(docs/plans/session-migration-acp-wide-goals.md G3).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

import pytest

from untether.session_migration import transfer
from untether.session_migration.native_adapter import (
    require_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    handle_receiver,
)
from untether.session_migration.settings import (
    EnginePersistenceSettings,
    ExecutablePersistenceProfile,
    SessionMigrationSettings,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS
from untether.session_migration.subprocess_adapter import ExecutablePersistenceAdapter

from .test_session_migration_transfer_end_to_end import _peer

ENGINE = "acme-agent-v1"  # novel: no in-tree adapter by this name
FIXTURE = Path(__file__).parent / "fixtures" / "native_persistence_engine.py"
RETURN_GENERATION = 3


@pytest.fixture(autouse=True)
def _gate_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))


@pytest.fixture(autouse=True)
def _clean_registry():
    yield
    unregister_persistence_adapter(ENGINE)


def _profile(store: Path) -> ExecutablePersistenceProfile:
    return ExecutablePersistenceProfile(
        format="executable-v1",
        executable=str(FIXTURE),
        interpreter=str(Path(sys.executable).resolve()),
        store=str(store),
        timeout=60.0,
    )


def _settings(store: Path) -> SessionMigrationSettings:
    return SessionMigrationSettings(
        enabled=True,
        peers={"vps": _peer()},
        persistence=EnginePersistenceSettings(engines={ENGINE: _profile(store)}),
    )


def _records(sid: str) -> dict[str, bytes]:
    marker = sid.encode()
    return {
        "history/tool.ndjson": b"tool\r\nraw" + marker,
        "assets/a.bin": b"\x00\xff\xfe" + marker,
    }


def _write_store_file(store: Path, sid: str, *, generation: int) -> None:
    payload = {
        "engine": ENGINE,
        "logical_id": sid,
        "native_id": sid,
        "generation": generation,
        "lineage": sid,
        "records": {
            name: base64.b64encode(blob).decode()
            for name, blob in _records(sid).items()
        },
    }
    (store / (sid + ".json")).write_text(json.dumps(payload), encoding="utf-8")


def _assert_store_records(store: Path, sids: list[str], *, generation: int) -> None:
    for sid in sids:
        landed = json.loads((store / f"{sid}.json").read_text(encoding="utf-8"))
        assert landed["generation"] == generation, f"stale generation for {sid}"
        for name, blob in _records(sid).items():
            assert base64.b64decode(landed["records"][name]) == blob, (
                f"record {name} not byte-exact for {sid}"
            )


def _receiver_rpc(store: Path, tmp_path: Path, *, engine: str = ENGINE):
    """Receiver RPC for one remote endpoint, run in-process (both legs).

    The remote side of an SSH transfer is a separate process with its own
    adapter registry (the declared persistence profile bound to its own
    store) and its own gate/fence root. This harness reproduces that
    boundary honestly — the shared ``_receiver_call`` helper cannot, because
    it swaps the registry under its own module's engine name, which would
    leave the novel engine resolving the coordinator-side registration.
    Replies are the protocol handler's raw JSON output, exactly what the
    production ``receiver_request`` transport parses and returns.

    Replies are the protocol handler's raw JSON output, exactly what the
    production ``receiver_request`` transport parses and returns —
    including the ``protocol: 1`` marker the coordinator must accept.
    """
    remote_adapter = ExecutablePersistenceAdapter(
        engine, FIXTURE, store, interpreter=sys.executable
    )
    gate = str(tmp_path / "receiver-gate")

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
        native = NativeReceiver(
            tmp_path / "receiver-native-state", max_archive=65536, max_expanded=65536
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
            authorized_peer="local",
        )
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
        from untether.session_migration import native_adapter

        previous = native_adapter._ADAPTERS.get(engine)
        native_adapter._ADAPTERS[engine] = lambda: remote_adapter
        saved_gate = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        os.environ["UNTETHER_SESSION_GATE_ROOT"] = gate
        try:
            raw = handle_receiver(endpoint, verb=action, payload=frame)
        finally:
            if saved_gate is None:
                os.environ.pop("UNTETHER_SESSION_GATE_ROOT", None)
            else:
                os.environ["UNTETHER_SESSION_GATE_ROOT"] = saved_gate
            if previous is None:
                native_adapter._ADAPTERS.pop(engine, None)
            else:
                native_adapter._ADAPTERS[engine] = previous
        reply = json.loads(raw)
        if action == "export-complete":
            assert reply["status"] == "export_completed", reply
        return reply

    return call


def test_novel_engine_joins_through_declared_profile_only(tmp_path):
    """Two sessions, both directions, byte-exact — zero core edits."""
    src_store = tmp_path / "a-store"
    dst_store = tmp_path / "b-store"
    src_store.mkdir()
    dst_store.mkdir()

    sids = [str(uuid.uuid4()), str(uuid.uuid4())]
    for sid in sids:
        _write_store_file(src_store, sid, generation=2)
    ops = tmp_path / "ops"

    # --- Push both A -> B: the coordinator registers from src profile ---
    push_call = _receiver_rpc(dst_store, tmp_path)
    pushes = [
        transfer.perform(
            _settings(src_store),
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="to",
            root=ops,
            receiver_call=push_call,
        )
        for sid in sids
    ]
    assert len({p.operation_id for p in pushes}) == 2
    for result, sid in zip(pushes, sids, strict=True):
        assert result.state == "completed"
        assert result.engine == ENGINE and result.native_id == sid

    # The production registry entry for the novel engine is the declared
    # executable adapter — constructed from the profile, not an engine branch.
    registered = require_persistence_adapter(ENGINE)
    assert isinstance(registered, ExecutablePersistenceAdapter)
    assert registered.engine == ENGINE
    _assert_store_records(dst_store, sids, generation=2)

    # The sessions evolve on B while owned there.
    for sid in sids:
        _write_store_file(dst_store, sid, generation=RETURN_GENERATION)

    # --- Pull both back B -> A: reclaim of the pushed identities ---
    # A real round trip returns into the SAME source store, whose untouched
    # pushed state collides on re-import and must be reclaimed through the
    # completed push fence (same sequencing the in-tree round-trip tests
    # pin); the receiver side of the leg runs on its own declared-profile
    # adapter for the B store, its own gate root, and real durable exports.
    pull_call = _receiver_rpc(dst_store, tmp_path)
    for sid in sids:
        result = transfer.perform(
            _settings(src_store),
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=ENGINE,
            direction="from",
            root=ops,
            receiver_call=pull_call,
        )
        assert result.state in {"verified", "bound"}
        assert result.engine == ENGINE and result.native_id == sid
    _assert_store_records(src_store, sids, generation=RETURN_GENERATION)
