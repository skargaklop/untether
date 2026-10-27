"""Red-first contract: reclaiming a migrated source identity after return.

A completed move leaves a durable tombstone at the source. The product
contract still requires local->remote->local round trips, so a *verified
descendant* inbound transfer must be able to reclaim that identity:

1. Only a ``completed`` fence owned by an operation whose record matches the
   same engine/native id and peer/project may be reclaimed.
2. Reclaim rebinds the fence to the inbound operation (pending) before the
   irreversible record replacement, then completes it.
3. Pending fences, divergent owners, foreign peers/projects and non-
   descendant operations are refused without any state change.

These tests import planned symbols that do not exist yet; they are red until
the implementation packet lands. They must not import invented helper
modules — only the real repository modules.
"""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from untether.session_migration.file_bundle_adapter import (
    FileBundlePersistenceAdapter,
    _write_record,
)
from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.settings import EnginePersistenceProfile
from untether.session_migration.source_fence import (
    SourceFenced,
    complete_source_fence,
    place_source_fence,
    reactivate_source_fence,
    read_source_fence,
)
from untether.session_migration.transfer import TransferRecord, _read, _write


def _profile(sessions_dir: Path) -> EnginePersistenceProfile:
    return EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(sessions_dir),
        session_record="pi",
        restart_argv=("fixture-restart-probe", "{sessions_dir}", "{session_id}"),
        restart_timeout_s=30.0,
    )


def _session_bytes(sid: str, text: str) -> bytes:
    header = {"type": "session", "version": 3, "id": sid}
    return (
        json.dumps(header).encode()
        + b"\n"
        + json.dumps({"type": "message", "text": text}).encode()
        + b"\n"
    )


def _adapter(store: Path) -> FileBundlePersistenceAdapter:
    return FileBundlePersistenceAdapter(
        engine="pi",
        profile=_profile(store),
        sessions_dir=store,
        restart_runner=lambda argv: None,
    )


def _prior_push_record(ops: Path, op_id: str, sid: str, digest: str) -> TransferRecord:
    return TransferRecord(
        operation_id=op_id,
        engine="pi",
        session_id=sid,
        direction="to",
        peer="peer-alpha",
        project="proj-42",
        state="completed",
        identity_sha256=digest,
        native_id=sid,
        generation=3,
        lineage="lineage-A",
        receipt_sha256=digest,
    )


def test_completed_descendant_reclaim_rotates_fence_and_replaces_record(tmp_path):
    fences = tmp_path / "gates" / "fences"
    fences.mkdir(parents=True)
    ops = tmp_path / "ops"
    store = tmp_path / "store"
    store.mkdir()
    sid = "reclaim-1"

    v1 = _session_bytes(sid, "first")
    _write_record(_profile(store), store, sid, v1)
    prior_op = str(uuid4())
    prior_digest = hashlib.sha256(v1).hexdigest()
    place_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        operation_id=prior_op,
        package_sha256=prior_digest,
    )
    complete_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        operation_id=prior_op,
        package_sha256=prior_digest,
    )
    _write(ops, _prior_push_record(ops, prior_op, sid, prior_digest), create=True)

    inbound_op = str(uuid4())
    v2 = _session_bytes(sid, "second")
    inbound_digest = hashlib.sha256(v2).hexdigest()

    # A completed tombstone still blocks runner admission.
    from untether.session_migration.session_gate import session_gate

    with (
        pytest.raises(SourceFenced),
        session_gate(root=tmp_path / "gates", engine_id="pi", native_id=sid),
    ):
        pass

    fence = reactivate_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        prior_operation_id=prior_op,
        operation_id=inbound_op,
        package_sha256=inbound_digest,
        prior_record=_read(ops, prior_op),
    )
    assert fence.state == "pending"
    assert fence.operation_id == inbound_op
    assert fence.package_sha256 == inbound_digest

    # While the reclaim is pending, runner admission for this identity is
    # still refused: the source must stay quiescent until replacement and
    # completion finish.
    with (
        pytest.raises(SourceFenced),
        session_gate(root=tmp_path / "gates", engine_id="pi", native_id=sid),
    ):
        pass

    adapter = _adapter(store)
    proof = adapter.import_overwrite(
        engine="pi",
        native_id=sid,
        records={"session.jsonl": v2},
        expected_prior_records={"session.jsonl": v1},
        generation=4,
        lineage="test-lineage",
    )
    assert proof["session.jsonl"] == hashlib.sha256(v2).hexdigest()
    assert (store / f"{sid}.jsonl").read_bytes() == v2

    complete_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        operation_id=inbound_op,
        package_sha256=inbound_digest,
    )
    assert read_source_fence(fences, engine="pi", native_id=sid).state == "completed"


def test_pending_fence_is_never_reclaimed(tmp_path):
    fences = tmp_path / "fences"
    sid = "pending-1"
    op = str(uuid4())
    digest = hashlib.sha256(b"pkg").hexdigest()
    place_source_fence(
        fences, engine="pi", native_id=sid, operation_id=op, package_sha256=digest
    )
    with pytest.raises(RuntimeError, match=r"FENCE_PENDING|RECLAIM_REFUSED"):
        reactivate_source_fence(
            fences,
            engine="pi",
            native_id=sid,
            prior_operation_id=op,
            operation_id=str(uuid4()),
            package_sha256=hashlib.sha256(b"new").hexdigest(),
            prior_record=None,
        )
    fence = read_source_fence(fences, engine="pi", native_id=sid)
    assert fence.operation_id == op and fence.state == "pending"


def test_divergent_or_foreign_reclaim_refused_without_state_change(tmp_path):
    fences = tmp_path / "fences"
    ops = tmp_path / "ops"
    sid = "divergent-1"
    prior_op = str(uuid4())
    digest = hashlib.sha256(b"pkg").hexdigest()
    place_source_fence(
        fences, engine="pi", native_id=sid, operation_id=prior_op, package_sha256=digest
    )
    complete_source_fence(
        fences, engine="pi", native_id=sid, operation_id=prior_op, package_sha256=digest
    )
    record = _prior_push_record(ops, prior_op, sid, digest)
    _write(ops, record, create=True)

    for kwargs in (
        {"peer": "peer-omega"},  # foreign peer
        {"project": "proj-99"},  # foreign project
        {"lineage": "lineage-B"},  # divergent lineage
        {"direction": "from"},  # prior record was not an outbound move
    ):
        changed = record.model_copy(update=kwargs)
        with pytest.raises(RuntimeError, match=r"RECLAIM_REFUSED|DIVERGENT_LINEAGE"):
            reactivate_source_fence(
                fences,
                engine="pi",
                native_id=sid,
                prior_operation_id=prior_op,
                operation_id=str(uuid4()),
                package_sha256=hashlib.sha256(b"next").hexdigest(),
                prior_record=changed,
                prior_peer=record.peer,
                prior_project=record.project,
                prior_lineage=record.lineage,
            )
        fence = read_source_fence(fences, engine="pi", native_id=sid)
        assert fence.operation_id == prior_op and fence.state == "completed"


def test_import_overwrite_replaces_only_expected_prior_record(tmp_path):
    store = tmp_path / "store"
    store.mkdir()
    sid = "overwrite-1"
    adapter = _adapter(store)
    v1 = _session_bytes(sid, "v1")
    _write_record(_profile(store), store, sid, v1)

    stale = {"session.jsonl": b"not-the-current-record\n"}
    v2 = _session_bytes(sid, "v2")
    with pytest.raises(CapabilityUnavailable, match="OVERWRITE_PRIOR_MISMATCH"):
        adapter.import_overwrite(
            engine="pi",
            native_id=sid,
            records={"session.jsonl": v2},
            expected_prior_records=stale,
            generation=4,
            lineage="test-lineage",
        )
    assert (store / f"{sid}.jsonl").read_bytes() == v1

    proof = adapter.import_overwrite(
        engine="pi",
        native_id=sid,
        records={"session.jsonl": v2},
        expected_prior_records={"session.jsonl": v1},
        generation=4,
        lineage="test-lineage",
    )
    assert proof["session.jsonl"] == hashlib.sha256(v2).hexdigest()
    assert (store / f"{sid}.jsonl").read_bytes() == v2


def test_reclaim_crash_replay_is_idempotent(tmp_path):
    fences = tmp_path / "fences"
    ops = tmp_path / "ops"
    sid = "replay-1"
    prior_op = str(uuid4())
    digest = hashlib.sha256(b"pkg").hexdigest()
    place_source_fence(
        fences, engine="pi", native_id=sid, operation_id=prior_op, package_sha256=digest
    )
    complete_source_fence(
        fences, engine="pi", native_id=sid, operation_id=prior_op, package_sha256=digest
    )
    record = _prior_push_record(ops, prior_op, sid, digest)
    _write(ops, record, create=True)

    inbound_op = str(uuid4())
    inbound_digest = hashlib.sha256(b"next").hexdigest()

    first = reactivate_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        prior_operation_id=prior_op,
        operation_id=inbound_op,
        package_sha256=inbound_digest,
        prior_record=record,
    )
    # Replay of the same inbound operation must be idempotent, not a conflict.
    second = reactivate_source_fence(
        fences,
        engine="pi",
        native_id=sid,
        prior_operation_id=prior_op,
        operation_id=inbound_op,
        package_sha256=inbound_digest,
        prior_record=record,
    )
    assert first == second
    assert second.prior_operation_id == prior_op
    # Exact inbound replay cannot borrow a different completed owner's evidence.
    with pytest.raises(RuntimeError, match="RECLAIM_REFUSED"):
        reactivate_source_fence(
            fences,
            engine="pi",
            native_id=sid,
            prior_operation_id=prior_op,
            operation_id=inbound_op,
            package_sha256=inbound_digest,
            prior_record=record.model_copy(update={"operation_id": str(uuid4())}),
        )
    from untether.session_migration.source_fence import activate_source_fence

    for owner, sha in ((str(uuid4()), inbound_digest), (inbound_op, "f" * 64)):
        with pytest.raises(RuntimeError, match="FENCE_CONFLICT"):
            activate_source_fence(
                fences,
                engine="pi",
                native_id=sid,
                operation_id=owner,
                package_sha256=sha,
            )
    # A different inbound operation must conflict, never steal the reclaim.
    with pytest.raises(RuntimeError, match=r"CONFLICT"):
        reactivate_source_fence(
            fences,
            engine="pi",
            native_id=sid,
            prior_operation_id=prior_op,
            operation_id=str(uuid4()),
            package_sha256=inbound_digest,
            prior_record=record,
        )
    fence = read_source_fence(fences, engine="pi", native_id=sid)
    assert fence.state == "pending" and fence.operation_id == inbound_op
