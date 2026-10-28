"""Reclaim integration: pi's shared parent journals stay immutable bytes.

Drives the real :class:`FileBundlePersistenceAdapter` through
``import_overwrite`` with a genuine ``parents.json`` + ``parent-N.jsonl``
archive. The pure module tests cover the gate in isolation; these tests prove
the adapter wires it before any write and again after the restart probe.
"""

import hashlib
import json
import sys
from pathlib import Path

import pytest

from untether.session_migration.file_bundle_adapter import FileBundlePersistenceAdapter
from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.settings import EnginePersistenceProfile

ROOT_NAME = "2026-01-02T03-04-05-sid.jsonl"
PARENT = "p1.jsonl"


@pytest.fixture(autouse=True)
def configured_gate(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gates"))


def _journal(session_id: str, parent: str | None = None, entries: tuple = ()) -> bytes:
    header: dict[str, object] = {"type": "session", "version": 3, "id": session_id}
    if parent is not None:
        header["parentSession"] = parent
    body = [json.dumps(header), *(json.dumps(entry) for entry in entries)]
    return ("\n".join(body) + "\n").encode()


def _adapter(root: Path, *, restart_runner=None) -> FileBundlePersistenceAdapter:
    root.mkdir(mode=0o700, exist_ok=True)
    profile = EnginePersistenceProfile(
        format="pi-jsonl-v3",
        sessions_dir=str(root),
        session_record="pi",
        restart_argv=(
            sys.executable,
            "-c",
            "import json,sys; p=sys.argv[1]; sid=sys.argv[2]; "
            "assert json.loads(open(p,encoding='utf8').readline())['id']==sid",
            "{session_path}",
            "{session_id}",
        ),
    )
    return FileBundlePersistenceAdapter(
        "pi", profile, root, restart_runner=restart_runner
    )


def _layout_bytes(name: str) -> bytes:
    return json.dumps({"version": 1, "root_journal": name}, sort_keys=True).encode()


def _parents_bytes(*names: str) -> bytes:
    return json.dumps({"version": 1, "journals": list(names)}, sort_keys=True).encode()


def _reclaim_archive(root: Path, root_payload: bytes) -> dict[str, bytes]:
    """One parent journal + full parity of layout/parents records."""
    parent_payload = _journal("p1")
    return {
        "session.jsonl": root_payload,
        "layout.json": _layout_bytes(ROOT_NAME),
        "parents.json": _parents_bytes(PARENT),
        "parent-1.jsonl": parent_payload,
    }


def _seed(root: Path, prior: bytes, parent: bytes) -> None:
    (root / ROOT_NAME).write_bytes(prior)
    (root / PARENT).write_bytes(parent)


def test_evolved_root_reclaims_with_unchanged_parent(tmp_path):
    store = tmp_path / "store"
    engine = _adapter(store)
    pointer = str(store / PARENT)
    prior_root = _journal("sid", pointer, ({"type": "message", "n": 1},))
    evolved_root = prior_root + b'{"type": "message", "n": 2}\n'
    prior = _reclaim_archive(store, prior_root)
    incoming = _reclaim_archive(store, evolved_root)
    _seed(store, prior_root, incoming["parent-1.jsonl"])
    proof = engine.import_overwrite(
        engine="pi",
        native_id="sid",
        records=incoming,
        expected_prior_records=prior,
        generation=1,
        lineage="reclaim-lineage",
    )
    assert proof == {
        name: hashlib.sha256(blob).hexdigest() for name, blob in incoming.items()
    }
    assert (
        proof["parent-1.jsonl"]
        == hashlib.sha256(incoming["parent-1.jsonl"]).hexdigest()
    )
    assert (store / ROOT_NAME).read_bytes() == evolved_root
    # Shared lineage is read-only: the parent file is byte-for-byte untouched.
    assert (store / PARENT).read_bytes() == incoming["parent-1.jsonl"]


def test_foreign_parent_refused_with_zero_root_mutation(tmp_path):
    store = tmp_path / "store"
    engine = _adapter(store)
    pointer = str(store / PARENT)
    prior_root = _journal("sid", pointer, ({"type": "message"},))
    parent = _journal("p1")
    foreign = _journal("p1", entries=({"type": "message", "foreign": True},))
    prev = _reclaim_archive(store, prior_root)
    incoming = _reclaim_archive(store, prior_root)
    _seed(store, prior_root, foreign)
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_MISMATCH"):
        engine.import_overwrite(
            engine="pi",
            native_id="sid",
            records=incoming,
            expected_prior_records=prev,
            generation=1,
            lineage="reclaim-lineage",
        )
    assert (store / ROOT_NAME).read_bytes() == prior_root
    assert (store / PARENT).read_bytes() == foreign
    assert parent != foreign


def test_restart_mutated_parent_refused(tmp_path):
    store = tmp_path / "store"
    mutated = _journal("p1", entries=({"type": "message", "post": True},))
    engine = _adapter(
        store, restart_runner=lambda _argv: (store / PARENT).write_bytes(mutated)
    )
    pointer = str(store / PARENT)
    prior_root = _journal("sid", pointer)
    evolved_root = prior_root + b'{"type": "message", "n": 2}\n'
    prior = _reclaim_archive(store, prior_root)
    incoming = _reclaim_archive(store, evolved_root)
    _seed(store, prior_root, incoming["parent-1.jsonl"])
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_MISMATCH"):
        engine.import_overwrite(
            engine="pi",
            native_id="sid",
            records=incoming,
            expected_prior_records=prior,
            generation=1,
            lineage="reclaim-lineage",
        )
    assert (store / PARENT).read_bytes() == mutated
    assert (store / ROOT_NAME).read_bytes() == evolved_root


def test_retry_accepts_already_replaced_root(tmp_path):
    store = tmp_path / "store"
    engine = _adapter(store)
    pointer = str(store / PARENT)
    prior_root = _journal("sid", pointer, ({"type": "message", "n": 1},))
    evolved_root = prior_root + b'{"type": "message", "n": 2}\n'
    prior = _reclaim_archive(store, prior_root)
    incoming = _reclaim_archive(store, evolved_root)
    _seed(store, prior_root, incoming["parent-1.jsonl"])
    first = engine.import_overwrite(
        engine="pi",
        native_id="sid",
        records=incoming,
        expected_prior_records=prior,
        generation=1,
        lineage="reclaim-lineage",
    )
    # Fenced retry: the pending fence still presents the prior push package as
    # expected_prior; current bytes already equal the target and stay accepted.
    retry = engine.import_overwrite(
        engine="pi",
        native_id="sid",
        records=incoming,
        expected_prior_records=prior,
        generation=1,
        lineage="reclaim-lineage",
    )
    assert first == retry
    assert (store / ROOT_NAME).read_bytes() == evolved_root
    assert (store / PARENT).read_bytes() == incoming["parent-1.jsonl"]


def test_parent_set_parity_refused_before_writes(tmp_path):
    store = tmp_path / "store"
    engine = _adapter(store)
    pointer = str(store / PARENT)
    prior_root = _journal("sid", pointer)
    prior = _reclaim_archive(store, prior_root)
    del prior["parents.json"]
    del prior["parent-1.jsonl"]
    incoming = _reclaim_archive(store, prior_root)
    _seed(store, prior_root, incoming["parent-1.jsonl"])
    before = {p.name for p in store.iterdir()}
    with pytest.raises(CapabilityUnavailable, match="RECORD_LAYOUT_REFUSED"):
        engine.import_overwrite(
            engine="pi",
            native_id="sid",
            records=incoming,
            expected_prior_records=prior,
            generation=1,
            lineage="reclaim-lineage",
        )
    assert {p.name for p in store.iterdir()} == before


def test_undeclared_parent_slot_refused_before_writes(tmp_path):
    store = tmp_path / "store"
    engine = _adapter(store)
    pointer = str(store / PARENT)
    prior_root = _journal("sid", pointer)
    prior = _reclaim_archive(store, prior_root)
    incoming = _reclaim_archive(store, prior_root)
    incoming["parent-9.jsonl"] = prior.pop("parent-1.jsonl")
    _seed(store, prior_root, _journal("p1"))
    with pytest.raises(CapabilityUnavailable, match="RECORD_LAYOUT_REFUSED"):
        engine.import_overwrite(
            engine="pi",
            native_id="sid",
            records=incoming,
            expected_prior_records=prior,
            generation=1,
            lineage="reclaim-lineage",
        )
    assert (store / ROOT_NAME).read_bytes() == prior_root
