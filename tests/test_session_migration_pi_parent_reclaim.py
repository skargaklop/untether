"""Tests for the pi shared-parent reclaim preflight."""

import json
from pathlib import Path

import pytest

from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.pi_parent_reclaim import validate_reclaim_parents

ROOT = "root.jsonl"
STORE = "/sessions"
ROOT_POINTER = STORE + "/p1.jsonl"


def _journal(session_id: str, parent: str | None = None) -> bytes:
    header: dict[str, object] = {"type": "session", "version": 3, "id": session_id}
    if parent is not None:
        header["parentSession"] = parent
    return json.dumps(header).encode() + b"\n" + b'{"type": "message"}\n'


def _archive(
    journals: list[str] | None,
    payloads: list[bytes],
    root_parent: str | None,
    *,
    root: bytes | None = None,
) -> dict[str, bytes]:
    records: dict[str, bytes] = {
        "session.jsonl": root if root is not None else _journal("root", root_parent)
    }
    if journals is not None:
        records["parents.json"] = json.dumps(
            {"version": 1, "journals": journals}, sort_keys=True
        ).encode()
    for index, data in enumerate(payloads, 1):
        records[f"parent-{index}.jsonl"] = data
    return records


def _store(sessions_dir: Path, pairs: list[tuple[str, bytes]]) -> Path:
    for basename, payload in pairs:
        (sessions_dir / basename).write_bytes(payload)
    return sessions_dir


def _single_parent() -> dict[str, bytes]:
    return _archive(["p1.jsonl"], [_journal("p1")], ROOT_POINTER)


def test_unchanged_parents_accepted(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    store = _store(tmp_path, [("p1.jsonl", _journal("p1"))])
    assert validate_reclaim_parents(store, incoming, prior, ROOT, "root") is None


def test_two_parent_chain_accepted(tmp_path):
    payloads = [_journal("p1", STORE + "/p2.jsonl"), _journal("p2")]
    incoming = _archive(["p1.jsonl", "p2.jsonl"], payloads, ROOT_POINTER)
    prior = _archive(["p1.jsonl", "p2.jsonl"], payloads, ROOT_POINTER)
    store = _store(tmp_path, list(zip(["p1.jsonl", "p2.jsonl"], payloads, strict=True)))
    assert validate_reclaim_parents(store, incoming, prior, ROOT, "root") is None


def test_no_parents_accepted(tmp_path):
    records = {"session.jsonl": _journal("root")}
    assert validate_reclaim_parents(tmp_path, records, records, ROOT, "root") is None


def test_evolved_root_with_unchanged_parents_accepted(tmp_path):
    prior = _single_parent()
    evolved = _journal("root", ROOT_POINTER) + b'{"type": "message", "n": 2}\n'
    incoming = _single_parent()
    incoming["session.jsonl"] = evolved
    store = _store(tmp_path, [("p1.jsonl", _journal("p1"))])
    assert validate_reclaim_parents(store, incoming, prior, ROOT, "root") is None


def test_layout_and_other_records_stay_adapter_concern(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    incoming["layout.json"] = json.dumps(
        {"version": 1, "root_journal": "elsewhere.jsonl"}, sort_keys=True
    ).encode()
    prior["layout.json"] = json.dumps(
        {"version": 1, "root_journal": "origin.jsonl"}, sort_keys=True
    ).encode()
    store = _store(tmp_path, [("p1.jsonl", _journal("p1"))])
    assert validate_reclaim_parents(store, incoming, prior, ROOT, "root") is None


def test_independently_evolved_parent_same_id_refused(tmp_path):
    prior = _single_parent()
    evolved = _archive(
        ["p1.jsonl"], [_journal("p1") + b'{"type": "message", "n": 9}\n'], ROOT_POINTER
    )
    store = _store(tmp_path, [("p1.jsonl", _journal("p1"))])
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_DIVERGED"):
        validate_reclaim_parents(store, evolved, prior, ROOT, "root")
    # Refusal never keeps the differing local bytes as certified shared lineage.
    assert (tmp_path / "p1.jsonl").read_bytes() == _journal("p1")


def test_added_parent_manifest_refused(tmp_path):
    prior = _single_parent()
    gained = _archive(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("p1", STORE + "/p2.jsonl"), _journal("p2")],
        ROOT_POINTER,
    )
    store = _store(tmp_path, [("p1.jsonl", _journal("p1"))])
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_DIVERGED"):
        validate_reclaim_parents(store, gained, prior, ROOT, "root")


def test_changed_destination_parent_refused_before_mutation(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    foreign = _journal("p1") + b'{"type": "message", "foreign": true}\n'
    store = _store(tmp_path, [("p1.jsonl", foreign)])
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_MISMATCH"):
        validate_reclaim_parents(store, incoming, prior, ROOT, "root")
    assert (tmp_path / "p1.jsonl").read_bytes() == foreign


def test_missing_destination_parent_refused(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_MISSING"):
        validate_reclaim_parents(tmp_path, incoming, prior, ROOT, "root")


def test_non_regular_destination_parent_refused(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    (tmp_path / "p1.jsonl").mkdir()
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_UNSAFE"):
        validate_reclaim_parents(tmp_path, incoming, prior, ROOT, "root")


def test_symlink_destination_parent_refused(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    target = tmp_path / "elsewhere.jsonl"
    target.write_bytes(_journal("p1"))
    try:
        (tmp_path / "p1.jsonl").symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable on this platform")
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_UNSAFE"):
        validate_reclaim_parents(tmp_path, incoming, prior, ROOT, "root")


def test_extra_undeclared_store_parent_ignored(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    store = _store(
        tmp_path,
        [("p1.jsonl", _journal("p1")), ("unrelated.jsonl", _journal("other"))],
    )
    assert validate_reclaim_parents(store, incoming, prior, ROOT, "root") is None


def test_malformed_incoming_chain_refused(tmp_path):
    prior = _single_parent()
    broken = _archive(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("p1", STORE + "/p2.jsonl")],
        ROOT_POINTER,
    )
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_MISSING"):
        validate_reclaim_parents(tmp_path, broken, prior, ROOT, "root")


def test_malformed_prior_chain_refused(tmp_path):
    incoming = _single_parent()
    broken = _archive(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("p1", STORE + "/p2.jsonl")],
        ROOT_POINTER,
    )
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_MISSING"):
        validate_reclaim_parents(tmp_path, incoming, broken, ROOT, "root")


def test_postprobe_rereads_written_parents(tmp_path):
    incoming = _single_parent()
    prior = _single_parent()
    with pytest.raises(CapabilityUnavailable, match="RECLAIM_PARENT_MISSING"):
        validate_reclaim_parents(tmp_path, incoming, prior, ROOT, "root")
    _store(tmp_path, [("p1.jsonl", _journal("p1"))])
    assert validate_reclaim_parents(tmp_path, incoming, prior, ROOT, "root") is None
