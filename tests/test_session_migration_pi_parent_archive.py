"""Pure, filesystem-free tests for pi parent record preflight validation."""

import json

import pytest

from untether.session_migration import file_bundle_adapter
from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.pi_parent_archive import validate_parent_records

ROOT = "root.jsonl"
WIN_STORE = r"C:\Users\dev\.pi\sessions"
POSIX_STORE = "/home/dev/.pi/sessions"


def _journal(
    session_id: str, parent: str | None = None, version: int = 3, kind: str = "session"
) -> bytes:
    header: dict[str, object] = {"type": kind, "version": version, "id": session_id}
    if parent is not None:
        header["parentSession"] = parent
    return json.dumps(header).encode() + b"\n" + b'{"type": "message"}\n'


def _bundle(
    journals: list[str] | None, payloads: list[bytes], root_parent: str | None
) -> dict[str, bytes]:
    records: dict[str, bytes] = {"session.jsonl": _journal("root", root_parent)}
    if journals is not None:
        record = {"version": 1, "journals": journals}
        records["parents.json"] = json.dumps(record).encode()
    for index, data in enumerate(payloads, 1):
        records[f"parent-{index}.jsonl"] = data
    return records


def test_valid_two_parent_chain_returns_ordered_pairs():
    records = _bundle(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("p1", WIN_STORE + r"\p2.jsonl"), _journal("p2")],
        WIN_STORE + r"\p1.jsonl",
    )
    assert validate_parent_records(records, ROOT, "root") == [
        ("p1.jsonl", records["parent-1.jsonl"]),
        ("p2.jsonl", records["parent-2.jsonl"]),
    ]


def test_no_parent_archive_returns_empty():
    records = {"session.jsonl": _journal("root")}
    assert validate_parent_records(records, ROOT, "root") == []


def test_missing_index_refused():
    records = _bundle(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("p1", POSIX_STORE + "/p2.jsonl")],
        "/s/p1.jsonl",
    )
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_MISSING"):
        validate_parent_records(records, ROOT, "root")


def test_orphan_parent_payload_refused():
    records = {"session.jsonl": _journal("root"), "parent-1.jsonl": _journal("p1")}
    with pytest.raises(CapabilityUnavailable, match="PARENT_RECORD_UNEXPECTED"):
        validate_parent_records(records, ROOT, "root")


def test_duplicate_parent_identity_refused():
    records = _bundle(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("dup", "/s/p2.jsonl"), _journal("dup")],
        "/s/p1.jsonl",
    )
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_CYCLE"):
        validate_parent_records(records, ROOT, "root")


def test_parent_reusing_root_identity_refused():
    records = _bundle(["p1.jsonl"], [_journal("root")], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="PARENT_REF_CYCLE"):
        validate_parent_records(records, ROOT, "root")


def test_root_name_collision_casefold_refused():
    records = _bundle(["Root.JSONL"], [_journal("p1")], "Root.JSONL")
    with pytest.raises(CapabilityUnavailable, match="PARENTS_RECORD_REFUSED"):
        validate_parent_records(records, ROOT, "root")


def test_foreign_chain_link_refused():
    records = _bundle(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("p1", "/s/other.jsonl"), _journal("p2")],
        "/s/p1.jsonl",
    )
    with pytest.raises(CapabilityUnavailable, match="PARENTS_CHAIN_MISMATCH"):
        validate_parent_records(records, ROOT, "root")


def test_malformed_header_refused():
    records = _bundle(["p1.jsonl"], [b"not json\n"], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_MALFORMED"):
        validate_parent_records(records, ROOT, "root")


def test_non_v3_header_refused():
    records = _bundle(["p1.jsonl"], [_journal("p1", version=2)], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_FORMAT_REFUSED"):
        validate_parent_records(records, ROOT, "root")


def test_non_session_header_refused():
    records = _bundle(["p1.jsonl"], [_journal("p1", kind="message")], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_FORMAT_REFUSED"):
        validate_parent_records(records, ROOT, "root")


def test_empty_header_identity_refused():
    records = _bundle(["p1.jsonl"], [_journal("")], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_MALFORMED"):
        validate_parent_records(records, ROOT, "root")


def test_oversized_header_line_refused():
    oversized = b'{"id": "' + b"x" * 65536 + b'"}\n'
    records = _bundle(["p1.jsonl"], [oversized], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_MALFORMED"):
        validate_parent_records(records, ROOT, "root")


def test_journal_over_byte_cap_refused(monkeypatch):
    monkeypatch.setattr(file_bundle_adapter, "_MAX_RECORD_BYTES", 256)
    oversized = _journal("p1") + b"x" * 512 + b"\n"
    records = _bundle(["p1.jsonl"], [oversized], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="SESSION_RECORD_MALFORMED"):
        validate_parent_records(records, ROOT, "root")


def test_trailing_unresolved_parent_refused():
    records = _bundle(["p1.jsonl"], [_journal("p1", "/s/p3.jsonl")], "/s/p1.jsonl")
    with pytest.raises(CapabilityUnavailable, match="PARENT_CHAIN_INCOMPLETE"):
        validate_parent_records(records, ROOT, "root")


def test_root_pointer_without_parents_record_refused():
    records = {"session.jsonl": _journal("root", "/s/p1.jsonl")}
    with pytest.raises(CapabilityUnavailable, match="PARENT_CHAIN_INCOMPLETE"):
        validate_parent_records(records, ROOT, "root")


def test_windows_absolute_pointer_is_parsed_lexically():
    records = _bundle(
        ["w1.jsonl", "w2.jsonl"],
        [_journal("w1", WIN_STORE + "\\w2.jsonl"), _journal("w2")],
        WIN_STORE + "\\w1.jsonl",
    )
    assert [name for name, _ in validate_parent_records(records, ROOT, "root")] == [
        "w1.jsonl",
        "w2.jsonl",
    ]


def test_posix_absolute_pointer_mismatch_refused_not_opened():
    records = _bundle(
        ["p1.jsonl", "p2.jsonl"],
        [_journal("p1", POSIX_STORE + "/elsewhere.jsonl"), _journal("p2")],
        POSIX_STORE + "/p1.jsonl",
    )
    with pytest.raises(CapabilityUnavailable, match="PARENTS_CHAIN_MISMATCH"):
        validate_parent_records(records, ROOT, "root")
