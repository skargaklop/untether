"""Behavioral tests for ``validate_asset_manifest`` (pi output assets).

Pure in-memory verification: every input is a disposable ``dict[str, bytes]``
record map plus synthetic journal byte strings; the validator itself touches
no filesystem, so no tmp/home writes occur. Refusal is signalled by
``CapabilityUnavailable("PI_OUTPUT_REFUSED")``.

Untrusted source paths must be lexically rejected on import without looking
up destination filesystem paths.
"""

from __future__ import annotations

import hashlib
import json
from typing import cast

import pytest

from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.pi_output_assets import (
    MANIFEST_RECORD,
    RECORD_PREFIX,
    validate_asset_manifest,
)


def _canon(obj: object) -> bytes:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True).encode()


def _journal(*refs: str) -> bytes:
    """Synthetic journal bytes whose bashExecution entries carry ``refs``."""
    header = {"type": "session", "version": 3, "id": "s"}
    entries = [header]
    entries.extend(
        {"type": "message", "message": {"role": "bashExecution", "fullOutputPath": ref}}
        for ref in refs
    )
    if len(entries) == 1:
        entries.append({"type": "message", "message": {"role": "user"}})
    body = b"\n".join(_canon(e) for e in entries[1:])
    return _canon(header) + b"\n" + body + b"\n"


def _manifest_entry(payload: bytes, record: str | None = None) -> dict:
    return {
        "record": record or (RECORD_PREFIX + "pi-bash-" + "a" * 16 + ".log"),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
    }


def _records(manifest_obj: object, manifest_bytes: bytes | None = None, **extra: bytes):
    return {
        MANIFEST_RECORD: manifest_bytes
        if manifest_bytes is not None
        else _canon(manifest_obj),
        **extra,
    }


def _valid_setup() -> tuple[str, str, bytes, dict[str, bytes], list[bytes]]:
    ref = "/tmp/pi-bash-" + "a" * 16 + ".log"
    record = RECORD_PREFIX + "pi-bash-" + "a" * 16 + ".log"
    payload = b"spill-bytes"
    manifest = {ref: _manifest_entry(payload, record)}
    records = _records(manifest, **{record: payload})
    return ref, record, payload, records, [_journal(ref)]


def test_valid_manifest_returns_reference_mapping() -> None:
    ref, record, payload, records, journals = _valid_setup()
    listing = validate_asset_manifest(records, journals)
    assert listing == {ref: record}
    # Round-trip: the mapping carries the exact validated payload bytes.
    assert records[listing[ref]] == payload


@pytest.mark.parametrize(
    "ref",
    [
        "/private/var/tmp/pi-bash-aaaaaaaaaaaaaaaa.log",
        "C:/Users/example/Temp/pi-bash-aaaaaaaaaaaaaaaa.log",
        r"C:\Users\example\Temp\pi-bash-aaaaaaaaaaaaaaaa.log",
    ],
)
def test_manifest_validates_source_path_without_local_filesystem(ref: str) -> None:
    # Host-specific source paths are metadata here, never destinations for writes.
    record = RECORD_PREFIX + "pi-bash-aaaaaaaaaaaaaaaa.log"
    payload = b"portable spill"
    records = _records({ref: _manifest_entry(payload, record)}, **{record: payload})
    assert validate_asset_manifest(records, [_journal(ref)]) == {ref: record}


def test_valid_manifest_multiple_refs_canonical_order() -> None:
    payloads = {f"/tmp/pi-bash-{i:016x}.log": f"blob-{i}".encode() for i in range(3)}
    manifest = {
        ref: _manifest_entry(blob, RECORD_PREFIX + ref.rsplit("/", 1)[-1])
        for ref, blob in payloads.items()
    }
    records = _records(
        manifest,
        **{
            RECORD_PREFIX + ref.rsplit("/", 1)[-1]: blob
            for ref, blob in payloads.items()
        },
    )
    journals = [_journal(*payloads)]
    listing = validate_asset_manifest(records, journals)
    assert listing == {ref: RECORD_PREFIX + ref.rsplit("/", 1)[-1] for ref in payloads}


def _tampered(ref: str, entry: object, record: str, payload: bytes) -> dict[str, bytes]:
    return _records({ref: entry}, **{record: payload})


def test_payload_changed_refuses() -> None:
    ref, record, payload, records, journals = _valid_setup()
    entry = _manifest_entry(payload, record)
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(
            _tampered(ref, entry, record, b"changed-bytes"), journals
        )


def test_sha256_wrong_digest_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"sha256": "0" * 64}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_sha256_valid_format_other_digest_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {
        "sha256": hashlib.sha256(b"other").hexdigest()
    }
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_sha256_non_hex_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"sha256": "z" * 64}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_sha256_not_str_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"sha256": 123}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_size_too_large_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"size": len(payload) + 1}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_size_zero_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"size": 0}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_size_negative_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"size": -1}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_size_float_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"size": float(len(payload))}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_size_bool_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"size": True}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_record_missing_from_archive_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {
        "record": RECORD_PREFIX + "pi-bash-" + "b" * 16 + ".log"
    }
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_record_wrong_prefix_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"record": "other/payload.bin"}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_record_bad_log_basename_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"record": RECORD_PREFIX + "evil.log"}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_record_key_missing_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = {"sha256": hashlib.sha256(payload).hexdigest(), "size": len(payload)}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_entry_not_a_dict_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, "not-a-dict", record, payload), journals)


def test_record_not_str_refuses() -> None:
    ref, record, payload, _records, journals = _valid_setup()
    entry = _manifest_entry(payload, record) | {"record": None}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_tampered(ref, entry, record, payload), journals)


def test_missing_payload_record_refuses() -> None:
    ref, record, payload, records, journals = _valid_setup()
    del records[record]
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(records, journals)


def test_manifest_record_absent_refuses() -> None:
    _ref, record, _payload, records, journals = _valid_setup()
    del records[MANIFEST_RECORD]
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(records, journals)


def test_oversized_manifest_blob_refuses() -> None:
    ref, record, payload, records, journals = _valid_setup()
    records[MANIFEST_RECORD] = b" " * (4 * 1024 * 1024 + 1)
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(records, journals)


def test_manifest_blob_not_bytes_refuses() -> None:
    ref, record, payload, _base, journals = _valid_setup()
    # Honest malformed-input boundary: the manifest record arrives as ``str``
    # instead of ``bytes``. The cast documents that the test deliberately
    # passes an out-of-contract value; the validator must refuse it, not the
    # type checker. No suppression comment is used.
    malformed: dict[str, object] = {
        MANIFEST_RECORD: _canon({ref: _manifest_entry(payload, record)}).decode()
    }
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(cast("dict[str, bytes]", malformed), journals)


@pytest.mark.parametrize(
    "blob",
    [
        pytest.param(b"not json at all", id="malformed-json"),
        pytest.param(b"[]", id="json-array-not-object"),
        pytest.param(b"null", id="json-null"),
        pytest.param(b'"{ref}"', id="json-string"),
        pytest.param(b"   ", id="whitespace-only"),
    ],
)
def test_structurally_invalid_manifest_refuses(blob: bytes) -> None:
    _ref, _record, _payload, _base, journals = _valid_setup()
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest({MANIFEST_RECORD: blob}, journals)


def test_noncanonical_manifest_refuses() -> None:
    ref, record, payload, _base, journals = _valid_setup()
    manifest = {ref: _manifest_entry(payload, record)}
    noncanonical_variants = [
        json.dumps(manifest).encode(),  # default separators (spaces)
        json.dumps(manifest, separators=(",", ": "), sort_keys=True).encode(),
        _canon(manifest) + b" ",  # trailing whitespace
        _canon(manifest) + b"\n",
        b"{" + _canon(manifest)[1:-1] + b",}",  # trailing comma
    ]
    for blob in noncanonical_variants:
        with pytest.raises(CapabilityUnavailable):
            validate_asset_manifest(
                _records(manifest, manifest_bytes=blob, **{record: payload}),
                journals,
            )


def test_noncanonical_unicode_escaped_ref_refuses() -> None:
    # An escaped form of a valid ref re-serializes differently -> refused.
    payload = b"spill"
    ref = "/tmp/pi-bash-" + "a" * 16 + ".log"
    entry = _manifest_entry(payload)
    blob = (
        '{"\\/tmp\\/pi-bash-'
        + "a" * 16
        + '.log":{"record":"'
        + entry["record"]
        + '","sha256":"'
        + entry["sha256"]
        + '","size":5}}'
    ).encode()
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(
            _records({ref: entry}, manifest_bytes=blob, **{entry["record"]: payload}),
            [_journal(ref)],
        )


@pytest.mark.parametrize(
    "ref",
    [
        pytest.param("", id="empty-ref"),
        pytest.param("x\x00y", id="nul-byte-ref"),
        pytest.param(42, id="non-string-ref"),
        pytest.param(None, id="none-ref"),
    ],
)
def test_malformed_refs_refuse(ref) -> None:
    payload = b"p"
    entry = _manifest_entry(payload)
    manifest = {ref: entry} if ref is not None else {None: entry}
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_records(manifest, **{entry["record"]: payload}), [])


def test_missing_journal_ref_refuses() -> None:
    # Journal carries a spill ref the manifest omits -> exact-set violation.
    ref, record, payload, records, journals = _valid_setup()
    other = "/tmp/pi-bash-" + "c" * 16 + ".log"
    other_record = RECORD_PREFIX + "pi-bash-" + "c" * 16 + ".log"
    journals = [_journal(ref, other)]
    records = _records(
        {ref: _manifest_entry(payload, record)},
        **{record: payload, other_record: b"orphan"},
    )
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(records, journals)


def test_extra_manifest_ref_refuses() -> None:
    # Manifest lists a ref the journals never mention -> exact-set violation.
    ref, record, payload, records, _journals = _valid_setup()
    phantom = "/tmp/pi-bash-" + "d" * 16 + ".log"
    phantom_record = RECORD_PREFIX + "pi-bash-" + "d" * 16 + ".log"
    records = _records(
        {
            ref: _manifest_entry(payload, record),
            phantom: _manifest_entry(b"phantom", phantom_record),
        },
        **{record: payload, phantom_record: b"phantom"},
    )
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(records, [_journal(ref)])


def test_ref_count_over_budget_refuses() -> None:
    n = 4097
    manifest = {}
    extra: dict[str, bytes] = {}
    for i in range(n):
        ref = f"/tmp/pi-bash-{i:016x}.log"
        record = RECORD_PREFIX + f"pi-bash-{i:016x}.log"
        payload = f"b{i}".encode()
        manifest[ref] = _manifest_entry(payload, record)
        extra[record] = payload
    journals = [_journal(*manifest)]
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(_records(manifest, **extra), journals)


# ---------------------------------------------------------------------------
# Untrusted-record path traversal: MUST refuse. Reproduced before validation:
#   records = {MANIFEST_RECORD: canonical manifest mapping
#              "/etc/../etc/passwd" -> carried pi-output record with valid
#              sha256/size}, journals = one bashExecution whose
#              fullOutputPath is the same traversal string.
#   validate_asset_manifest(...) ACCEPTS and returns
#   {"/etc/../etc/passwd": "pi-output/pi-bash-aaaa...log"}.
# Refuse unsafe syntax before any destination asset-import consumer; the
# source temp-directory membership check belongs to export, not import.
# ---------------------------------------------------------------------------
_TRAVERSAL_REFS = [
    pytest.param("/etc/../etc/passwd", id="dotdot-escape"),
    pytest.param(
        "/tmp/../../../x/pi-bash-" + "a" * 16 + ".log", id="dotdot-with-log-name"
    ),
    pytest.param("pi-bash-" + "a" * 16 + ".log", id="relative-path"),
    pytest.param("C:/Windows/system32/config/SAM", id="absolute-foreign-target"),
]


@pytest.mark.parametrize("ref", _TRAVERSAL_REFS)
def test_untrusted_traversal_ref_refuses(ref: str) -> None:
    payload = b"evil"
    record = RECORD_PREFIX + "pi-bash-" + "a" * 16 + ".log"
    manifest = {ref: _manifest_entry(payload, record)}
    records = _records(manifest, **{record: payload})
    journals = [_journal(ref)]
    with pytest.raises(CapabilityUnavailable):
        validate_asset_manifest(records, journals)
