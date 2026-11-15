from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    RestoreProof,
    assert_exact_restore,
    require_persistence_adapter,
)
from untether.session_migration.native_archive import (
    FidelityError,
    decode_native,
    encode_native,
)


def test_two_sessions_both_directions_preserve_every_record(tmp_path):
    for ident in (str(uuid4()), str(uuid4())):
        records = {
            "history/events.ndjson": b'{"type":"tool","path":"C:\\\\old"}\r\n',
            "assets/image.bin": b"\x00\xff\xfe\n",
            "tools/output.json": b'{"stdout":"unchanged"}\n',
        }
        archive = encode_native(
            engine="fixture",
            logical_id=ident,
            native_id=ident,
            generation=17,
            lineage="lineage-" + ident,
            records=records,
            max_bytes=32768,
        )
        for _hop in ("to", "from"):
            restored = decode_native(archive, max_archive=32768, max_expanded=32768)
            assert restored.records == records
            assert restored.lineage == "lineage-" + ident
            archive = encode_native(
                engine=restored.engine,
                logical_id=restored.logical_id,
                native_id=restored.native_id,
                generation=restored.generation,
                lineage=restored.lineage,
                records=restored.records,
                max_bytes=32768,
            )
        hashes = {
            name: hashlib.sha256(content).hexdigest()
            for name, content in records.items()
        }
        proof = RestoreProof(ident, ident, 17, "lineage-" + ident, hashes, True, True)
        assert assert_exact_restore(restored, proof) == ident


def test_missing_engine_adapter_refuses_without_reseeding():
    with pytest.raises(
        CapabilityUnavailable, match="ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE"
    ):
        require_persistence_adapter("unknown-acp-engine-fixture")


def test_unverified_followup_is_rejected():
    session = str(uuid4())
    raw = encode_native(
        engine="fixture",
        logical_id=session,
        native_id="old",
        generation=1,
        lineage="line",
        records={"history": b"event"},
        max_bytes=4096,
    )
    archive = decode_native(raw, max_archive=4096, max_expanded=4096)
    proof = RestoreProof(
        session,
        "fresh",
        1,
        "line",
        {
            "history": hashlib.sha256(b"event").hexdigest(),
        },
        True,
        False,
    )
    with pytest.raises(CapabilityUnavailable, match="EXACT_RESTORE_UNVERIFIED"):
        assert_exact_restore(archive, proof)


def test_archive_tampering_and_untrusted_paths_refuse():
    with pytest.raises(FidelityError, match="ARCHIVE_INVALID"):
        encode_native(
            engine="fixture",
            logical_id="a",
            native_id="b",
            generation=0,
            lineage="l",
            records={"../secret": b"x"},
            max_bytes=4096,
        )
    source = encode_native(
        engine="fixture",
        logical_id="a",
        native_id="b",
        generation=0,
        lineage="l",
        records={"history": b"abc"},
        max_bytes=4096,
    )
    with pytest.raises(FidelityError):
        decode_native(source[:-12], max_archive=4096, max_expanded=4096)
