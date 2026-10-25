from __future__ import annotations

import hashlib
import io
import json
import zipfile
from uuid import uuid4

import pytest

from untether.session_migration.package import PackageError, pack_context, unpack_context
from untether.session_migration.receipt import Receipt, ReceiptError, ReceiptStore


def test_context_roundtrip_preserves_binary_and_newlines():
    content = b"line\r\nline\n\\old\\path\x00"
    blob = pack_context(engine="acp", project="demo", content=content, max_bytes=10240)
    result = unpack_context(blob, max_archive=10240, max_expanded=10240)
    assert result.payload == content
    assert result.digest == hashlib.sha256(content).hexdigest()


def test_refuse_archive_member_injection():
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("manifest.json", "{}")
        z.writestr("../escape", "oops")
    with pytest.raises(PackageError, match="MEMBERS_INVALID"):
        unpack_context(out.getvalue(), max_archive=4096, max_expanded=4096)


def test_refuse_oversized_source_before_archive():
    with pytest.raises(PackageError, match="BOUNDS"):
        pack_context(engine="acp", project="demo", content=b"A" * 20, max_bytes=10)


def test_refuse_tampered_digest():
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("manifest.json", json.dumps({
            "protocol": 1, "mode": "context", "engine": "acp",
            "project": "demo", "size": 3, "sha256": "0" * 64,
        }))
        z.writestr("context.json", b"abc")
    with pytest.raises(PackageError, match="HASH_MISMATCH"):
        unpack_context(out.getvalue(), max_archive=4096, max_expanded=4096)


def test_receipt_write_once_and_idempotent(tmp_path):
    store = ReceiptStore(tmp_path / "receipts")
    op = str(uuid4())
    r = Receipt(op, "a" * 64, "1", "new-session", "verified")
    assert store.record(r) == r
    assert store.read(op) == r
    assert store.record(r) == r
    with pytest.raises(ReceiptError, match="OPERATION_CONFLICT"):
        store.record(Receipt(op, "b" * 64, "1", "new-session", "verified"))


def test_unverified_receipt_never_published(tmp_path):
    store = ReceiptStore(tmp_path / "receipts")
    with pytest.raises(ReceiptError, match="UNVERIFIED_RECEIPT"):
        store.record(Receipt(str(uuid4()), "x", "1", "native-id", "uploaded"))
    assert not (tmp_path / "receipts").exists()
