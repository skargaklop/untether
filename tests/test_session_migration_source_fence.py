"""Source fence tests: durable tombstone, idempotent replay, fail-closed."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from untether.session_migration.source_fence import (
    SourceFenceConflict,
    SourceFenceCorrupt,
    SourceFenced,
    SourceFenceRefused,
    assert_source_unfenced,
    complete_source_fence,
    place_source_fence,
    read_source_fence,
)

OP_A = str(uuid4())
OP_B = str(uuid4())
SHA_A = "a" * 64
SHA_B = "b" * 64


def _place(root, *, op=OP_A, sha=SHA_A, engine="pi", native_id="abc-123"):
    return place_source_fence(
        root, engine=engine, native_id=native_id, operation_id=op, package_sha256=sha
    )


def test_place_creates_pending_record(tmp_path):
    fence = _place(tmp_path)
    assert fence.state == "pending"
    stored = read_source_fence(tmp_path, engine="pi", native_id="abc-123")
    assert stored == fence


def test_same_operation_and_hash_is_idempotent(tmp_path):
    first = _place(tmp_path)
    second = _place(tmp_path)
    assert second == first
    assert second.state == "pending"
    from untether.session_migration.source_fence import _fence_path

    assert _fence_path(tmp_path, "pi", "abc-123").exists()
    assert len(list(tmp_path.glob("fence-*.json"))) == 1


def test_replay_after_completion_is_idempotent(tmp_path):
    _place(tmp_path)
    complete_source_fence(
        tmp_path,
        engine="pi",
        native_id="abc-123",
        operation_id=OP_A,
        package_sha256=SHA_A,
    )
    again = _place(tmp_path)
    assert again.state == "completed"


def test_conflicting_operation_refuses(tmp_path):
    _place(tmp_path, op=OP_A)
    with pytest.raises(SourceFenceConflict, match="FENCE_CONFLICT"):
        _place(tmp_path, op=OP_B)


def test_conflicting_hash_refuses(tmp_path):
    _place(tmp_path, sha=SHA_A)
    with pytest.raises(SourceFenceConflict):
        _place(tmp_path, sha=SHA_B)


def test_completed_fence_still_blocks_source(tmp_path):
    _place(tmp_path)
    complete_source_fence(
        tmp_path,
        engine="pi",
        native_id="abc-123",
        operation_id=OP_A,
        package_sha256=SHA_A,
    )
    with pytest.raises(SourceFenced, match=OP_A):
        assert_source_unfenced(tmp_path, engine="pi", native_id="abc-123")


def test_pending_fence_blocks_source(tmp_path):
    _place(tmp_path)
    with pytest.raises(SourceFenced):
        assert_source_unfenced(tmp_path, engine="pi", native_id="abc-123")


def test_unfenced_source_passes(tmp_path):
    assert_source_unfenced(tmp_path, engine="pi", native_id="abc-123")


def test_fenced_identity_scoped_by_engine(tmp_path):
    _place(tmp_path, engine="pi", native_id="abc-123")
    assert_source_unfenced(tmp_path, engine="claude", native_id="abc-123")
    with pytest.raises(SourceFenced):
        assert_source_unfenced(tmp_path, engine="pi", native_id="abc-123")


def test_identity_aliases_do_not_collide(tmp_path):
    _place(tmp_path, engine="ab", native_id="c")
    assert_source_unfenced(tmp_path, engine="a", native_id="bc")
    with pytest.raises(SourceFenced):
        assert_source_unfenced(tmp_path, engine="ab", native_id="c")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("engine", ""),
        ("native_id", ""),
        ("engine", "x" * 129),
        ("native_id", "bad\x01id"),
        ("operation_id", "not-a-uuid"),
        ("operation_id", ""),
        ("package_sha256", "ZZ" * 32),
        ("package_sha256", "a" * 63),
    ],
)
def test_invalid_place_args_refuse(tmp_path, field, value):
    kwargs = {
        "engine": "pi",
        "native_id": "abc-123",
        "operation_id": OP_A,
        "package_sha256": SHA_A,
    }
    kwargs[field] = value
    with pytest.raises(SourceFenceRefused):
        place_source_fence(tmp_path, **kwargs)


def test_complete_requires_pending_match(tmp_path):
    _place(tmp_path)
    with pytest.raises(SourceFenceConflict):
        complete_source_fence(
            tmp_path,
            engine="pi",
            native_id="abc-123",
            operation_id=OP_B,
            package_sha256=SHA_A,
        )
    complete_source_fence(
        tmp_path,
        engine="pi",
        native_id="abc-123",
        operation_id=OP_A,
        package_sha256=SHA_A,
    )
    with pytest.raises(SourceFenceConflict):
        complete_source_fence(
            tmp_path,
            engine="pi",
            native_id="abc-123",
            operation_id=OP_A,
            package_sha256=SHA_A,
        )


@pytest.mark.parametrize("payload", ["", "not json", "[]", '{"engine": "pi"}'])
def test_malformed_stored_record_fails_closed(tmp_path, payload):
    from untether.session_migration.source_fence import _fence_path

    _fence_path(tmp_path, "pi", "abc-123").write_text(payload, encoding="utf-8")
    for call in (
        lambda: read_source_fence(tmp_path, engine="pi", native_id="abc-123"),
        lambda: assert_source_unfenced(tmp_path, engine="pi", native_id="abc-123"),
        lambda: place_source_fence(
            tmp_path,
            engine="pi",
            native_id="abc-123",
            operation_id=OP_A,
            package_sha256=SHA_A,
        ),
    ):
        with pytest.raises(SourceFenceCorrupt):
            call()


@pytest.mark.parametrize(
    ("field", "value"),
    [("operation_id", "not-a-uuid"), ("state", "bogus"), ("engine", "")],
)
def test_stored_tampered_value_fails_closed(tmp_path, field, value):
    _place(tmp_path)
    from untether.session_migration.source_fence import _fence_path

    path = _fence_path(tmp_path, "pi", "abc-123")
    data = json.loads(path.read_text(encoding="utf-8"))
    data[field] = value
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(SourceFenceCorrupt):
        assert_source_unfenced(tmp_path, engine="pi", native_id="abc-123")


def test_no_reaping_and_tombstone_persists(tmp_path):
    _place(tmp_path)
    complete_source_fence(
        tmp_path,
        engine="pi",
        native_id="abc-123",
        operation_id=OP_A,
        package_sha256=SHA_A,
    )
    # Still on disk, still blocking: no deletion, no resume, no timers.
    assert read_source_fence(tmp_path, engine="pi", native_id="abc-123") is not None
    with pytest.raises(SourceFenced):
        assert_source_unfenced(tmp_path, engine="pi", native_id="abc-123")
