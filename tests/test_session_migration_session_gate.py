"""Session gate: O_EXCL cross-process lock, fail-closed, no liveness claims."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from untether.session_migration.session_gate import (
    GateRefused,
    SourceBusy,
    session_gate,
)
from untether.session_migration.source_fence import (
    SourceFenced,
    complete_source_fence,
    place_source_fence,
)


def test_active_gate_same_identity_refuses(tmp_path):
    root = tmp_path / "gates"
    with (
        session_gate(root=root, engine_id="pi", native_id="abc-123"),
        pytest.raises(SourceBusy, match="abc-123"),
        session_gate(root=root, engine_id="pi", native_id="abc-123"),
    ):
        pass  # pragma: no cover


def test_active_gate_different_native_id_allowed(tmp_path):
    root = tmp_path / "gates"
    with (
        session_gate(root=root, engine_id="pi", native_id="abc-123"),
        session_gate(root=root, engine_id="pi", native_id="other-456") as p,
    ):
        assert p.exists()


def test_same_native_id_different_engine_allowed(tmp_path):
    root = tmp_path / "gates"
    with (
        session_gate(root=root, engine_id="pi", native_id="abc-123"),
        session_gate(root=root, engine_id="claude", native_id="abc-123"),
    ):
        pass


def test_exception_in_body_releases_gate(tmp_path):
    root = tmp_path / "gates"
    with (
        pytest.raises(RuntimeError, match="boom"),
        session_gate(root=root, engine_id="pi", native_id="abc"),
    ):
        raise RuntimeError("boom")
    with session_gate(root=root, engine_id="pi", native_id="abc") as p:
        assert p.exists()


def test_release_removes_own_gate_file_only(tmp_path):
    root = tmp_path / "gates"
    with session_gate(root=root, engine_id="pi", native_id="a"):
        pass
    gates = sorted(p.name for p in root.iterdir())
    assert gates == [], f"gate file not unlinked on release: {gates}"
    # Sibling gate for another identity survives untouched.
    with (
        session_gate(root=root, engine_id="pi", native_id="a"),
        session_gate(root=root, engine_id="pi", native_id="b") as p,
    ):
        assert p.exists()


def test_release_preserves_replacement_gate(tmp_path):
    root = tmp_path / "gates"
    with session_gate(root=root, engine_id="pi", native_id="replaced") as path:
        saved = root / "original-held.lock"
        try:
            path.rename(saved)
        except PermissionError:
            pytest.skip("platform denies renaming an open gate")
        path.write_text("replacement-owner", encoding="utf-8")
    assert path.read_text(encoding="utf-8") == "replacement-owner"


def test_stale_gate_file_fails_closed(tmp_path):
    root = tmp_path / "gates"
    root.mkdir()
    from untether.session_migration.session_gate import _gate_path

    stale = _gate_path(root, "pi", "abc")
    stale.write_text("pid=424242\n", encoding="utf-8")
    with (
        pytest.raises(SourceBusy, match="stale"),
        session_gate(root=root, engine_id="pi", native_id="abc"),
    ):
        pass  # pragma: no cover
    assert stale.exists()  # refused gate is never deleted by contender
    os.remove(stale)  # manual operator reap makes the gate usable again
    with session_gate(root=root, engine_id="pi", native_id="abc"):
        pass


def test_empty_identity_refused(tmp_path):
    with (
        pytest.raises(GateRefused, match="GATE_ID_EMPTY"),
        session_gate(root=tmp_path / "gates", engine_id="", native_id="x"),
    ):
        pass


def test_symlinked_root_refused(tmp_path):
    if os.name == "nt" and not _symlinks_available():
        pytest.skip("Windows symlink privileges unavailable")
    target = tmp_path / "real-root"
    target.mkdir()
    link = tmp_path / "link-root"
    link.symlink_to(target, target_is_directory=True)
    with (
        pytest.raises(GateRefused, match="GATE_ROOT_SYMLINK"),
        session_gate(root=link, engine_id="pi", native_id="abc"),
    ):
        pass  # pragma: no cover


def _symlinks_available() -> bool:
    probe = Path(os.environ.get("TMP", os.environ.get("TEMP", "."))) / (
        "untether-gate-symlink-probe"
    )
    try:
        probe.unlink(missing_ok=True)
        probe.symlink_to(".", target_is_directory=True)
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def _place_fence(root, *, native_id, state):
    """Place a durable fence for (pi, native_id) in the requested state."""
    kwargs = {
        "engine": "pi",
        "native_id": native_id,
        "operation_id": str(uuid.uuid4()),
        "package_sha256": "ab" * 32,
    }
    fences = root / "fences"
    place_source_fence(fences, **kwargs)
    if state == "completed":
        complete_source_fence(fences, **kwargs)
    return kwargs


@pytest.mark.parametrize("state", ["pending", "completed"])
def test_durable_fence_blocks_gate(tmp_path, state):
    root = tmp_path / "gates"
    kwargs = _place_fence(root, native_id="fenced-1", state=state)
    entered = False
    with (
        pytest.raises(SourceFenced, match=f"state {state}"),
        session_gate(root=root, engine_id="pi", native_id=kwargs["native_id"]),
    ):
        entered = True  # pragma: no cover
    assert not entered, "gate body ran despite durable fence"
    leftovers = sorted(p.name for p in root.iterdir())
    assert leftovers == ["fences"], f"transient gate file not cleaned: {leftovers}"


@pytest.mark.parametrize("state", ["pending", "completed"])
def test_durable_fence_does_not_block_other_identity(tmp_path, state):
    root = tmp_path / "gates"
    _place_fence(root, native_id="fenced-2", state=state)
    with session_gate(root=root, engine_id="pi", native_id="other") as gate:
        assert gate.exists()
