"""Disposable-root contracts for the unregistered OMP persistence codec."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from untether.session_migration.native_archive import FidelityError
from untether.session_migration.omp_bundle import collect_omp_records


def journal(identity: str, *entries: dict) -> bytes:
    return (
        b"\n".join(
            json.dumps(e).encode()
            for e in ({"type": "session", "id": identity, "version": 3}, *entries)
        )
        + b"\n"
    )


@pytest.fixture
def roots(tmp_path: Path):
    sessions, blobs = tmp_path / "sessions", tmp_path / "blobs"
    sessions.mkdir()
    blobs.mkdir()
    (sessions / "random-name.jsonl").write_bytes(journal("wanted"))
    return sessions, blobs


def collect(roots, **limits):
    return collect_omp_records(
        sessions_dir=roots[0],
        blobs_dir=roots[1],
        native_id="wanted",
        **({"max_expanded": 100_000, "max_file_bytes": 10_000} | limits),
    )


def test_exact_bytes_title_children_and_blobs(roots):
    sessions, blobs = roots
    binary = b"\x00\xffimage"
    digest = hashlib.sha256(binary).hexdigest()
    ref = "blob:sha256:" + digest
    slot = {"type": "title", "v": 1, "title": "raw", "updatedAt": "now", "pad": ""}
    raw = json.dumps(slot, separators=(",", ":")).encode() + b"\n"
    slot["pad"] = " " * (256 - len(raw))
    title = json.dumps(slot, separators=(",", ":")).encode() + b"\n"
    root = title + journal(
        "wanted",
        {
            "type": "message",
            "message": {
                "role": "toolResult",
                "content": [{"data": ref}, {"text": ref}],
            },
        },
    )
    (sessions / "random-name.jsonl").write_bytes(root)
    nested = sessions / "random-name" / "nested"
    nested.mkdir(parents=True)
    child = journal("child", {"type": "message", "value": ref})
    (nested / "agent.jsonl").write_bytes(child)
    (nested / "binary.dat").write_bytes(binary)
    (blobs / digest).write_bytes(binary)
    (blobs / (digest + ".png")).write_bytes(b"not canonical")
    assert collect(roots) == {
        "session.jsonl": root,
        "artifacts/nested/agent.jsonl": child,
        "artifacts/nested/binary.dat": binary,
        "blobs/" + digest: binary,
    }


@pytest.mark.parametrize(
    "bad",
    [
        b"{",
        b"null\n",
        b"{}\n",
        b"",
        b' {"type":"session","id":"wanted","id":"other"}\n',
        journal("wanted") + journal("again"),
        journal("wanted") + b"NaN\n",
    ],
)
def test_malformed_refused(roots, bad):
    (roots[0] / "random-name.jsonl").write_bytes(bad)
    with pytest.raises(FidelityError):
        collect(roots)


def test_duplicate_and_missing_identity(roots):
    (roots[0] / "other.jsonl").write_bytes(journal("wanted"))
    with pytest.raises(FidelityError, match="DUPLICATE"):
        collect(roots)
    (roots[0] / "other.jsonl").unlink()
    (roots[0] / "random-name.jsonl").write_bytes(journal("other"))
    with pytest.raises(FidelityError, match="NOT_UNIQUE"):
        collect(roots)


@pytest.mark.parametrize("corrupt", [False, True])
def test_missing_corrupt_blob(roots, corrupt):
    digest = hashlib.sha256(b"image").hexdigest()
    (roots[0] / "random-name.jsonl").write_bytes(
        journal(
            "wanted",
            {
                "type": "message",
                "data": "blob:sha256:" + digest,
            },
        )
    )
    if corrupt:
        (roots[1] / digest).write_bytes(b"wrong")
    with pytest.raises(FidelityError):
        collect(roots)


def test_exact_refs_only_and_limits(roots):
    (roots[0] / "random-name.jsonl").write_bytes(
        journal(
            "wanted",
            {
                "type": "message",
                "text": "prefix blob:sha256:" + "a" * 64,
                "data": "blob:sha256:" + "A" * 64,
            },
        )
    )
    assert set(collect(roots)) == {"session.jsonl"}
    for limits in ({"max_expanded": 1}, {"max_file_bytes": 1}, {"max_files": 0}):
        with pytest.raises(FidelityError):
            collect(roots, **limits)


@pytest.mark.parametrize("location", ["root", "artifact", "component", "blob"])
def test_symlinks_refused(roots, tmp_path, location):
    sessions, blobs = roots
    target = tmp_path / "target"
    target.write_bytes(b"outside")
    link = sessions / "link"
    if location == "artifact":
        parent = sessions / "random-name"
        parent.mkdir()
        link = parent / "link"
    elif location == "blob":
        digest = hashlib.sha256(b"outside").hexdigest()
        (sessions / "random-name.jsonl").write_bytes(
            journal(
                "wanted",
                {
                    "type": "message",
                    "data": "blob:sha256:" + digest,
                },
            )
        )
        link = blobs / digest
    elif location in ("root", "component"):
        link = tmp_path / "alias"
        target = sessions
    try:
        link.symlink_to(target, target_is_directory=target.is_dir())
    except (OSError, NotImplementedError):
        pytest.skip("Windows symlink privilege unavailable")
    if location == "root":
        roots = link, blobs
    elif location == "component":
        sub = sessions / "sub"
        sub.mkdir()
        roots = link / "sub", blobs
    with pytest.raises(FidelityError):
        collect(roots)


def test_traversal_and_nested_malformed(roots):
    with pytest.raises(FidelityError):
        collect_omp_records(
            sessions_dir=roots[0] / ".." / "sessions",
            blobs_dir=roots[1],
            native_id="wanted",
            max_expanded=1000,
            max_file_bytes=1000,
        )
    parent = roots[0] / "random-name"
    parent.mkdir()
    (parent / "child.jsonl").write_bytes(b"bad")
    with pytest.raises(FidelityError):
        collect(roots)
