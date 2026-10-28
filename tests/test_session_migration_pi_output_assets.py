"""Focused tests for pi bash full-output spill asset carriage.

Export must carry validated ``pi-bash-<16 hex>.log`` bytes from the OS temp
directory plus a canonical manifest without touching journal bytes; import of
a manifest-carrying archive must refuse before ANY store write (restore-to-tmp
semantics are not implemented and a silent drop would falsely certify
fidelity); no-ref archives keep the exact prior record layout.
"""

import json
import re
import sys
from pathlib import Path

import pytest

from untether.session_migration.native_adapter import CapabilityUnavailable

from .test_session_migration_file_bundle_adapter import (
    adapter,
)


def _journal(sid: str, full_output_path: str | None) -> bytes:
    header = {"type": "session", "version": 3, "id": sid}
    lines = [header]
    if full_output_path is not None:
        lines.append(
            {
                "type": "message",
                "message": {
                    "role": "bashExecution",
                    "fullOutputPath": full_output_path,
                },
            }
        )
    else:
        lines.append({"type": "message", "message": {"role": "user"}})
    return (
        json.dumps(header) + "\n" + "\n".join(json.dumps(e) for e in lines[1:]) + "\n"
    ).encode()


@pytest.fixture(autouse=True)
def configured_gate(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gates"))


def _spill_log(name: str, blob: bytes | None = b"x" * 32) -> Path:
    # Real Pi writes spill logs as DIRECT children of the OS temp dir; the
    # validator requires exactly that. Use a random 16-hex name (the only
    # accepted pattern) and clean up afterwards.
    import secrets
    import tempfile

    if name == "auto":
        name = f"pi-bash-{secrets.token_hex(8)}.log"
    target = Path(tempfile.gettempdir()) / name
    target.write_bytes(blob if blob is not None else b"")
    return target


@pytest.fixture
def spill(tmp_path):
    created: list[Path] = []

    def make(name: str = "auto", blob: bytes = b"spill-" + b"y" * 16) -> Path:
        path = _spill_log(name, blob)
        created.append(path)
        return path

    yield make
    for path in created:
        path.unlink(missing_ok=True)


def test_export_carries_spill_bytes_and_manifest(tmp_path, spill):
    log = spill("pi-bash-0123456789abcdef.log", b"OUTPUT-BYTES" * 4)
    store = tmp_path / "store"
    engine = adapter(store)
    (store / "s1.jsonl").write_bytes(_journal("s1", str(log)))
    archive = engine.export_exact("s1")
    assert set(archive.records) == {
        "session.jsonl",
        "pi-output/pi-bash-0123456789abcdef.log",
        "pi-output-manifest.json",
    }
    assert (
        archive.records["pi-output/pi-bash-0123456789abcdef.log"] == b"OUTPUT-BYTES" * 4
    )
    manifest = json.loads(archive.records["pi-output-manifest.json"])
    entry = manifest[str(log)]
    assert entry["record"] == "pi-output/pi-bash-0123456789abcdef.log"
    assert entry["size"] == len(b"OUTPUT-BYTES" * 4)
    assert re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
    # The journal bytes are carried exactly, never rewritten.
    assert archive.records["session.jsonl"] == (store / "s1.jsonl").read_bytes()


def test_export_without_refs_keeps_prior_layout(tmp_path):
    store = tmp_path / "store"
    engine = adapter(store)
    (store / "s1.jsonl").write_bytes(_journal("s1", None))
    archive = engine.export_exact("s1")
    assert set(archive.records) == {"session.jsonl"}


@pytest.mark.parametrize(
    "bad_ref",
    [
        "pi-bash-relative.log",
        "/etc/pi-bash-0123456789abcdef.log",  # foreign directory
        "C:\\Windows\\Temp\\pi-bash-0123456789abcdef.log"
        if sys.platform == "win32"
        else "/definitely/not/tmp/pi-bash-0123456789abcdef.log",
        str(Path.cwd() / "pi-bash-0123456789abcdef.log"),
    ],
)
def test_foreign_or_relative_ref_refused_before_read(tmp_path, bad_ref):
    store = tmp_path / "store"
    engine = adapter(store)
    (store / "s1.jsonl").write_bytes(_journal("s1", bad_ref))
    with pytest.raises(CapabilityUnavailable):
        engine.export_exact("s1")


def test_missing_spill_file_refused(tmp_path, monkeypatch):
    import tempfile

    missing = Path(tempfile.gettempdir()) / "pi-bash-ffffffffffffffff.log"
    missing.unlink(missing_ok=True)
    store = tmp_path / "store"
    engine = adapter(store)
    (store / "s1.jsonl").write_bytes(_journal("s1", str(missing)))
    with pytest.raises(CapabilityUnavailable):
        engine.export_exact("s1")


def test_import_with_manifest_refuses_before_writes(tmp_path, spill):
    log = spill("pi-bash-0123456789abcdef.log")
    store = tmp_path / "store"
    engine = adapter(store)
    (store / "s1.jsonl").write_bytes(_journal("s1", str(log)))
    archive = engine.export_exact("s1")

    fresh = tmp_path / "fresh-store"
    target = adapter(fresh)
    with pytest.raises(CapabilityUnavailable, match="PI_OUTPUT_IMPORT_NOT_IMPLEMENTED"):
        target.import_exact(archive)
    # Refusal happened before ANY store write.
    assert list(fresh.iterdir()) == []


def test_overwrite_with_manifest_refused(tmp_path, spill):
    log = spill("pi-bash-0123456789abcdef.log")
    store = tmp_path / "store"
    engine = adapter(store)
    (store / "s1.jsonl").write_bytes(_journal("s1", str(log)))
    archive = engine.export_exact("s1")
    with pytest.raises(CapabilityUnavailable, match="PI_OUTPUT_IMPORT_NOT_IMPLEMENTED"):
        engine.import_overwrite(
            engine="pi",
            native_id="s1",
            records=archive.records,
            expected_prior_records={"session.jsonl": archive.records["session.jsonl"]},
            generation=1,
            lineage=archive.lineage,
        )
    # The store journal is untouched by the refusal.
    assert (store / "s1.jsonl").read_bytes() == archive.records["session.jsonl"]
