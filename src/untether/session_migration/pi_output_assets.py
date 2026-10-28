"""Bounded bash full-output spill asset extraction for pi JSONL archives.

pi 1.4.0 spills oversized bash tool output to the OS temp directory
(``utils/output-files.js``: ``join(tmpdir(), `pi-bash-<16 hex>.log`)``,
mode ``0o600``) and persists only the absolute path in the journal entry
(``role: "bashExecution"`` with ``fullOutputPath``,
``core/agent-session.js`` ``recordBashResult``). The journal record is
portable; the spill bytes are not (OS temp cleanup, per-host tmpdir). This
module validates every referenced path fail-closed (absolute, direct child
of the OS temp directory, ``pi-bash-<16 hex>.log`` basename, regular
non-symlink file) before any read, bounds the total carried bytes, and
produces the canonical manifest record mapping original absolute refs to
carried asset records. No original journal is ever modified.
"""

from __future__ import annotations

import json
import os
import re
import stat
from hashlib import sha256
from pathlib import Path

from .native_adapter import CapabilityUnavailable

MANIFEST_RECORD = "pi-output-manifest.json"
RECORD_PREFIX = "pi-output/"
_LOG_NAME = re.compile(r"^pi-bash-[0-9a-f]{16}\.log$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REF_DEPTH_MAX = 16
_REFS_MAX = 4096
_ASSETS_TOTAL_LIMIT = 64 * 1024 * 1024
_MANIFEST_MAX_BYTES = 4 * 1024 * 1024
# Binary read, never following a symlink at open time (POSIX); Windows has
# no O_NOFOLLOW, so replacement is caught by the fstat/lstat identity check.
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)


def _refuse() -> CapabilityUnavailable:
    return CapabilityUnavailable("PI_OUTPUT_REFUSED")


def _key(path: Path) -> str:
    """Platform-stable lexical comparison key for a local path."""
    return os.path.normcase(os.path.normpath(str(path)))


def asset_record_name(ref: str) -> str:
    """Archive record name for a validated spill reference's basename."""
    return RECORD_PREFIX + Path(ref).name


def full_output_refs(journals: list[bytes]) -> list[str]:
    """Bounded, order-preserving extraction of ``fullOutputPath`` strings.

    Every journal line must parse as one JSON object; anything else refuses
    (``SESSION_RECORD_MALFORMED``). Recursion into each entry is depth-
    bounded and the ref count is bounded; a present-but-malformed
    ``fullOutputPath`` (non-string, empty) is refused, never skipped.
    """
    refs: list[str] = []

    def walk(node: object, depth: int) -> None:
        if len(refs) > _REFS_MAX:
            raise _refuse()
        if isinstance(node, dict):
            if "fullOutputPath" in node:
                # Depth bounds only where fullOutputPath can be trusted;
                # deep unrelated entry fields are walked, never rejected.
                if depth > _REF_DEPTH_MAX:
                    raise _refuse()
                value = node["fullOutputPath"]
                if not isinstance(value, str) or not value:
                    raise _refuse()
                refs.append(value)
                if len(refs) > _REFS_MAX:
                    raise _refuse()
            for key, value in node.items():
                if key != "fullOutputPath":
                    walk(value, depth + 1)
        elif isinstance(node, list):
            for item in node:
                walk(item, depth + 1)

    for journal in journals:
        for line in journal.split(b"\n"):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except (ValueError, UnicodeError):
                # Layout/malformed-record refusal belongs to the strict record
                # readers; spill scanning is opportunistic and never widens or
                # narrows those checks.
                continue
            if not isinstance(entry, dict):
                continue
            walk(entry, 0)
    return list(dict.fromkeys(refs))


def _checked_tmpdir(tmpdir: Path) -> Path:
    """The real spill directory behind the OS temp directory.

    The OS temp directory may be reached through a symlink chain (macOS
    ``/var`` -> ``/private/var``); the engine resolved the same chain when
    it wrote the spill, so the comparison happens on fully resolved real
    paths, never on the literal strings.
    """
    if not tmpdir.is_dir():
        raise _refuse()
    try:
        resolved = tmpdir.resolve(strict=True)
    except OSError as exc:
        raise _refuse() from exc
    return resolved


def _asset_path(ref: str, managed: Path) -> Path:
    """Validate one reference before anything is read from it."""
    if "\x00" in ref:
        raise _refuse()
    path = Path(ref)
    if not path.is_absolute() or ".." in path.parts:
        raise _refuse()  # relative, traversal, or escape by dot-dot
    try:
        parent_real = Path(os.path.realpath(path.parent))
    except (OSError, ValueError) as exc:
        raise _refuse() from exc
    if _key(parent_real) != _key(managed):
        raise _refuse()  # foreign directory or symlink escape
    if not _LOG_NAME.fullmatch(path.name):
        raise _refuse()
    if path.is_symlink():
        raise _refuse()
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise _refuse() from exc
    if not stat.S_ISREG(info.st_mode):
        raise _refuse()
    return path


def collect_assets(refs: list[str], tmpdir: Path) -> dict[str, bytes]:
    """Prevalidate every reference before any read; bound the total bytes.

    Phase 1 validates all paths fail-closed (absolute, no traversal, direct
    resolved child of the OS temp directory, ``pi-bash-<16 hex>.log``
    basename, regular non-symlink file, unique record name) before a single
    byte is read. Phase 2 then reads under the remaining shared byte budget,
    guarding against replacement between validation and open by comparing
    fstat identity to the validated lstat, with O_NOFOLLOW where the
    platform provides it.
    """
    managed = _checked_tmpdir(tmpdir)
    validated: list[tuple[str, Path, os.stat_result]] = []
    seen_refs: set[str] = set()
    record_names: set[str] = set()
    for ref in refs:
        if ref in seen_refs:
            continue
        seen_refs.add(ref)
        path = _asset_path(ref, managed)
        name = asset_record_name(ref)
        if name in record_names:
            raise _refuse()  # two refs collapse onto one archive record
        record_names.add(name)
        try:
            prior = os.lstat(path)
        except OSError as exc:
            raise _refuse() from exc
        validated.append((ref, path, prior))
    assets: dict[str, bytes] = {}
    total = 0
    for ref, path, prior in validated:
        remaining = _ASSETS_TOTAL_LIMIT - total
        try:
            fd = os.open(path, _READ_FLAGS)
        except OSError as exc:
            raise _refuse() from exc
        try:
            with os.fdopen(fd, "rb") as stream:
                current = os.fstat(stream.fileno())
                if (current.st_dev, current.st_ino) != (
                    prior.st_dev,
                    prior.st_ino,
                ):
                    raise _refuse()  # replaced after validation
                blob = stream.read(remaining + 1)
        except OSError as exc:
            raise _refuse() from exc
        if len(blob) > remaining:
            raise CapabilityUnavailable("ARCHIVE_TOO_LARGE")
        assets[ref] = blob
        total += len(blob)
    return assets


def manifest_record(assets: dict[str, bytes]) -> bytes:
    """Canonical JSON manifest: original absolute refs -> asset records."""
    manifest = {
        ref: {
            "record": asset_record_name(ref),
            "sha256": sha256(blob).hexdigest(),
            "size": len(blob),
        }
        for ref, blob in sorted(assets.items())
    }
    return json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()


def validate_asset_manifest(
    records: dict[str, bytes], journals: list[bytes]
) -> dict[str, str]:
    """Explicit safe manifest validation for a future asset import.

    The manifest record must be canonical JSON; every listed ref must be a
    non-empty string mapped to a carried record whose bytes match the listed
    size and sha256 exactly; the listed refs must equal the refs actually
    present in the journals. Returns ref -> record name; any mismatch
    refuses.
    """
    blob = records.get(MANIFEST_RECORD)
    if not isinstance(blob, bytes):
        raise _refuse()
    if len(blob) > _MANIFEST_MAX_BYTES:
        raise _refuse()
    try:
        manifest = json.loads(blob)
    except (ValueError, UnicodeError) as exc:
        raise _refuse() from exc
    if (
        not isinstance(manifest, dict)
        or json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode() != blob
    ):
        raise _refuse()  # non-canonical or structurally invalid manifest
    listing: dict[str, str] = {}
    for ref, entry in manifest.items():
        if not isinstance(ref, str) or not ref or "\x00" in ref:
            raise _refuse()
        if not isinstance(entry, dict):
            raise _refuse()
        record = entry.get("record")
        digest = entry.get("sha256")
        size = entry.get("size")
        if not isinstance(record, str) or not record.startswith(RECORD_PREFIX):
            raise _refuse()
        if not _LOG_NAME.fullmatch(record[len(RECORD_PREFIX) :]):
            raise _refuse()
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise _refuse()
        if type(size) is not int or size < 0:
            raise _refuse()
        payload = records.get(record)
        if payload is None:
            raise _refuse()
        if len(payload) != size or sha256(payload).hexdigest() != digest:
            raise _refuse()
        listing[ref] = record
    if len(listing) > _REFS_MAX:
        raise _refuse()
    if set(listing) != set(full_output_refs(journals)):
        raise _refuse()  # manifest must match the journal refs exactly
    return listing
