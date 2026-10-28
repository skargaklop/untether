"""Bounded tool-output spill asset extraction for OpenCode sync archives.

OpenCode 1.17.13 bounds oversized tool results into engine-owned spill files
(``<data_root>/xdg_data_home/opencode/tool-output/tool_*``, 7-day retention,
``packages/core/src/tool-output-store.ts``) and references them only by path
in event ``data.outputPaths``. The event stream is portable; the bytes are
not. This module validates every referenced path fail-closed (absolute,
direct child of the managed directory, regular non-symlink file) before any
read, bounds the total carried bytes, and produces the archive manifest
record that maps original absolute refs to carried asset records. No
original event is ever modified.
"""

from __future__ import annotations

import json
import os
import re
import stat
from hashlib import sha256
from pathlib import Path

from .native_adapter import CapabilityUnavailable

_MANAGED_SUBPATH = ("xdg_data_home", "opencode", "tool-output")
_MANAGED_DIRECTORY = "tool-output"
_ASSET_RECORD_PREFIX = _MANAGED_DIRECTORY + "/"
MANIFEST_RECORD = "tool-output-manifest.json"
_SPILL_NAME = re.compile(r"^tool_[A-Za-z0-9._-]{1,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_OUTPUT_DEPTH_MAX = 16
_OUTPUT_REFS_MAX = 4096
_ASSETS_TOTAL_LIMIT = 64 * 1024 * 1024
# Binary read, never following a symlink at open time (POSIX); Windows has
# no O_NOFOLLOW, so replacement is caught by the fstat/lstat identity check.
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)


def _refuse() -> CapabilityUnavailable:
    return CapabilityUnavailable("TOOL_OUTPUT_REFUSED")


def _key(path: Path) -> str:
    """Platform-stable lexical comparison key for a local path."""
    return os.path.normcase(os.path.normpath(str(path)))


def managed_dir(data_root: Path) -> Path:
    """Engine-owned spill directory inside the disposable data root."""
    return data_root.joinpath(*_MANAGED_SUBPATH)


def asset_record_name(ref: str) -> str:
    """Archive record name for a validated spill reference's basename."""
    return _ASSET_RECORD_PREFIX + Path(ref).name


def output_refs(events: list[dict]) -> list[str]:
    """Bounded, order-preserving extraction of ``outputPaths`` strings.

    Recursion into event ``data`` is depth-bounded and the ref count is
    bounded; any malformed ``outputPaths`` entry (non-list, non-string,
    empty) is refused, never silently skipped.
    """
    refs: list[str] = []

    def walk(node: object, depth: int) -> None:
        if len(refs) > _OUTPUT_REFS_MAX:
            raise _refuse()
        if isinstance(node, dict):
            if "outputPaths" in node:
                # Depth bounds only where outputPaths can be trusted; deep
                # unrelated event fields are walked, never rejected.
                if depth > _OUTPUT_DEPTH_MAX or not isinstance(
                    node["outputPaths"], list
                ):
                    raise _refuse()
                for item in node["outputPaths"]:
                    if not isinstance(item, str) or not item:
                        raise _refuse()
                    refs.append(item)
                    if len(refs) > _OUTPUT_REFS_MAX:
                        raise _refuse()
            for key, value in node.items():
                if key != "outputPaths":
                    walk(value, depth + 1)
        elif isinstance(node, list):
            for item in node:
                walk(item, depth + 1)

    for event in events:
        data = event.get("data") if isinstance(event, dict) else None
        if isinstance(data, dict):
            walk(data, 0)
    return list(dict.fromkeys(refs))


def _checked_managed_dir(data_root: Path) -> Path:
    """The managed directory must be a real, link-free directory."""
    managed = managed_dir(data_root)
    if not managed.is_dir() or managed.is_symlink():
        raise _refuse()
    try:
        resolved = managed.resolve(strict=True)
    except OSError as exc:
        raise _refuse() from exc
    if _key(resolved) != _key(managed):
        raise _refuse()  # a symlink somewhere in the chain
    return managed


def _asset_path(ref: str, managed: Path) -> Path:
    """Validate one reference before anything is read from it."""
    if "\x00" in ref:
        raise _refuse()
    path = Path(ref)
    if not path.is_absolute() or _key(path.parent) != _key(managed):
        raise _refuse()  # relative, foreign, or traversal escape
    if not _SPILL_NAME.fullmatch(path.name):
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


def collect_assets(refs: list[str], data_root: Path) -> dict[str, bytes]:
    """Prevalidate every reference before any read; bound the total bytes.

    Phase 1 validates all paths fail-closed (absolute, direct child of the
    managed directory, regular non-symlink file, unique record name) before
    a single byte is read. Phase 2 then reads under the remaining shared
    byte budget, guarding against replacement between validation and open
    by comparing fstat identity to the validated lstat, with O_NOFOLLOW
    where the platform provides it.
    """
    managed = _checked_managed_dir(data_root)
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
    records: dict[str, bytes], events: list[dict]
) -> dict[str, str]:
    """Explicit safe manifest validation for a future asset import.

    The manifest record must be canonical JSON; every listed ref must be a
    non-empty string mapped to a carried record whose bytes match the listed
    size and sha256 exactly; the listed refs must equal the refs actually
    present in the events. Returns ref -> record name; any mismatch refuses.
    """
    blob = records.get(MANIFEST_RECORD)
    if not isinstance(blob, bytes):
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
        if not isinstance(record, str) or not record.startswith(_ASSET_RECORD_PREFIX):
            raise _refuse()
        if not _SPILL_NAME.fullmatch(record[len(_ASSET_RECORD_PREFIX) :]):
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
    if set(listing) != set(output_refs(events)):
        raise _refuse()  # manifest must match the event refs exactly
    return listing
