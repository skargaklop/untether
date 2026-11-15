"""Strict bounded package codec; session histories are untrusted bytes."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass

PROTOCOL = 1
MAX_MANIFEST = 16384
MEMBER = "context.json"


class PackageError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ContextPackage:
    engine: str
    project: str
    payload: bytes
    digest: str


def pack_context(*, engine: str, project: str, content: bytes, max_bytes: int) -> bytes:
    """Package explicitly exported context, NOT a native Pi/OMP session."""
    if not engine or not project or len(content) > max_bytes:
        raise PackageError("BOUNDS_OR_METADATA")
    manifest = {
        "protocol": PROTOCOL,
        "mode": "context",
        "engine": engine,
        "project": project,
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    if len(raw) > MAX_MANIFEST:
        raise PackageError("MANIFEST_TOO_LARGE")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as out:
        out.writestr("manifest.json", raw)
        out.writestr(MEMBER, content)
    result = buf.getvalue()
    if len(result) > max_bytes:
        raise PackageError("ARCHIVE_TOO_LARGE")
    return result


def unpack_context(
    data: bytes, *, max_archive: int, max_expanded: int
) -> ContextPackage:
    if len(data) > max_archive:
        raise PackageError("ARCHIVE_TOO_LARGE")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = archive.namelist()
            if len(names) != 2 or set(names) != {"manifest.json", MEMBER}:
                raise PackageError("MEMBERS_INVALID")
            total = 0
            for entry in archive.infolist():
                total += entry.file_size
                if (
                    entry.is_dir()
                    or entry.flag_bits & 1
                    or entry.compress_type != zipfile.ZIP_STORED
                ):
                    raise PackageError("UNSAFE_MEMBER")
                if entry.external_attr >> 16 & 0o170000 not in (0, 0o100000):
                    raise PackageError("UNSAFE_MEMBER")
            if (
                total > max_expanded
                or archive.getinfo("manifest.json").file_size > MAX_MANIFEST
            ):
                raise PackageError("EXPANDED_LIMIT")
            metadata = json.loads(archive.read("manifest.json"))
            if (
                not isinstance(metadata, dict)
                or metadata.get("protocol") != PROTOCOL
                or metadata.get("mode") != "context"
            ):
                raise PackageError("PROTOCOL_INVALID")
            content = archive.read(MEMBER)
            if (
                type(metadata.get("size")) is not int
                or metadata["size"] != len(content)
                or hashlib.sha256(content).hexdigest() != metadata.get("sha256")
            ):
                raise PackageError("HASH_MISMATCH")
            if any(
                not isinstance(metadata.get(k), str) or not metadata[k]
                for k in ("engine", "project")
            ):
                raise PackageError("METADATA_INVALID")
            return ContextPackage(
                metadata["engine"], metadata["project"], content, metadata["sha256"]
            )
    except (
        zipfile.BadZipFile,
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        KeyError,
    ) as exc:
        raise PackageError("PACKAGE_MALFORMED") from exc
