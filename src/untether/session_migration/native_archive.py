"""Lossless opaque native session archive; no transcript prompt conversion."""

from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from dataclasses import dataclass

_SHA = re.compile(r"^[0-9a-f]{64}$")


class FidelityError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class NativeArchive:
    engine: str
    logical_id: str
    native_id: str
    generation: int
    lineage: str
    records: dict[str, bytes]
    digest: str


def _safe_name(name: str) -> bool:
    return bool(
        name
        and not name.startswith("/")
        and "\\" not in name
        and all(part not in ("", ".", "..") for part in name.split("/"))
        and not any(ord(c) < 32 for c in name)
    )


def encode_native(
    *,
    engine: str,
    logical_id: str,
    native_id: str,
    generation: int,
    lineage: str,
    records: dict[str, bytes],
    max_bytes: int,
) -> bytes:
    if (
        not all((engine, logical_id, native_id, lineage))
        or generation < 0
        or not records
        or any(
            not _safe_name(k) or not isinstance(v, bytes) for k, v in records.items()
        )
    ):
        raise FidelityError("ARCHIVE_INVALID")
    manifest = {
        "protocol": 2,
        "mode": "native",
        "engine": engine,
        "logical_id": logical_id,
        "native_id": native_id,
        "generation": generation,
        "lineage": lineage,
        "records": {
            k: {"size": len(v), "sha256": hashlib.sha256(v).hexdigest()}
            for k, v in sorted(records.items())
        },
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_STORED) as bundle:
        bundle.writestr("manifest.json", json.dumps(manifest, sort_keys=True))
        for name, data in sorted(records.items()):
            bundle.writestr("records/" + name, data)
    content = out.getvalue()
    if len(content) > max_bytes:
        raise FidelityError("ARCHIVE_TOO_LARGE")
    return content


def decode_native(data: bytes, *, max_archive: int, max_expanded: int) -> NativeArchive:
    if len(data) > max_archive:
        raise FidelityError("ARCHIVE_TOO_LARGE")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as bundle:
            names = bundle.namelist()
            if len(names) != len(set(names)) or "manifest.json" not in names:
                raise FidelityError("MEMBERS_INVALID")
            manifest_info = bundle.getinfo("manifest.json")
            if manifest_info.file_size > 16384:
                raise FidelityError("MANIFEST_TOO_LARGE")
            manifest = json.loads(bundle.read("manifest.json"))
            if (
                not isinstance(manifest, dict)
                or manifest.get("protocol") != 2
                or manifest.get("mode") != "native"
                or not isinstance(manifest.get("records"), dict)
                or not manifest["records"]
            ):
                raise FidelityError("SCHEMA_INVALID")
            specs = manifest["records"]
            if any(not isinstance(k, str) or not _safe_name(k) for k in specs):
                raise FidelityError("RECORD_PATH_INVALID")
            if set(names) != {"manifest.json"} | {"records/" + k for k in specs}:
                raise FidelityError("MEMBERS_INVALID")
            size = 0
            records = {}
            for member in bundle.infolist():
                if (
                    member.is_dir()
                    or member.flag_bits & 1
                    or member.compress_type != zipfile.ZIP_STORED
                    or (member.external_attr >> 16 & 0o170000) not in (0, 0o100000)
                ):
                    raise FidelityError("UNSAFE_MEMBER")
                size += member.file_size
                if size > max_expanded:
                    raise FidelityError("EXPANDED_LIMIT")
            for name, spec in specs.items():
                value = bundle.read("records/" + name)
                if (
                    not isinstance(spec, dict)
                    or type(spec.get("size")) is not int
                    or spec["size"] != len(value)
                    or not isinstance(spec.get("sha256"), str)
                    or not _SHA.fullmatch(spec["sha256"])
                    or hashlib.sha256(value).hexdigest() != spec["sha256"]
                ):
                    raise FidelityError("RECORD_HASH_MISMATCH")
                records[name] = value
            for key in ("engine", "logical_id", "native_id", "lineage"):
                if not isinstance(manifest.get(key), str) or not manifest[key]:
                    raise FidelityError("IDENTITY_INVALID")
            gen = manifest.get("generation")
            if type(gen) is not int or gen < 0:
                raise FidelityError("GENERATION_INVALID")
            return NativeArchive(
                manifest["engine"],
                manifest["logical_id"],
                manifest["native_id"],
                gen,
                manifest["lineage"],
                records,
                hashlib.sha256(data).hexdigest(),
            )
    except (zipfile.BadZipFile, KeyError, UnicodeError, json.JSONDecodeError) as exc:
        raise FidelityError("ARCHIVE_MALFORMED") from exc
