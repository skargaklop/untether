"""Restore collected OMP bundles into an operator-declared disposable store.

Counterpart to ``omp_bundle.collect_omp_records``: exact-byte journal rewrite
(including the 256-byte title slot), recursive sibling artifact recreation,
and canonical SHA-256-verified blob placement. Refuses collisions (O_EXCL),
symlinks/reparse points in existing ancestors, traversal, missing or mismatched
blobs, and identity drift before writes. Requires quiescent trusted roots:
concurrent hostile replacement is not protected against. Interrupted writes
can leave a partial disposable tree; discard it rather than activating it.
No production activation, adapter registration, or coordinator wiring.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path, PurePosixPath

from .omp_bundle import _checked, _journal

_BLOB_KEY = "blobs/"
_ARTIFACT_KEY = "artifacts/"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_JOURNAL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.jsonl$")


def _safe_relative(name: str) -> PurePosixPath:
    relative = PurePosixPath(name)
    if (
        name in ("", ".")
        or "\\" in name
        or relative.is_absolute()
        or ".." in relative.parts
        or "." in relative.parts
        or any(
            part.endswith((".", " "))
            or any(c in part for c in '<>:"|?*')
            or part.split(".")[0].upper()
            in {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"}
            or re.fullmatch(
                r"(?:COM|LPT)[1-9\u00b9\u00b2\u00b3]", part.split(".")[0].upper()
            )
            for part in relative.parts
        )
        or any(ord(c) < 32 for c in name)
        or str(relative) != name
    ):
        raise ValueError("OMP_RESTORE_PATH_INVALID")
    return relative


def _preflight(targets: list[tuple[Path, bytes]]) -> None:
    planned: set[tuple[str, ...]] = set()
    for path, _ in targets:
        if ".." in path.parts:
            raise ValueError("OMP_RESTORE_PATH_INVALID")
        for part in path.parts[1:] if path.anchor else path.parts:
            _safe_relative(part)
        absolute = Path(os.path.abspath(path))
        key = tuple(part.casefold() for part in absolute.parts)
        if key in planned:
            raise ValueError("OMP_RESTORE_TARGET_COLLISION")
        planned.add(key)
        # lstat detects dangling links too; _checked checks all existing ancestors.
        for ancestor in absolute.parents:
            try:
                ancestor.lstat()
            except FileNotFoundError:
                continue
            _checked(ancestor, directory=True)
            break
        try:
            absolute.lstat()
        except FileNotFoundError:
            continue
        raise FileExistsError(f"OMP_RESTORE_TARGET_EXISTS: {path.name}")
    if any(key[:i] in planned for key in planned for i in range(1, len(key))):
        raise ValueError("OMP_RESTORE_TARGET_COLLISION")


def _write_exclusive(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = None
    try:
        fd = os.open(path, flags, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as out:
            fd = None
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
    except FileExistsError:
        if fd is not None:
            os.close(fd)
        raise
    except OSError as exc:
        if fd is not None:
            os.close(fd)
        raise OSError(f"OMP_RESTORE_WRITE_FAILED: {path.name}") from exc


def restore_omp_records(
    *,
    records: dict[str, bytes],
    sessions_dir: Path,
    blobs_dir: Path,
    journal_name: str,
) -> Path:
    """Restore one collected bundle; returns the journal path for verification.

    ``journal_name`` is the source journal filename (stem carries the sibling
    artifact root); it must be supplied explicitly because the header does not
    encode it. ``blobs_dir`` is the canonical blob store root (hash files are
    written directly inside it; disposable stores pass ``<agent>/blobs/data``).
    """
    if not _SAFE_JOURNAL.fullmatch(journal_name):
        raise ValueError("OMP_RESTORE_JOURNAL_NAME_INVALID")
    _safe_relative(journal_name)
    journal_bytes = records.get("session.jsonl")
    if journal_bytes is None:
        raise ValueError("OMP_RESTORE_MISSING_JOURNAL")
    journal_path = sessions_dir / journal_name
    sibling_root = journal_path.with_suffix("")
    targets: list[tuple[Path, bytes]] = []
    for name, data in records.items():
        if name == "session.jsonl":
            continue
        if name.startswith(_BLOB_KEY):
            digest = name[len(_BLOB_KEY) :]
            if not _HEX64.fullmatch(digest):
                raise ValueError("OMP_RESTORE_BLOB_NAME_INVALID")
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("OMP_RESTORE_BLOB_MISMATCH")
            targets.append((blobs_dir / digest, data))
        elif name.startswith(_ARTIFACT_KEY):
            relative = _safe_relative(name[len(_ARTIFACT_KEY) :])
            targets.append((sibling_root / Path(*relative.parts), data))
        else:
            raise ValueError("OMP_RESTORE_RECORD_UNKNOWN")
    # Validate every child journal identity before writing anything: a nested
    # session with the same id would be a second copy, not an artifact.
    header_id, refs = _journal(journal_bytes)
    for path, data in targets:
        if path.suffix.casefold() == ".jsonl":
            identity, found = _journal(data)
            refs.update(found)
            if identity == header_id:
                raise ValueError("OMP_RESTORE_IDENTITY_COLLISION")
    if any(_BLOB_KEY + digest not in records for digest in refs):
        raise ValueError("OMP_RESTORE_MISSING_BLOB")
    _preflight([(journal_path, journal_bytes), *targets])
    _write_exclusive(journal_path, journal_bytes)
    for path, data in targets:
        _write_exclusive(path, data)
    return journal_path
