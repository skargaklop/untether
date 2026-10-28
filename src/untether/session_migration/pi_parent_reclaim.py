"""Reclaim preflight for pi's shared, immutable parent journals.

A reclaim replaces the reclaiming session's own journal and metadata; pi's
``parentSession`` lineage is different in kind. A parent journal is cited by
every descendant, so it is shared lineage that no single reclaim owns and that
a reclaim must never rewrite, adopt, or silently re-hash. This module is the
fail-closed gate for that boundary: byte-only and pure apart from bounded reads
of the declared store's parent files.

It runs the pure archive validator
(:func:`pi_parent_archive.validate_parent_records`) over BOTH the incoming and
the expected-prior archives, refuses any disagreement between them about the
``parents.json`` manifest or the indexed ``parent-N.jsonl`` payloads, then
proves that every declared destination parent already exists in the store as one
safe regular (non-symlink) file whose exact bytes equal the incoming payload.

Because it never writes, ``file_bundle_adapter`` can call it once as preflight
and once again as a postprobe, after its restart probe, with the same arguments.

Root-journal evolution with an unchanged parent lineage is expected and
accepted; layout and metadata parity stay the adapter's concern.
"""

from __future__ import annotations

from pathlib import Path

from .native_adapter import CapabilityUnavailable
from .pi_parent_archive import validate_parent_records


def _bundle():
    """Import ``file_bundle_adapter`` at call time: it lazily imports this module."""
    from . import file_bundle_adapter

    return file_bundle_adapter


def _manifest(records: dict[str, bytes]) -> bytes | None:
    """Raw ``parents.json`` record, or ``None`` for an archive without parents."""
    return records.get(_bundle()._PARENTS_RECORD)


def validate_reclaim_parents(
    sessions_dir: Path,
    records: dict[str, bytes],
    expected_prior_records: dict[str, bytes],
    root_name: str,
    native_id: str,
) -> None:
    """Refuse a reclaim whose parent lineage is not already shared immutable bytes.

    Args:
        sessions_dir: Declared store directory. The adapter owns store
            validation; this function only reads the declared parent files.
        records: Incoming archive records: the root ``session.jsonl``, the
            optional ``parents.json`` manifest, and any ``parent-N.jsonl``.
        expected_prior_records: The archive the store is expected to hold now,
            in the same record shape.
        root_name: Destination root journal basename (from the layout record).
        native_id: Declared session identity, unchanged by a reclaim.

    Returns:
        ``None`` on success; the helper reports only refusal.

    Raises:
        CapabilityUnavailable: ``PARENTS_*``/``SESSION_*`` from the pure
            validator for a malformed chain; ``RECLAIM_PARENT_DIVERGED`` when
            the archives disagree about the manifest or indexed payloads;
            ``RECLAIM_PARENT_UNSAFE``/``RECLAIM_PARENT_MISSING`` for an unsafe
            or absent destination; ``RECLAIM_PARENT_MISMATCH`` when the stored
            parent bytes differ from the incoming payload.
    """
    incoming = validate_parent_records(records, root_name, native_id)
    prior = validate_parent_records(expected_prior_records, root_name, native_id)
    # Parents are shared lineage: prior and incoming must describe the same
    # immutable journals byte-for-byte. Differing local bytes are never kept
    # and never certified against remote hashes.
    if incoming != prior or _manifest(records) != _manifest(expected_prior_records):
        raise CapabilityUnavailable("RECLAIM_PARENT_DIVERGED")
    bundle = _bundle()
    for basename, payload in incoming:
        destination = sessions_dir / basename
        if destination.is_symlink():
            raise CapabilityUnavailable("RECLAIM_PARENT_UNSAFE")
        if not destination.exists():
            raise CapabilityUnavailable("RECLAIM_PARENT_MISSING")
        if not destination.is_file():
            raise CapabilityUnavailable("RECLAIM_PARENT_UNSAFE")
        if not bundle._capped_bytes_eq(destination, payload):
            raise CapabilityUnavailable("RECLAIM_PARENT_MISMATCH")
