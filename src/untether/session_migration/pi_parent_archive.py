"""Pure preflight validation of the pi parent journals carried in a file bundle.

``file_bundle_adapter`` exports pi's ``parentSession`` lineage as a
``parents.json`` record plus ``parent-N.jsonl`` payloads; this module checks
that pairing before an import touches the store. It is byte-only and pure:
``parentSession`` pointers are parsed lexically as Windows or POSIX paths
(never opened, never resolved against this host's filesystem), and nothing is
read from or written to disk.

Pairing: ``parents.json`` lists the original basenames in
immediate-parent-first order, so ``parent-1.jsonl`` holds ``journals[0]``,
``parent-2.jsonl`` holds ``journals[1]``, and so on down the chain.

The shared gates (``_parents_record_parse``, ``_header_matches_session``,
``_MAX_RECORD_BYTES``) are imported from ``file_bundle_adapter`` inside the
call: that module will call this one once the importer is wired, and a
module-level import would close the cycle.
"""

import json
import re
from pathlib import PurePosixPath, PureWindowsPath

from .native_adapter import CapabilityUnavailable

_HEADER_BYTES = 65536
_PARENT_RECORD = re.compile(r"parent-(\d+)\.jsonl\Z")
_WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]|^\\\\")


def _bundle():
    from . import file_bundle_adapter

    return file_bundle_adapter


def _journal_header(data: object, limit: int) -> dict:
    """Decode one journal's bounded first line, mirroring the strict reader."""
    if not isinstance(data, bytes) or not data or len(data) > limit:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    if b"\n" not in data:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    first = data.split(b"\n", 1)[0]
    if len(first) > _HEADER_BYTES:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    try:
        header = json.loads(first)
    except (ValueError, UnicodeError) as exc:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED") from exc
    if not isinstance(header, dict):
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    return header


def _identity(header: dict) -> str:
    """Return the journal identity, refusing malformed or non-v3 session headers."""
    identity = header.get("id")
    if not isinstance(identity, str) or not identity:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    if not _bundle()._header_matches_session(header, identity):
        raise CapabilityUnavailable("SESSION_RECORD_FORMAT_REFUSED")
    return identity


def _pointer_basename(header: dict) -> str | None:
    """Basename of ``parentSession`` parsed as a Windows or POSIX path.

    The pointer is a path inside the source store. It is compared lexically
    only: the absolute source path is never opened, so a Windows path on a
    POSIX host still yields its basename and vice versa.
    """
    value = header.get("parentSession")
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    path = (
        PureWindowsPath(value) if _WINDOWS_PATH.match(value) else PurePosixPath(value)
    )
    if not path.name:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    return path.name


def validate_parent_records(
    records: dict[str, bytes], root_name: str, root_id: str
) -> list[tuple[str, bytes]]:
    """Validate one archive's parent records and pair them with their basenames.

    Args:
        records: Archive records keyed by record name; ``session.jsonl`` is the
            root journal and, when the session has parents, ``parents.json``
            plus contiguous ``parent-N.jsonl`` payloads are present.
        root_name: Original root journal basename (from the layout record).
        root_id: Declared identity of the root journal.

    Returns:
        ``(original basename, payload)`` pairs in immediate-parent-first order;
        an archive without parents returns ``[]``.

    Raises:
        CapabilityUnavailable: Fail-closed on an orphan or extra parent index,
            unsafe or root-colliding basename, duplicated journal identity
            (including the root), malformed/non-v3/non-session header, a parent
            chain link that names a different journal, or a chain whose last
            parent still points at another parent.
    """
    bundle = _bundle()
    root_payload = records.get("session.jsonl")
    if not isinstance(root_payload, bytes):
        raise CapabilityUnavailable("ROOT_RECORD_MISSING")
    if not isinstance(root_id, str) or not root_id:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    root_header = _journal_header(root_payload, limit=bundle._MAX_RECORD_BYTES)
    if _identity(root_header) != root_id:
        raise CapabilityUnavailable("SESSION_RECORD_FORMAT_REFUSED")

    payloads: dict[int, bytes] = {}
    payload_keys: list[str] = []
    for key, data in records.items():
        matched = _PARENT_RECORD.fullmatch(key)
        if matched:
            payload_keys.append(key)
            payloads[int(matched.group(1))] = data

    declared = records.get(bundle._PARENTS_RECORD)
    if declared is None:
        journals: list[str] = []
    elif isinstance(declared, bytes):
        journals = bundle._parents_record_parse(declared)
    else:
        raise CapabilityUnavailable("PARENTS_RECORD_REFUSED")

    expected = set(range(1, len(journals) + 1))
    if set(payloads) - expected or len(payloads) != len(payload_keys):
        raise CapabilityUnavailable("PARENT_RECORD_UNEXPECTED")
    if expected - set(payloads):
        raise CapabilityUnavailable("PARENT_REF_MISSING")
    if any(name.casefold() == root_name.casefold() for name in journals):
        raise CapabilityUnavailable("PARENTS_RECORD_REFUSED")

    identities = {root_id}
    headers: list[dict] = [root_header]
    for index in range(1, len(journals) + 1):
        header = _journal_header(payloads[index], limit=bundle._MAX_RECORD_BYTES)
        identity = _identity(header)
        if identity in identities:
            raise CapabilityUnavailable("PARENT_REF_CYCLE")
        identities.add(identity)
        headers.append(header)

    if journals:
        if _pointer_basename(root_header) != journals[0]:
            raise CapabilityUnavailable("PARENTS_CHAIN_MISMATCH")
    elif _pointer_basename(root_header) is not None:
        raise CapabilityUnavailable("PARENT_CHAIN_INCOMPLETE")

    for index in range(1, len(journals) + 1):
        found = _pointer_basename(headers[index])
        expected_next = journals[index] if index < len(journals) else None
        if expected_next is None:
            if found is not None:
                raise CapabilityUnavailable("PARENT_CHAIN_INCOMPLETE")
        elif found != expected_next:
            raise CapabilityUnavailable("PARENTS_CHAIN_MISMATCH")

    return [
        (journals[index - 1], payloads[index]) for index in range(1, len(journals) + 1)
    ]
