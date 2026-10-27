"""Single shared native transfer coordinator: one strict path for push and pull.

Both directions carry an explicit session id and engine, preserve the
logical/native identity exactly as RestoreProof allows, verify hashes on
every hop, write receipts once, and bind a destination chat through public
stores only after a verified restore. No transcript fallback, no per-session
descriptors, no arbitrary file access.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import anyio
from pydantic import BaseModel, ConfigDict

from . import reclaim as _reclaim
from .file_bundle_adapter import register_from_settings as _register_file_bundle
from .intent import OperationConflict, OperationIntentStore
from .native_adapter import (
    CapabilityUnavailable,
    assert_exact_restore,
    registered_adapter_engines,
    require_persistence_adapter,
)
from .native_archive import FidelityError, NativeArchive, decode_native, encode_native
from .receipt import Receipt, ReceiptError, ReceiptStore
from .reclaim import _read_imported
from .settings import MigrationPeer, SessionMigrationSettings
from .source_fence import (
    activate_source_fence,
    complete_source_fence,
    place_source_fence,
    read_source_fence,
)
from .telegram_enrollment import EnrollmentError, EnrollmentStore

SESSION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
ENGINE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")

_TRANSITIONS: dict[str, frozenset[str]] = {
    "prepared": frozenset(("imported",)),
    "imported": frozenset(("verified", "completed")),
    "verified": frozenset(("bound",)),
    "bound": frozenset(),
    "completed": frozenset(),
}


class TransferRefusal(CapabilityUnavailable):
    """Redacted code-only refusal; safe to show in chat or CLI output."""


# Redacted-refusal alias so the command layer catches every coordinator
# failure (including engine-adapter unavailability) without leaking detail.
CapabilityRefusal = TransferRefusal


@dataclass(frozen=True, slots=True)
class TransferResult:
    operation_id: str
    session_id: str
    engine: str
    direction: str
    peer: str
    state: str
    native_id: str
    receipt_sha256: str


class TransferRecord(BaseModel):
    """File-local contract for one bounded native transfer operation."""

    model_config = ConfigDict(extra="forbid")

    operation_id: str
    engine: str
    session_id: str
    direction: str
    peer: str
    project: str
    state: str
    identity_sha256: str
    native_id: str | None = None
    generation: int | None = None
    lineage: str | None = None
    receipt_sha256: str | None = None
    destination_id: str | None = None
    destination_binding: str | None = None
    target_sha256: str | None = None


ReceiverCall = Callable[..., dict]

__all__ = [
    "TransferRecord",
    "TransferRefusal",
    "TransferResult",
    "build_inbound_package",
    "perform",
    "registered_adapter_engines",
]


def build_inbound_package(
    cfg: SessionMigrationSettings, archive: NativeArchive
) -> bytes:
    """Encode an exported native archive into the bounded wire package."""
    return encode_native(
        engine=archive.engine,
        logical_id=archive.logical_id,
        native_id=archive.native_id,
        generation=archive.generation,
        lineage=archive.lineage,
        records=archive.records,
        max_bytes=cfg.max_archive_bytes,
    )


def _strict_identity(archive: NativeArchive, engine: str) -> None:
    if archive.engine != engine:
        raise CapabilityUnavailable("ENGINE_MISMATCH")
    if not (archive.native_id and archive.logical_id and archive.lineage):
        raise TransferRefusal("IDENTITY_INVALID")


def _identity_sha256(archive: NativeArchive) -> str:
    identity = json.dumps(
        {
            "engine": archive.engine,
            "logical_id": archive.logical_id,
            "native_id": archive.native_id,
            "generation": archive.generation,
            "lineage": archive.lineage,
            "records": {
                name: hashlib.sha256(blob).hexdigest()
                for name, blob in sorted(archive.records.items())
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(identity.encode()).hexdigest()


def _operation_id(
    engine: str, session_id: str, identity: str, peer: str, project: str, direction: str
) -> str:
    """Deterministic per transfer identity: replays reuse the same operation."""
    return str(
        uuid5(
            NAMESPACE_URL,
            f"untether-native-migration:{engine}:{session_id}:{identity}:{peer}:{project}:{direction}",
        )
    )


def _validate_call(
    cfg: SessionMigrationSettings,
    *,
    peer_name: str,
    project: str,
    session_id: str,
    engine: str,
    direction: str,
) -> MigrationPeer:
    if not SESSION_ID.fullmatch(session_id):
        raise TransferRefusal("SESSION_ID_INVALID")
    if not ENGINE_ID.fullmatch(engine):
        raise TransferRefusal("ENGINE_ID_INVALID")
    if direction not in ("to", "from"):
        raise TransferRefusal("DIRECTION_INVALID")
    if not cfg.enabled:
        raise TransferRefusal("MIGRATION_DISABLED")
    peer = cfg.peers.get(peer_name)
    if peer is None:
        raise TransferRefusal("PEER_UNKNOWN")
    if project not in peer.projects:
        raise TransferRefusal("PROJECT_UNMAPPED")
    return peer


def _operations(root: Path) -> Path:
    return root / "operations"


def _read(root: Path, operation_id: str) -> TransferRecord | None:
    path = root / (operation_id + ".json")
    if path.is_symlink():
        raise TransferRefusal("OPERATION_SYMLINK")
    if not path.exists():
        return None
    try:
        record = TransferRecord.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TransferRefusal("OPERATION_CORRUPT") from exc
    if record.operation_id != operation_id or record.state not in _TRANSITIONS:
        raise TransferRefusal("OPERATION_CORRUPT")
    return record


def _write(root: Path, record: TransferRecord, *, create: bool) -> None:
    if root.is_symlink():
        raise TransferRefusal("OPERATION_DIR_SYMLINK")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / (record.operation_id + ".json")
    if path.is_symlink():
        raise TransferRefusal("OPERATION_SYMLINK")
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=root, prefix=".transfer-", delete=False
        ) as out:
            tmp = Path(out.name)
            out.write(record.model_dump_json())
            out.flush()
            os.fsync(out.fileno())
        if create:
            try:
                os.link(tmp, path)
            except FileExistsError as exc:
                raise TransferRefusal("OPERATION_ALREADY_EXISTS") from exc
        else:
            os.replace(tmp, path)
    finally:
        if tmp:
            tmp.unlink(missing_ok=True)


def _transition(
    root: Path, record: TransferRecord, state: str, **fields: str | None
) -> TransferRecord:
    if state not in _TRANSITIONS.get(record.state, frozenset()):
        raise TransferRefusal(f"ILLEGAL_TRANSITION: {record.state}->{state}")
    update = {"state": state}
    update.update({k: v for k, v in fields.items() if v is not None})
    updated = record.model_copy(update=update)
    if not updated.destination_id or not updated.receipt_sha256:
        raise TransferRefusal("RECEIPT_REQUIRED")
    _write(root, updated, create=False)
    return updated


def _result(record: TransferRecord, peer_name: str) -> TransferResult:
    return TransferResult(
        operation_id=record.operation_id,
        session_id=record.session_id,
        engine=record.engine,
        direction=record.direction,
        peer=peer_name,
        state=record.state,
        native_id=record.native_id or "",
        receipt_sha256=record.receipt_sha256 or "",
    )


def _verify_reply(
    reply: object,
    *,
    operation_id: str,
    digest: str,
    engine: str,
    native_id: str,
    endpoint_id: str,
) -> None:
    required = {
        "protocol",
        "endpoint_id",
        "operation_id",
        "package_sha256",
        "importer_version",
        "native_id",
        "status",
        "engine",
    }
    if not isinstance(reply, dict) or set(reply) != required:
        raise TransferRefusal("RECEIVER_REPLY_SCHEMA")
    if reply["protocol"] != 1 or reply["operation_id"] != operation_id:
        raise TransferRefusal("RECEIVER_OPERATION_MISMATCH")
    if reply["engine"] != engine:
        raise TransferRefusal("RECEIVER_ENGINE_MISMATCH")
    if reply["endpoint_id"] != endpoint_id:
        raise TransferRefusal("RECEIVER_ENDPOINT_MISMATCH")
    if reply["package_sha256"] != digest:
        raise TransferRefusal("RECEIVER_PACKAGE_MISMATCH")
    if (
        reply["status"] != "verified"
        or not isinstance(reply["native_id"], str)
        or reply["native_id"] != native_id
        or not isinstance(reply["importer_version"], str)
        or not reply["importer_version"]
    ):
        raise TransferRefusal("RECEIVER_NOT_VERIFIED")


def _ssh_receiver_call(
    peer: MigrationPeer, direction: str, cfg: SessionMigrationSettings
) -> ReceiverCall:
    """Default transport: the fixed-verb SSH RPC over the real receiver."""
    from .remote import receiver_request

    def call(
        operation_id: str,
        package: bytes = b"",
        *,
        peer: MigrationPeer,
        project: str,
        engine: str,
        action: str = "receive",
        session_id: str | None = None,
    ) -> dict:
        return receiver_request(
            peer,
            project=project,
            direction=direction,
            action=action,
            operation_id=operation_id,
            package=package,
            engine=engine,
            session_id=session_id,
            max_package_bytes=cfg.max_archive_bytes,
            timeout_s=cfg.transfer_timeout_s,
        )

    return call


def _cache_path(root: Path, operation_id: str, digest: str) -> Path:
    if str(UUID(operation_id)) != operation_id or not re.fullmatch(
        r"[0-9a-f]{64}", digest
    ):
        raise TransferRefusal("PACKAGE_CACHE_ID_INVALID")
    if root.is_symlink() or root.resolve() != root.absolute():
        raise TransferRefusal("PACKAGE_CACHE_UNSAFE")
    return root / f"{operation_id}-{digest}.package"


def _cached_package(root: Path, operation_id: str, digest: str, limit: int) -> bytes:
    path = _cache_path(root, operation_id, digest)
    if path.is_symlink():
        raise TransferRefusal("PACKAGE_CACHE_UNSAFE")
    try:
        with path.open("rb") as stream:
            package = stream.read(limit + 1)
    except OSError as exc:
        raise TransferRefusal("PACKAGE_CACHE_UNAVAILABLE") from exc
    if len(package) > limit or hashlib.sha256(package).hexdigest() != digest:
        raise TransferRefusal("PACKAGE_CACHE_CORRUPT")
    return package


def _cache_package(
    root: Path, operation_id: str, digest: str, package: bytes, limit: int
) -> None:
    path = _cache_path(root, operation_id, digest)
    if len(package) > limit:
        raise TransferRefusal("PACKAGE_CACHE_LIMIT")
    tmp = None
    try:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with tempfile.NamedTemporaryFile("wb", dir=root, delete=False) as out:
            tmp = Path(out.name)
            out.write(package)
            out.flush()
            os.fsync(out.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            if _cached_package(root, operation_id, digest, limit) != package:
                raise TransferRefusal("PACKAGE_CACHE_CONFLICT") from None
        if os.name == "posix":
            fd = os.open(root, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    except OSError as exc:
        raise TransferRefusal("PACKAGE_CACHE_WRITE_FAILED") from exc
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)


def _push(
    cfg: SessionMigrationSettings,
    *,
    peer_name: str,
    peer: MigrationPeer,
    project: str,
    session_id: str,
    engine: str,
    root: Path,
    receiver_call: ReceiverCall | None,
) -> TransferResult:
    gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
    if not gate_root:
        raise TransferRefusal("SOURCE_GATE_UNCONFIGURED")
    fences = Path(gate_root) / "fences"
    ops = _operations(root)
    target = hashlib.sha256(peer.model_dump_json().encode()).hexdigest()
    fence = read_source_fence(fences, engine=engine, native_id=session_id)
    if fence is not None and fence.state != "active":
        op_id, digest = fence.operation_id, fence.package_sha256
        record = _read(ops, op_id)
        if record is None or (
            record.engine,
            record.session_id,
            record.direction,
            record.peer,
            record.project,
            record.target_sha256,
        ) != (engine, session_id, "to", peer_name, project, target):
            raise TransferRefusal("OPERATION_CONFLICT")
        package = _cached_package(ops, op_id, digest, cfg.max_archive_bytes)
        try:
            archive = decode_native(
                package,
                max_archive=cfg.max_archive_bytes,
                max_expanded=cfg.max_expanded_bytes,
            )
        except FidelityError as exc:
            raise TransferRefusal("PACKAGE_CACHE_CORRUPT") from exc
        _strict_identity(archive, engine)
        if (
            archive.logical_id != session_id
            or archive.native_id != record.native_id
            or archive.generation != record.generation
            or archive.lineage != record.lineage
            or _identity_sha256(archive) != record.identity_sha256
            or _operation_id(
                engine, session_id, record.identity_sha256, peer_name, project, "to"
            )
            != op_id
        ):
            raise TransferRefusal("OPERATION_CONFLICT")
        if record.state == "completed":
            if record.receipt_sha256 != digest or fence.state != "completed":
                raise TransferRefusal("OPERATION_CONFLICT")
            return _result(record, peer_name)
    else:
        exporter = require_persistence_adapter(engine)
        snapshot = getattr(exporter, "export_snapshot", None)
        if not callable(snapshot):
            raise TransferRefusal("SNAPSHOT_ADMISSION_UNAVAILABLE")
        with ExitStack() as held:
            archive = held.enter_context(snapshot(session_id))
            _strict_identity(archive, engine)
            if archive.native_id != session_id:
                from .session_gate import session_gate

                held.enter_context(
                    session_gate(
                        root=Path(gate_root),
                        engine_id=engine,
                        native_id=archive.native_id,
                    )
                )
            if archive.logical_id != session_id:
                raise TransferRefusal("SESSION_IDENTITY_MISMATCH")
            package = build_inbound_package(cfg, archive)
            digest = hashlib.sha256(package).hexdigest()
            identity = _identity_sha256(archive)
            op_id = _operation_id(
                engine, session_id, identity, peer_name, project, "to"
            )
            _cache_package(ops, op_id, digest, package, cfg.max_archive_bytes)
            record = TransferRecord(
                operation_id=op_id,
                engine=engine,
                session_id=session_id,
                direction="to",
                peer=peer_name,
                project=project,
                state="prepared",
                identity_sha256=identity,
                native_id=archive.native_id,
                generation=archive.generation,
                lineage=archive.lineage,
                target_sha256=target,
            )
            existing = _read(ops, op_id)
            if existing is None:
                _write(ops, record, create=True)
            elif existing != record:
                raise TransferRefusal("OPERATION_CONFLICT")
            for native_id in dict.fromkeys((session_id, archive.native_id)):
                place_source_fence(
                    fences,
                    engine=engine,
                    native_id=native_id,
                    operation_id=op_id,
                    package_sha256=digest,
                )
    for native_id in dict.fromkeys((session_id, archive.native_id)):
        current = read_source_fence(fences, engine=engine, native_id=native_id)
        if (
            current is None
            or current.operation_id != op_id
            or current.package_sha256 != digest
        ):
            raise TransferRefusal("OPERATION_CONFLICT")
    call = receiver_call or _ssh_receiver_call(peer, "to", cfg)
    reply = call(op_id, package, peer=peer, project=project, engine=engine)
    _verify_reply(
        reply,
        operation_id=op_id,
        digest=digest,
        engine=engine,
        native_id=archive.native_id,
        endpoint_id=peer.expected_endpoint_id,
    )
    if record.state == "prepared":
        record = _transition(
            ops,
            record,
            "imported",
            destination_id=archive.native_id,
            receipt_sha256=digest,
        )
    if record.state != "imported" or record.receipt_sha256 != digest:
        raise TransferRefusal("OPERATION_CONFLICT")
    for native_id in dict.fromkeys((session_id, archive.native_id)):
        current = read_source_fence(fences, engine=engine, native_id=native_id)
        if current is None:
            raise TransferRefusal("FENCE_MISSING")
        if current.state == "pending":
            complete_source_fence(
                fences,
                engine=engine,
                native_id=native_id,
                operation_id=op_id,
                package_sha256=digest,
            )
        elif current.operation_id != op_id or current.package_sha256 != digest:
            raise TransferRefusal("OPERATION_CONFLICT")
    record = _transition(ops, record, "completed")
    return _result(record, peer_name)


def _prior_pushed_records(ops: Path, prior_op_id: str, archive: NativeArchive):
    return _reclaim._prior_pushed_records(
        ops, prior_op_id, archive, read_record=_read, cached_package=_cached_package
    )


def _reclaim_import(**kwargs) -> tuple[str, NativeArchive]:
    return _reclaim._reclaim_import(
        **kwargs,
        read_record=_read,
        cached_package=_cached_package,
        identity_sha256=_identity_sha256,
        refusal=TransferRefusal,
    )


def _finalize_reclaim(
    archive: NativeArchive, record: TransferRecord, digest: str
) -> None:
    _reclaim._finalize_reclaim(
        archive,
        record,
        digest,
        identity_sha256=_identity_sha256,
        refusal=TransferRefusal,
        activate_fence=activate_source_fence,
    )


def _complete_remote_export(
    call: ReceiverCall | None,
    export_op: str | None,
    *,
    peer: MigrationPeer,
    project: str,
    engine: str,
    session_id: str,
    digest: str,
) -> None:
    if export_op is None or call is None:
        return
    reply = call(
        export_op,
        json.dumps({"package_sha256": digest}).encode(),
        peer=peer,
        project=project,
        engine=engine,
        action="export-complete",
        session_id=session_id,
    )
    if reply != {
        "status": "export_completed",
        "operation_id": export_op,
        "engine": engine,
        "session_id": session_id,
        "package_sha256": digest,
        "endpoint_id": peer.expected_endpoint_id,
    }:
        raise TransferRefusal("REMOTE_EXPORT_COMPLETION_MISMATCH")


def _pull_unlocked(
    cfg: SessionMigrationSettings,
    *,
    peer_name: str,
    peer: MigrationPeer,
    project: str,
    session_id: str,
    engine: str,
    root: Path,
    config_path: Path | None,
    receiver_call: ReceiverCall | None,
    incoming_package: bytes | None,
    destination_chat: int | None,
    destination_topic: int | None,
    destination_bot_id: int | None,
    source_bot_id: int | None,
) -> TransferResult:
    export_op = None
    call = None
    if incoming_package is None:
        call = receiver_call or _ssh_receiver_call(peer, "from", cfg)
        export_op = str(
            uuid5(
                NAMESPACE_URL,
                f"untether-native-export:{engine}:{session_id}:{peer_name}:{project}",
            )
        )
        export_reply = call(
            export_op,
            b"",
            peer=peer,
            project=project,
            engine=engine,
            action="export",
            session_id=session_id,
        )
        if not isinstance(export_reply, dict):
            raise TransferRefusal("RECEIVER_REPLY_SCHEMA")
        if export_reply.get("status") != "exported":
            raise TransferRefusal("REMOTE_EXPORT_FAILED")
        incoming_package = export_reply.get("package")
        if incoming_package is None and "package_base64" in export_reply:
            try:
                incoming_package = base64.b64decode(export_reply["package_base64"])
            except Exception as exc:
                raise TransferRefusal("PACKAGE_MALFORMED") from exc
        if incoming_package is None:
            raise TransferRefusal("PACKAGE_REQUIRED")
        if (
            export_reply.get("package_sha256")
            and hashlib.sha256(incoming_package).hexdigest()
            != export_reply["package_sha256"]
        ):
            raise TransferRefusal("PACKAGE_HASH_MISMATCH")
    digest = hashlib.sha256(incoming_package).hexdigest()
    try:
        archive = decode_native(
            incoming_package,
            max_archive=cfg.max_archive_bytes,
            max_expanded=cfg.max_expanded_bytes,
        )
    except FidelityError as exc:
        raise TransferRefusal(f"ARCHIVE_UNSAFE: {exc}") from exc
    _strict_identity(archive, engine)
    if archive.logical_id != session_id:
        raise TransferRefusal("SESSION_IDENTITY_MISMATCH")
    identity = _identity_sha256(archive)
    op_id = _operation_id(engine, session_id, identity, peer_name, project, "from")
    ops = _operations(root)
    existing = _read(ops, op_id)
    if existing is not None:
        if existing.identity_sha256 != identity or existing.engine != engine:
            raise TransferRefusal("OPERATION_CONFLICT")
        if existing.state == "bound" or (
            existing.state == "verified" and destination_chat is None
        ):
            if existing.receipt_sha256 != digest:
                raise TransferRefusal("RECEIPT_MISMATCH")
            _finalize_reclaim(archive, existing, digest)
            _complete_remote_export(
                call,
                export_op,
                peer=peer,
                project=project,
                engine=engine,
                session_id=session_id,
                digest=digest,
            )
            return _result(existing, peer_name)
        record = existing
    else:
        record = TransferRecord(
            operation_id=op_id,
            engine=engine,
            session_id=session_id,
            direction="from",
            peer=peer_name,
            project=project,
            state="prepared",
            identity_sha256=identity,
            native_id=archive.native_id,
            generation=archive.generation,
            lineage=archive.lineage,
        )
        _write(ops, record, create=True)
    if record.state == "verified":
        # Retry of an already verified pull: reuse the stored verified receipt,
        # import exactly once, and run only the binding routine now.
        if record.receipt_sha256 != digest:
            raise TransferRefusal("RECEIPT_MISMATCH")
        native_id = record.destination_id or record.native_id or ""
        digest = record.receipt_sha256
    else:
        try:
            importer = require_persistence_adapter(engine)
        except CapabilityUnavailable as exc:
            raise TransferRefusal(
                str(exc) or "ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE"
            ) from exc
        intents = OperationIntentStore(root / "native" / "intents")
        try:
            # Reserve BEFORE the irreversible adapter import; conflict stays ambiguous.
            intents.begin(op_id, digest)
        except OperationConflict as exc:
            gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
            fence = (
                read_source_fence(
                    Path(gate_root) / "fences",
                    engine=engine,
                    native_id=archive.native_id,
                )
                if gate_root
                else None
            )
            intent = intents.inspect(op_id)
            if (
                fence is None
                or fence.state != "pending"
                or fence.operation_id != op_id
                or fence.package_sha256 != digest
                or not fence.prior_operation_id
                or not intent
                or intent.get("sha256") != digest
            ):
                raise TransferRefusal(str(exc)) from exc
        try:
            proof = importer.import_exact(archive)
            native_id = assert_exact_restore(archive, proof)
            reloaded = _read_imported(importer, native_id)
        except CapabilityUnavailable as exc:
            if str(exc) != "SESSION_ID_COLLISION":
                raise TransferRefusal(str(exc) or "RESTORE_UNVERIFIED") from exc
            native_id, reloaded = _reclaim_import(
                importer=importer,
                archive=archive,
                op_id=op_id,
                digest=digest,
                peer_name=peer_name,
                project=project,
                ops=ops,
            )
        if (
            reloaded.records != archive.records
            or reloaded.engine != archive.engine
            or reloaded.logical_id != archive.logical_id
            or reloaded.native_id != archive.native_id
            or reloaded.lineage != archive.lineage
            or reloaded.generation != archive.generation
        ):
            raise TransferRefusal("RESTORE_IDENTITY_MISMATCH")
        try:
            ReceiptStore(root / "native" / "receipts").record(
                Receipt(op_id, digest, "native-v1", native_id, "verified")
            )
        except ReceiptError as exc:
            raise TransferRefusal(str(exc)) from exc
        record = _transition(
            ops, record, "imported", destination_id=native_id, receipt_sha256=digest
        )
        record = _transition(ops, record, "verified")
    _finalize_reclaim(archive, record, digest)
    if destination_chat is None:
        _complete_remote_export(
            call,
            export_op,
            peer=peer,
            project=project,
            engine=engine,
            session_id=session_id,
            digest=digest,
        )
        return _result(record, peer_name)

    from ..model import ResumeToken
    from ..telegram.chat_sessions import ChatSessionStore, resolve_sessions_path
    from ..telegram.topic_state import TopicStateStore, resolve_state_path

    cfg_file = config_path or (root.parent / "untether.toml")
    token = ResumeToken(engine=engine, value=native_id)
    try:
        if destination_topic is not None:
            tstore = TopicStateStore(resolve_state_path(cfg_file))
            anyio.run(
                tstore.set_session_resume, destination_chat, destination_topic, token
            )
            binding = f"topic:{destination_chat}:{destination_topic}"
        else:
            cstore = ChatSessionStore(resolve_sessions_path(cfg_file))
            anyio.run(cstore.set_session_resume, destination_chat, None, token)
            binding = f"chat:{destination_chat}"
    except Exception as exc:
        raise TransferRefusal(f"SESSION_BINDING_FAILED: {exc}") from exc

    if destination_bot_id is not None and source_bot_id is not None:
        try:
            EnrollmentStore(root / "enrollment.json").enroll(
                bot_id=destination_bot_id,
                chat_id=destination_chat,
                topic_id=destination_topic,
                peer_name=peer_name,
                endpoint_id=peer.expected_endpoint_id,
                confirmed=True,
                source_bot_id=source_bot_id,
            )
        except EnrollmentError as exc:
            raise TransferRefusal(str(exc)) from exc

    record = _transition(
        ops,
        record,
        "bound",
        destination_binding=binding,
    )
    _complete_remote_export(
        call,
        export_op,
        peer=peer,
        project=project,
        engine=engine,
        session_id=session_id,
        digest=digest,
    )
    return _result(record, peer_name)


def _pull(cfg: SessionMigrationSettings, **kwargs) -> TransferResult:
    from .session_gate import migration_lock

    gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
    if not gate_root:
        raise TransferRefusal("SOURCE_GATE_UNCONFIGURED")
    with migration_lock(
        root=Path(gate_root), engine_id=kwargs["engine"], native_id=kwargs["session_id"]
    ):
        return _pull_unlocked(cfg, **kwargs)


def _ensure_engine_adapter(cfg: SessionMigrationSettings, engine: str) -> None:
    """Config-boundary adapter registration, shared by CLI and Telegram paths.

    Registers the in-tree file-bundle adapter for any engine with a declared
    persistence profile exactly once per process; engines without a profile
    stay with entry-point plugin adapters (or refuse natively).
    """
    if engine not in registered_adapter_engines():
        _register_file_bundle(cfg, engine=engine)


def perform(
    cfg: SessionMigrationSettings,
    *,
    peer_name: str,
    project: str,
    session_id: str,
    engine: str,
    direction: str,
    root: Path,
    config_path: Path | None = None,
    receiver_call: ReceiverCall | None = None,
    incoming_package: bytes | None = None,
    destination_chat: int | None = None,
    destination_topic: int | None = None,
    destination_bot_id: int | None = None,
    source_bot_id: int | None = None,
) -> TransferResult:
    """Execute one bounded native session transfer with verified receipts."""
    peer = _validate_call(
        cfg,
        peer_name=peer_name,
        project=project,
        session_id=session_id,
        engine=engine,
        direction=direction,
    )
    # Config-boundary wiring so both the CLI and Telegram /session resolve the
    # same declared persistence adapter; engines without a profile keep their
    # entry-point plugin (or refuse) unchanged.
    _ensure_engine_adapter(cfg, engine)
    if direction == "to":
        return _push(
            cfg,
            peer_name=peer_name,
            peer=peer,
            project=project,
            session_id=session_id,
            engine=engine,
            root=root,
            receiver_call=receiver_call,
        )
    return _pull(
        cfg,
        peer_name=peer_name,
        peer=peer,
        project=project,
        session_id=session_id,
        engine=engine,
        root=root,
        config_path=config_path,
        receiver_call=receiver_call,
        incoming_package=incoming_package,
        destination_chat=destination_chat,
        destination_topic=destination_topic,
        destination_bot_id=destination_bot_id,
        source_bot_id=source_bot_id,
    )
