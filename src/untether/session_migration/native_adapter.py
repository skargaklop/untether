"""Engine persistence adapters must prove exact restore; ACP alone is insufficient."""
from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from .native_archive import NativeArchive


class CapabilityUnavailable(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RestoreProof:
    logical_id: str
    native_id: str
    generation: int
    lineage: str
    record_hashes: dict[str, str]
    restarted: bool
    resumed: bool


class PersistenceAdapter(Protocol):
    engine: str

    def export_exact(self, session_id: str) -> NativeArchive: ...
    def import_exact(self, archive: NativeArchive) -> RestoreProof: ...


_ADAPTERS: dict[str, Callable[[], PersistenceAdapter]] = {}


def register_persistence_adapter(engine: str, factory: Callable[[], PersistenceAdapter]) -> None:
    if not engine or engine in _ADAPTERS:
        raise CapabilityUnavailable("DUPLICATE_OR_INVALID_ENGINE")
    _ADAPTERS[engine] = factory


def require_persistence_adapter(engine: str) -> PersistenceAdapter:
    factory = _ADAPTERS.get(engine)
    if factory is None:
        raise CapabilityUnavailable(
            "ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE: ACP session/load does not import state"
        )
    adapter = factory()
    if adapter.engine != engine:
        raise CapabilityUnavailable("ENGINE_ID_MISMATCH")
    return adapter


def assert_exact_restore(archive: NativeArchive, proof: RestoreProof) -> str:
    hashes = {
        name: hashlib.sha256(content).hexdigest()
        for name, content in archive.records.items()
    }
    if (proof.logical_id != archive.logical_id
        or proof.lineage != archive.lineage
        or proof.generation != archive.generation
        or proof.record_hashes != hashes
        or not proof.native_id
        or not proof.restarted
        or not proof.resumed):
        raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
    return proof.native_id
