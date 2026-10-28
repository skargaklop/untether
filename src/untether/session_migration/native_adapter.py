"""Engine persistence adapters must prove exact restore; ACP alone is insufficient."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import entry_points
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
    @property
    def engine(self) -> str: ...

    def export_exact(self, session_id: str) -> NativeArchive: ...
    def import_exact(self, archive: NativeArchive) -> RestoreProof: ...


_ADAPTERS: dict[str, Callable[[], PersistenceAdapter]] = {}


def register_persistence_adapter(
    engine: str, factory: Callable[[], PersistenceAdapter]
) -> None:
    if not engine or engine in _ADAPTERS:
        raise CapabilityUnavailable("DUPLICATE_OR_INVALID_ENGINE")
    _ADAPTERS[engine] = factory


def registered_adapter_engines() -> tuple[str, ...]:
    """In-process registry contents; entry-point plugins are discovered lazily."""
    installed = set(_ADAPTERS)
    installed.update(
        ep.name for ep in entry_points(group="untether.session_persistence")
    )
    return tuple(sorted(installed))


def unregister_persistence_adapter(engine: str) -> None:
    """Remove an in-process registration; used only by tests and process exit."""
    _ADAPTERS.pop(engine, None)


def require_persistence_adapter(engine: str) -> PersistenceAdapter:
    factory = _ADAPTERS.get(engine)
    if factory is None:
        # Installed metadata, not source-tree guesses, is authoritative.
        matches = [
            ep
            for ep in entry_points(group="untether.session_persistence")
            if ep.name == engine
        ]
        if len(matches) == 1:
            try:
                factory = matches[0].load()
            except Exception as exc:
                raise CapabilityUnavailable(
                    "ENGINE_ADAPTER_PLUGIN_LOAD_FAILED"
                ) from exc
    if factory is None:
        raise CapabilityUnavailable(
            "ENGINE_NATIVE_PERSISTENCE_UNAVAILABLE: ACP session/load does not import state"
        )
    adapter = factory()
    if adapter.engine != engine:
        raise CapabilityUnavailable("ENGINE_ID_MISMATCH")
    return adapter


def assert_restored(
    adapter: PersistenceAdapter, archive: NativeArchive, stored: NativeArchive
) -> None:
    """Prove the stored store restores ``archive`` exactly.

    Identity (engine/logical/native/lineage/generation) is always exact, and
    record bytes are exact by default. An adapter whose engine appends on
    resume may expose a read-only ``verify_restored_store(archive, stored) ->
    bool`` hook; the hook still requires identical record keys and
    byte-identical non-journal records and may only tolerate a bounded,
    validated journal appendix. Identity is never relaxed.
    """
    if (
        stored.engine != archive.engine
        or stored.logical_id != archive.logical_id
        or stored.native_id != archive.native_id
        or stored.lineage != archive.lineage
        or stored.generation != archive.generation
    ):
        raise CapabilityUnavailable("RESTORE_IDENTITY_MISMATCH")
    verifier = getattr(adapter, "verify_restored_store", None)
    if callable(verifier):
        if verifier(archive, stored):
            return
        raise CapabilityUnavailable("RESTORE_IDENTITY_MISMATCH")
    if stored.records != archive.records:
        raise CapabilityUnavailable("RESTORE_IDENTITY_MISMATCH")


def assert_exact_restore(archive: NativeArchive, proof: RestoreProof) -> str:
    hashes = {
        name: hashlib.sha256(content).hexdigest()
        for name, content in archive.records.items()
    }
    if (
        proof.logical_id != archive.logical_id
        or proof.lineage != archive.lineage
        or proof.generation != archive.generation
        or proof.record_hashes != hashes
        or not proof.native_id
        or not proof.restarted
        or not proof.resumed
    ):
        raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
    return proof.native_id
