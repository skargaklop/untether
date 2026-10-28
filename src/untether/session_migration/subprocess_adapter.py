"""Optional native persistence contract bound to a named executable.

ACP session/load alone never qualifies as a native import.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .native_adapter import CapabilityUnavailable, RestoreProof
from .native_archive import NativeArchive
from .remote import _bounded_run, _ReplyOverflow

__all__ = ["ExecutablePersistenceAdapter"]


@dataclass(frozen=True, slots=True)
class ExecutablePersistenceAdapter:
    engine: str
    executable: Path
    store: Path
    timeout: float = 30.0
    interpreter: str | None = None
    max_response_bytes: int = 67108864
    max_request_bytes: int = 67108864

    def __post_init__(self) -> None:
        if self.max_response_bytes <= 0 or self.max_request_bytes <= 0:
            raise CapabilityUnavailable("PERSISTENCE_LIMIT_INVALID")

    def _argv(self) -> list[str]:
        prefix = [self.interpreter] if self.interpreter is not None else []
        return [
            *prefix,
            str(self.executable),
            "--native-session-protocol-v1",
            "--store",
            str(self.store),
        ]

    def _call(self, action: str, payload: dict) -> dict:
        if (
            not self.executable.is_file()
            or self.executable.is_symlink()
            or not self.store.is_dir()
            or self.store.is_symlink()
        ):
            raise CapabilityUnavailable("PERSISTENCE_EXECUTABLE_NOT_READY")
        request = json.dumps(
            {
                "version": 1,
                "engine": self.engine,
                "action": action,
                "payload": payload,
            },
            separators=(",", ":"),
        ).encode()
        if len(request) > self.max_request_bytes:
            raise CapabilityUnavailable("PERSISTENCE_REQUEST_TOO_LARGE")
        try:
            code, stdout = _bounded_run(
                self._argv(),
                payload=request,
                timeout_s=self.timeout,
                stdout_cap=self.max_response_bytes,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise CapabilityUnavailable("PERSISTENCE_ADAPTER_UNAVAILABLE") from exc
        except _ReplyOverflow as exc:
            raise CapabilityUnavailable("PERSISTENCE_RESPONSE_TOO_LARGE") from exc
        if code or len(stdout) > self.max_response_bytes:
            if code == 3 and action == "import_and_restart_verify":
                # Engine-native refusal: the session already exists (O_EXCL on
                # the engine's own store). Distinct from a generic refusal so
                # the coordinator can route it into the reclaim path.
                raise CapabilityUnavailable("SESSION_ID_COLLISION")
            if code in (5, 6) and action == "overwrite_and_restart_verify":
                # Overwrite preconditions failed on the engine side.
                raise CapabilityUnavailable("OVERWRITE_PRIOR_MISMATCH")
            raise CapabilityUnavailable("PERSISTENCE_ADAPTER_REFUSED")
        try:
            value = json.loads(stdout)
        except (UnicodeError, ValueError) as exc:
            raise CapabilityUnavailable("PERSISTENCE_RESPONSE_INVALID") from exc
        if not isinstance(value, dict) or value.get("version") != 1:
            raise CapabilityUnavailable("PERSISTENCE_PROTOCOL_MISMATCH")
        return value

    def export_exact(self, session_id: str) -> NativeArchive:
        data = self._call("export", {"session_id": session_id})
        if data.get("engine") != self.engine:
            raise CapabilityUnavailable("PERSISTENCE_ENGINE_MISMATCH")
        try:
            records = {
                name: base64.b64decode(blob, validate=True)
                for name, blob in data["records"].items()
            }
            if not (
                isinstance(data.get("logical_id"), str)
                and data["logical_id"]
                and isinstance(data.get("native_id"), str)
                and data["native_id"]
                and isinstance(data.get("lineage"), str)
                and data["lineage"]
                and type(data.get("generation")) is int
                and data["generation"] >= 0
            ):
                raise CapabilityUnavailable("PERSISTENCE_EXPORT_IDENTITY_INVALID")
            return NativeArchive(
                self.engine,
                data["logical_id"],
                data["native_id"],
                data["generation"],
                data["lineage"],
                records,
                hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest(),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise CapabilityUnavailable("PERSISTENCE_EXPORT_INVALID") from exc

    @contextmanager
    def export_snapshot(self, session_id: str) -> Iterator[NativeArchive]:
        """Export while holding the cross-process session gate.

        ``export_exact`` stays gate-free for direct codec tests; the
        migration coordinator must use this held-snapshot path instead.
        """
        gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not gate_root:
            raise CapabilityUnavailable("SOURCE_GATE_UNCONFIGURED")
        from .session_gate import session_gate

        with session_gate(
            root=Path(gate_root),
            engine_id=self.engine,
            native_id=session_id,
        ):
            yield self.export_exact(session_id)

    def import_exact(self, archive: NativeArchive) -> RestoreProof:
        if archive.engine != self.engine:
            raise CapabilityUnavailable("PERSISTENCE_ENGINE_MISMATCH")
        request = {
            "logical_id": archive.logical_id,
            "native_id": archive.native_id,
            "generation": archive.generation,
            "lineage": archive.lineage,
            "records": {
                name: base64.b64encode(blob).decode()
                for name, blob in archive.records.items()
            },
        }
        result = self._call("import_and_restart_verify", request)
        if result.get("engine") != self.engine:
            raise CapabilityUnavailable("PERSISTENCE_ENGINE_MISMATCH")
        try:
            return RestoreProof(
                result["logical_id"],
                result["native_id"],
                result["generation"],
                result["lineage"],
                result["record_hashes"],
                result["restarted"] is True,
                result["resumed"] is True,
            )
        except (KeyError, TypeError) as exc:
            raise CapabilityUnavailable("PERSISTENCE_PROOF_INVALID") from exc

    def import_overwrite(
        self,
        *,
        engine: str,
        native_id: str,
        records: dict[str, bytes],
        expected_prior_records: dict[str, bytes],
        generation: int = 0,
        lineage: str = "",
    ) -> dict[str, str]:
        """Digest-pinned overwrite through the engine's own restart verify.

        The engine process refuses unless the stored prior content matches
        ``expected_prior_records`` byte-for-byte; a divergent store exits
        nonzero and this surfaces as OVERWRITE_PRIOR_MISMATCH, never a silent
        replace.
        """
        if engine != self.engine:
            raise CapabilityUnavailable("PERSISTENCE_ENGINE_MISMATCH")
        request = {
            "logical_id": native_id,
            "native_id": native_id,
            "generation": generation,
            "lineage": lineage or native_id,
            "records": {
                name: base64.b64encode(blob).decode() for name, blob in records.items()
            },
            "expected_prior_records": {
                name: base64.b64encode(blob).decode()
                for name, blob in expected_prior_records.items()
            },
        }
        result = self._call("overwrite_and_restart_verify", request)
        if (
            result.get("engine") != engine
            or result.get("logical_id") != native_id
            or result.get("native_id") != native_id
            or result.get("generation") != generation
            or result.get("lineage") != (lineage or native_id)
            or result.get("restarted") is not True
            or result.get("resumed") is not True
        ):
            raise CapabilityUnavailable("PERSISTENCE_PROOF_INVALID")
        hashes = result.get("record_hashes")
        expected = {
            name: hashlib.sha256(blob).hexdigest() for name, blob in records.items()
        }
        if not isinstance(hashes, dict) or hashes != expected:
            raise CapabilityUnavailable("PERSISTENCE_PROOF_INVALID")
        return expected
