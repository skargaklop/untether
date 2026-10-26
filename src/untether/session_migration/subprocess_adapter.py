"""Optional native persistence contract bound to a named executable.

ACP session/load alone never qualifies as a native import.
"""
from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .native_adapter import CapabilityUnavailable, RestoreProof
from .native_archive import NativeArchive


@dataclass(frozen=True, slots=True)
class ExecutablePersistenceAdapter:
    engine: str
    executable: Path
    store: Path
    timeout: float = 30.0

    def _call(self, action: str, payload: dict) -> dict:
        if (not self.executable.is_file() or self.executable.is_symlink()
            or not self.store.is_dir() or self.store.is_symlink()):
            raise CapabilityUnavailable("PERSISTENCE_EXECUTABLE_NOT_READY")
        request = json.dumps({
            "version": 1, "engine": self.engine, "action": action, "payload": payload,
        }, separators=(",", ":")).encode()
        if len(request) > 67108864:
            raise CapabilityUnavailable("PERSISTENCE_REQUEST_TOO_LARGE")
        try:
            result = subprocess.run(
                [str(self.executable), "--native-session-protocol-v1",
                 "--store", str(self.store)],
                input=request, capture_output=True, timeout=self.timeout, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise CapabilityUnavailable("PERSISTENCE_ADAPTER_UNAVAILABLE") from exc
        if result.returncode or len(result.stdout) > 67108864:
            raise CapabilityUnavailable("PERSISTENCE_ADAPTER_REFUSED")
        try:
            value = json.loads(result.stdout)
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
            return NativeArchive(
                self.engine, data["logical_id"], data["native_id"],
                data["generation"], data["lineage"], records,
                hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest(),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise CapabilityUnavailable("PERSISTENCE_EXPORT_INVALID") from exc

    def import_exact(self, archive: NativeArchive) -> RestoreProof:
        if archive.engine != self.engine:
            raise CapabilityUnavailable("PERSISTENCE_ENGINE_MISMATCH")
        request = {
            "logical_id": archive.logical_id, "native_id": archive.native_id,
            "generation": archive.generation, "lineage": archive.lineage,
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
                result["logical_id"], result["native_id"], result["generation"],
                result["lineage"], result["record_hashes"],
                result["restarted"] is True, result["resumed"] is True,
            )
        except (KeyError, TypeError) as exc:
            raise CapabilityUnavailable("PERSISTENCE_PROOF_INVALID") from exc
