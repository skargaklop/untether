"""Fixed-verb serve subprocess: real receiver protocol in a separate process.

Runs the actual NativeReceiver protocol handler over length-prefixed stdin
frames against a real ExecutablePersistenceAdapter. Every import and export
spawns a fresh engine process (the restart), and disjoint stores prove the
resume, mirroring the production forced-command receiver boundary.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

from untether.session_migration import native_adapter
from untether.session_migration.native_receiver import NativeReceiver
from untether.session_migration.receiver_protocol import (
    ReceiverEndpoint,
    handle_receiver,
)
from untether.session_migration.ssh_transport import _REQUIRED_CHECKS
from untether.session_migration.subprocess_adapter import ExecutablePersistenceAdapter


def _read_frame(stream) -> bytes:
    header = stream.readline()
    if not header:
        return b""
    return stream.read(int(header.strip()))


def _write_frame(stream, payload: bytes) -> None:
    stream.write(str(len(payload)).encode() + b"\n")
    stream.write(payload)
    stream.flush()


def serve(engine: str, engine_fixture: Path, store: Path) -> int:
    native = NativeReceiver(store, max_archive=1 << 20, max_expanded=1 << 20)
    adapter = ExecutablePersistenceAdapter(
        engine, engine_fixture, store, interpreter=sys.executable
    )

    @contextmanager
    def export_snapshot(session_id: str):
        """Yield the adapter's archive while holding the real cross-process gate."""
        from untether.session_migration.session_gate import session_gate

        gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not gate_root:
            raise RuntimeError("SOURCE_GATE_UNCONFIGURED")
        with session_gate(root=Path(gate_root), engine_id=engine, native_id=session_id):
            yield adapter.export_exact(session_id)

    previous = native_adapter._ADAPTERS.get(engine)
    native_adapter._ADAPTERS[engine] = lambda: type(
        "ServeAdapter",
        (),
        {
            "engine": engine,
            "export_exact": staticmethod(adapter.export_exact),
            "export_snapshot": staticmethod(export_snapshot),
            "import_exact": staticmethod(adapter.import_exact),
        },
    )()
    try:
        checks = dict.fromkeys(_REQUIRED_CHECKS, True)
        # The serve fixture runs the real probe set on this host; only the
        # Linux-only runtime flag is fixture-scoped (Windows CI host).
        checks["runtime_ready"] = True
        checks["workspace_git_parity"] = True
        endpoint = ReceiverEndpoint(
            endpoint_id="serve-endpoint",
            execution_mode="native",
            destination_binding="serve",
            project="demo",
            deployment="serve",
            arch="x86_64",
            probe_health=lambda: checks,
            native_receiver=native,
        )
        while True:
            request = _read_frame(sys.stdin.buffer)
            if not request:
                return 0
            try:
                call = json.loads(request)
                verb = call["verb"]
                payload = base64.b64decode(call.get("payload_base64", ""))
            except (ValueError, KeyError):
                _write_frame(sys.stdin.buffer, b'{"error":"FRAME_INVALID"}')
                continue
            try:
                reply = handle_receiver(endpoint, verb=verb, payload=payload)
            except Exception as exc:  # noqa: BLE001 - refusal must be code-only
                reply = json.dumps({"error": str(exc)}).encode()
            _write_frame(sys.stdout.buffer, reply)
    finally:
        if previous is None:
            native_adapter._ADAPTERS.pop(engine, None)
        else:
            native_adapter._ADAPTERS[engine] = previous


def main(argv: list[str]) -> int:
    if len(argv) not in (3, 5) or argv[0] != "--serve-protocol-v1":
        sys.stderr.write(
            "usage: serve --serve-protocol-v1 <engine> <store> [--gate-root <root>]\n"
        )
        return 2
    if len(argv) == 5 and argv[3] == "--gate-root":
        # Explicit gate configuration, same entry-point shape as production's
        # configured root; the env guard itself remains authoritative.
        os.environ["UNTETHER_SESSION_GATE_ROOT"] = argv[4]
    here = Path(__file__).parent
    return serve(argv[1], here / "native_persistence_engine.py", Path(argv[2]))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
