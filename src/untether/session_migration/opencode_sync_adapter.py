"""Concrete persistence adapter for OpenCode sync stores (``opencode-sync-v1``).

Verified contract (replay probe 2026-10-27, ``D:/Temp/untether-opencode-replay-probe/``,
disposable A/B stores, engine 1.17.13): the native CLI ``export``/``import``
silently drops durable events (export produced ``messages: []``, B history
empty), so the fidelity route is the documented serve API:

- export: ``POST /sync/history {}`` returns the full raw event stream
  (including the ``session.created.1`` bootstrap) for every aggregate;
- import: ``POST /sync/replay {directory, events}`` validates and replays a
  complete event history and returns the preserved ``sessionID``;
- identity is preserved without remap; projected messages, tool execution
  (``message.updated``/``message.part.updated`` pairs) and unrelated sessions
  survive; events stay exact across a server restart.

Response events use ``aggregate_id`` (snake_case); the replay request requires
``aggregateID`` (camelCase) — the adapter renames on the wire. The engine runs
against a disposable per-endpoint ``data_root`` (HOME/USERPROFILE/XDG_*/APPDATA
tree) with provider keys stripped and plugins disabled; sessions of unrelated
aggregates are filtered out of the export so only this session's events are
moved and replayed.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path

from .native_adapter import (
    CapabilityUnavailable,
    RestoreProof,
    register_persistence_adapter,
    registered_adapter_engines,
)
from .native_archive import NativeArchive
from .settings import OpenCodeSyncProfile, SessionMigrationSettings

_LINEAGE_PREFIX = "opencode-sync-v1:"
_EVENTS_RECORD = "sync-events.json"
_EVENT_IDS_MAX = 100000
_HTTP_OK = 200
_SERVE_READY_TIMEOUT_S = 30.0


def _event_record(events: list[dict]) -> bytes:
    """Canonical bounded JSON for the event list."""
    try:
        data = json.dumps(events, separators=(",", ":"), sort_keys=True).encode()
    except (TypeError, ValueError) as exc:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED") from exc
    if not data or len(data) > 64 * 1024 * 1024:
        raise CapabilityUnavailable("ARCHIVE_TOO_LARGE")
    return data


def _events_from_record(data: bytes) -> list[dict]:
    try:
        events = json.loads(data)
    except (ValueError, UnicodeError) as exc:
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED") from exc
    if (
        not isinstance(events, list)
        or not events
        or len(events) > _EVENT_IDS_MAX
        or not all(isinstance(event, dict) for event in events)
    ):
        raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
    return events


def _replay_payload(session_id: str, events: list[dict]) -> list[dict]:
    """history-response events -> replay-request events, filtered to this
    session's aggregate, with the documented camelCase rename."""
    payload = []
    for event in events:
        if event.get("aggregate_id") != session_id:
            continue
        for key in ("id", "seq", "type", "data"):
            if not isinstance(event.get(key), (str, int)) and key != "data":
                raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        if not isinstance(event.get("data"), dict):
            raise CapabilityUnavailable("SESSION_RECORD_MALFORMED")
        payload.append(
            {
                "id": event["id"],
                "aggregateID": event["aggregate_id"],
                "seq": event["seq"],
                "type": event["type"],
                "data": event["data"],
            }
        )
    if not payload:
        raise CapabilityUnavailable("SESSION_NOT_FOUND")
    return payload


@dataclass(frozen=True, slots=True)
class OpenCodeSyncAdapter:
    """OpenCode persistence: sync/history collect -> replay -> verify."""

    engine: str
    profile: OpenCodeSyncProfile
    data_root: Path = field(init=False)
    http_runner: Callable[[str, str, dict | None], tuple[int, object]] | None = None
    serve_starter: Callable[[], object] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_root", Path(self.profile.data_root))
        _validate_opencode_store(self.data_root)

    # -- HTTP transport ----------------------------------------------------

    def _request(
        self, path: str, method: str = "GET", data: dict | None = None
    ) -> tuple[int, object]:
        if self.http_runner is not None:
            return self.http_runner(path, method, data)
        return _http_json(
            hostname=self.profile.hostname,
            port=self.profile.port,
            path=path,
            method=method,
            data=data,
            timeout_s=self.profile.request_timeout_s,
        )

    # -- helpers ---------------------------------------------------------

    def _collect_events(self, session_id: str) -> list[dict]:
        """Full raw event stream, filtered to this session's aggregate."""
        code, body = self._request("/sync/history", "POST", {})
        if code != _HTTP_OK or not isinstance(body, list):
            raise CapabilityUnavailable("PERSISTENCE_ADAPTER_UNAVAILABLE")
        return _replay_payload(session_id, body)

    def _serve(self, label: str) -> object:
        """Start the engine's serve subprocess (or the injected stand-in)
        against the disposable data root; bounded readiness wait on /doc."""
        if self.serve_starter is not None:
            return self.serve_starter()
        env = _isolated_env(self.data_root)
        argv = [
            self.profile.executable,
            "serve",
            "--hostname",
            self.profile.hostname,
            "--port",
            str(self.profile.port),
        ]
        log_path = self.data_root / ".untether-serve.log"
        try:
            stream = log_path.open("ab")
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=stream,
                stderr=stream,
                env=env,
                shell=False,  # nosec B603 operator-declared executable
            )
        except OSError as exc:
            raise CapabilityUnavailable("RESTART_PROBE_FAILED") from exc
        deadline = time.monotonic() + self.profile.serve_start_timeout_s
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        while time.monotonic() < deadline:
            try:
                request = urllib.request.Request(
                    f"http://{self.profile.hostname}:{self.profile.port}/doc"
                )
                with opener.open(request, timeout=1.0) as res:
                    if res.status == _HTTP_OK:
                        return proc
            except (OSError, ValueError):
                time.sleep(0.2)
        _kill_proc_tree(proc)
        raise CapabilityUnavailable("RESTART_PROBE_FAILED")

    def _lookup_session(self, session_id: str) -> None:
        """Prove the session exists by listing sessions over the live API."""
        code, body = self._request(f"/session/{session_id}")
        if (
            code != _HTTP_OK
            or not isinstance(body, dict)
            or body.get("id") != session_id
        ):
            raise CapabilityUnavailable("SESSION_NOT_FOUND")

    def _verify_replay(self, session_id: str, expected: list[dict]) -> None:
        """Independent verification: read back the replayed history and
        projected messages, exact order and identity."""
        code, body = self._request(f"/session/{session_id}")
        if (
            code != _HTTP_OK
            or not isinstance(body, dict)
            or body.get("id") != session_id
        ):
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        code, events = self._request("/sync/history", "POST", {})
        if code != _HTTP_OK or not isinstance(events, list):
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
        actual = _replay_payload(session_id, events)
        expected_sig = [(e["id"], e["seq"], e["type"]) for e in expected]
        actual_sig = [(e["id"], e["seq"], e["type"]) for e in actual]
        if actual_sig != expected_sig:
            raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")

    # -- PersistenceAdapter protocol --------------------------------------

    @contextmanager
    def export_snapshot(self, session_id: str) -> Iterator[NativeArchive]:
        gate_root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not gate_root:
            raise CapabilityUnavailable("SOURCE_GATE_UNCONFIGURED")
        from .session_gate import session_gate

        with session_gate(
            root=Path(gate_root),
            engine_id=self.engine,
            native_id=session_id,
        ):
            proc = None
            try:
                proc = self._serve("export")
                yield self._export_snapshot(session_id)
            finally:
                if proc is not None:
                    _maybe_kill(proc)

    def export_exact(self, session_id: str) -> NativeArchive:
        with self.export_snapshot(session_id) as archive:
            return archive

    def export_locked(self, session_id: str) -> NativeArchive:
        """Read only under a live migration lock owned by the caller context."""
        from .session_gate import assert_migration_lock_owned

        root = os.environ.get("UNTETHER_SESSION_GATE_ROOT")
        if not root:
            raise CapabilityUnavailable("SOURCE_GATE_UNCONFIGURED")
        assert_migration_lock_owned(
            root=Path(root), engine_id=self.engine, native_id=session_id
        )
        proc = None
        try:
            proc = self._serve("export")
            return self._export_snapshot(session_id)
        finally:
            if proc is not None:
                _maybe_kill(proc)

    def lookup_exact(self, session_id: str) -> str:
        proc = None
        try:
            proc = self._serve("lookup")
            self._lookup_session(session_id)
            return session_id
        finally:
            if proc is not None:
                _maybe_kill(proc)

    def lookup_for_project(self, session_id: str, project_root: Path) -> str:
        """OpenCode sessions carry a directory_id, not a cwd; project scoping
        for OpenCode stays with the coordinator's enrollment (explicit
        destination enrollment), so only identity is proven here."""
        proc = None
        try:
            proc = self._serve("lookup")
            self._lookup_session(session_id)
            return session_id
        finally:
            if proc is not None:
                _maybe_kill(proc)

    def _export_snapshot(self, session_id: str) -> NativeArchive:
        self._lookup_session(session_id)
        events = self._collect_events(session_id)
        data = _event_record(events)
        return NativeArchive(
            engine=self.engine,
            logical_id=session_id,
            native_id=session_id,
            generation=0,
            lineage=_LINEAGE_PREFIX + _EVENTS_RECORD,
            records={_EVENTS_RECORD: data},
            digest=sha256(data).hexdigest(),
        )

    def _check_archive_identity(
        self, archive: NativeArchive
    ) -> tuple[list[dict], bytes]:
        if archive.engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        if archive.logical_id != archive.native_id or not archive.records:
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        if set(archive.records) != {_EVENTS_RECORD}:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        if not archive.lineage.startswith(_LINEAGE_PREFIX):
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        events = _events_from_record(archive.records[_EVENTS_RECORD])
        # The record stores the canonical camelCase replay form; accept
        # either key form but require every PRESENT key to name this archive's
        # native id (foreign aggregates refuse).
        aggregate_ids = {
            event["aggregate_id"] for event in events if "aggregate_id" in event
        }
        payload_aggregate = {
            event["aggregateID"] for event in events if "aggregateID" in event
        }
        if not aggregate_ids and not payload_aggregate:
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        if aggregate_ids - {archive.native_id} or payload_aggregate - {
            archive.native_id
        }:
            raise CapabilityUnavailable("ARCHIVE_IDENTITY_INVALID")
        return events, archive.records[_EVENTS_RECORD]

    def import_exact(self, archive: NativeArchive) -> RestoreProof:
        events, data = self._check_archive_identity(archive)
        payload = [
            {
                "id": event["id"],
                "aggregateID": archive.native_id,
                "seq": event["seq"],
                "type": event["type"],
                "data": event["data"],
            }
            for event in events
        ]
        proc = None
        try:
            proc = self._serve("import")
            code, body = self._request(
                "/sync/replay",
                "POST",
                {"directory": str(self.data_root), "events": payload},
            )
            if code != _HTTP_OK or not isinstance(body, dict):
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
            if body.get("sessionID") != archive.native_id:
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
            self._verify_replay(archive.native_id, events)
        finally:
            if proc is not None:
                _maybe_kill(proc)
        return RestoreProof(
            logical_id=archive.logical_id,
            native_id=archive.native_id,
            generation=archive.generation,
            lineage=archive.lineage,
            record_hashes={_EVENTS_RECORD: sha256(data).hexdigest()},
            restarted=True,
            resumed=True,
        )

    # -- bounded reclaim support ------------------------------------------

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
        """Fenced replay overwrite: replay is idempotent on the event log, so
        reclaim replays the full new event list after proving the prior event
        stream matches the expected prior records."""
        if engine != self.engine:
            raise CapabilityUnavailable("ENGINE_MISMATCH")
        probe = NativeArchive(
            engine=engine,
            logical_id=native_id,
            native_id=native_id,
            generation=generation,
            lineage=lineage or _LINEAGE_PREFIX + _EVENTS_RECORD,
            records=records,
            digest="",
        )
        events, data = self._check_archive_identity(probe)
        prior_data = expected_prior_records.get(_EVENTS_RECORD)
        if prior_data is None:
            raise CapabilityUnavailable("RECORD_LAYOUT_REFUSED")
        proc = None
        try:
            proc = self._serve("reclaim")
            # Prior expectation: the current durable event stream for this
            # aggregate must equal the expected prior record (or the new
            # bytes — idempotent retry). _collect_events already returns the
            # same canonical camelCase payload form the archive stores.
            try:
                current = self._collect_events(native_id)
            except CapabilityUnavailable as exc:
                if str(exc) != "SESSION_NOT_FOUND":
                    raise
                current = []
            current_data = _event_record(current)
            if current_data not in (prior_data, data):
                raise CapabilityUnavailable("OVERWRITE_PRIOR_MISMATCH")
            payload = [
                {
                    "id": event["id"],
                    "aggregateID": native_id,
                    "seq": event["seq"],
                    "type": event["type"],
                    "data": event["data"],
                }
                for event in events
            ]
            code, body = self._request(
                "/sync/replay",
                "POST",
                {"directory": str(self.data_root), "events": payload},
            )
            if code != _HTTP_OK or not isinstance(body, dict):
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
            if body.get("sessionID") != native_id:
                raise CapabilityUnavailable("EXACT_RESTORE_UNVERIFIED")
            self._verify_replay(native_id, events)
        finally:
            if proc is not None:
                _maybe_kill(proc)
        return {_EVENTS_RECORD: sha256(data).hexdigest()}


def _isolated_env(data_root: Path) -> dict[str, str]:
    """Disposable environment per probe contract: strip provider keys, isolate
    HOME/USERPROFILE/XDG_*/APPDATA/LOCALAPPDATA/TMP under the data root,
    disable plugins/model fetch/autoupdate."""
    env: dict[str, str] = {}
    for key, value in os.environ.items():
        if key.startswith(
            (
                "OPENCODE_",
                "ANTHROPIC_",
                "OPENAI_",
                "GOOGLE_",
                "AWS_",
                "AZURE_",
                "GROQ_",
            )
        ):
            continue
        env[key] = value
    for key in (
        "HOME",
        "USERPROFILE",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
        "XDG_RUNTIME_DIR",
        "APPDATA",
        "LOCALAPPDATA",
        "TMP",
        "TEMP",
        "TMPDIR",
    ):
        target = data_root / key.lower()
        target.mkdir(parents=True, exist_ok=True)
        env[key] = str(target)
    env.update(
        OPENCODE_DISABLE_AUTOUPDATE="true",
        OPENCODE_DISABLE_MODELS_FETCH="true",
        OPENCODE_DISABLE_DEFAULT_PLUGINS="true",
        OPENCODE_CONFIG_CONTENT='{"provider":{},"plugin":[]}',
        PYTHONUTF8="1",
    )
    return env


def _maybe_kill(proc: object) -> None:
    """Kill and reap an owned serve handle if it is a real process."""
    if isinstance(proc, subprocess.Popen):
        _kill_proc_tree(proc)


def _kill_proc_tree(proc: subprocess.Popen) -> None:
    """Kill and reap the owned serve child (taskkill /T /F on Windows)."""
    if proc.poll() is None:
        if os.name == "nt":
            with suppress(subprocess.TimeoutExpired, OSError):
                subprocess.run(
                    ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                    capture_output=True,
                    timeout=5,
                )
        else:
            proc.kill()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:  # pragma: no cover - reap fallback
        proc.kill()
        proc.wait()


def _http_json(
    *,
    hostname: str,
    port: int,
    path: str,
    method: str,
    data: dict | None,
    timeout_s: float,
) -> tuple[int, object]:
    """Bounded loopback JSON request to the local serve API."""
    url = f"http://{hostname}:{port}{path}"
    payload = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(
        url,
        data=payload,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout_s) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as exc:
        try:
            body = json.load(exc)
        except (ValueError, UnicodeError):
            body = None
        return exc.code, body
    except (OSError, ValueError) as exc:
        raise CapabilityUnavailable("PERSISTENCE_ADAPTER_UNAVAILABLE") from exc


def _validate_opencode_store(data_root: Path) -> None:
    """Fail closed unless the declared root is a real, link-free directory
    prepared for this endpoint's disposable environment."""
    if not data_root.is_dir() or data_root.is_symlink():
        raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY")
    try:
        resolved = data_root.resolve(strict=True)
    except OSError as exc:
        raise CapabilityUnavailable("PERSISTENCE_STORE_NOT_READY") from exc
    if resolved != data_root:
        raise CapabilityUnavailable("PERSISTENCE_STORE_SYMLINK") from None


def register_from_settings(settings: SessionMigrationSettings, *, engine: str) -> None:
    """Route the declared ``opencode-sync-v1`` profile here; every other
    format keeps the file bundle adapter's routing untouched."""
    from .session_gate import configure_session_gate

    configure_session_gate(settings)
    profile = settings.persistence.engines.get(engine)
    if profile is None:
        return
    if not isinstance(profile, OpenCodeSyncProfile):
        from .file_bundle_adapter import register_from_settings as register_file_bundle

        register_file_bundle(settings, engine=engine)
        return
    if engine in registered_adapter_engines():
        return
    register_persistence_adapter(
        engine,
        lambda: OpenCodeSyncAdapter(engine=engine, profile=profile),
    )
