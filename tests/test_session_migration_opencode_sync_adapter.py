"""Unit tests for the OpenCode sync persistence adapter (``opencode-sync-v1``).

Requirement under test: ``OpenCodeSyncAdapter`` exports a session as its
durable sync event stream via the documented serve API route
(``POST /sync/history`` filtered to the aggregate), imports it via
``POST /sync/replay`` and proves the restore by reading back identity and
exact event order — the verified fidelity route (native CLI export/import
silently drops durable events).

Unit tests inject ``http_runner`` so no server process is spawned; the
serve lifecycle itself (spawn/kill) is exercised only through refusal paths
(store validation) and the real-engine runtime test.
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.request
from hashlib import sha256
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest
from pydantic import ValidationError

from untether.session_migration.native_adapter import CapabilityUnavailable
from untether.session_migration.native_archive import NativeArchive
from untether.session_migration.opencode_sync_adapter import (
    _EVENTS_RECORD,
    OpenCodeSyncAdapter,
)
from untether.session_migration.settings import OpenCodeSyncProfile

_ENGINE = "opencode"


class _FakeHttp:
    """Scripted loopback API: records requests, returns queued responses."""

    def __init__(self, responses: list[tuple[int, object]]) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[str, str, dict | None]] = []

    def __call__(self, path: str, method: str, data: dict | None):
        self.requests.append((path, method, data))
        if not self.responses:
            raise AssertionError(f"unexpected request {method} {path}")
        return self.responses.pop(0)


def _events(sid: str, marks: list[str]) -> list[dict]:
    return [
        {
            "id": f"evt_{i}",
            "aggregateID": sid,
            "seq": i,
            "type": "session.created.1" if i == 0 else "session.next.prompt.admitted.1",
            "data": {"text": mark},
        }
        for i, mark in enumerate([""] + marks)
    ]


def _history_response(sid: str, marks: list[str]) -> tuple[int, list[dict]]:
    # serve /sync/history returns the snake_case raw form
    return (
        200,
        [
            {**event, "aggregate_id": event.pop("aggregateID")}
            for event in _events(sid, marks)
        ]
        + [  # unrelated aggregate must be filtered out
            {
                "id": "evt_other",
                "aggregate_id": "ses_other",
                "seq": 0,
                "type": "session.created.1",
                "data": {},
            }
        ],
    )


def _profile(tmp_path: Path) -> OpenCodeSyncProfile:
    root = tmp_path / "data-root"
    root.mkdir(parents=True)
    return OpenCodeSyncProfile(
        format="opencode-sync-v1",
        executable="C:/fake/opencode.exe",
        data_root=str(root),
        port=18791,
    )


def _adapter(profile: OpenCodeSyncProfile, http: _FakeHttp) -> OpenCodeSyncAdapter:
    return OpenCodeSyncAdapter(
        engine=_ENGINE, profile=profile, http_runner=http, serve_starter=lambda: None
    )


def test_store_validation_refuses_missing_root(tmp_path: Path) -> None:
    profile = OpenCodeSyncProfile(
        format="opencode-sync-v1",
        executable="C:/fake/opencode.exe",
        data_root=str(tmp_path / "absent"),
        port=18791,
    )
    with pytest.raises(CapabilityUnavailable, match="PERSISTENCE_STORE_NOT_READY"):
        OpenCodeSyncAdapter(engine=_ENGINE, profile=profile)


def test_store_validation_refuses_wrong_root_name(tmp_path: Path) -> None:
    # The data root is the disposable environment root, not the store itself;
    # any real directory is acceptable, so build one and pass a symlinked path.
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(real)
    except OSError:
        pytest.skip("symlink privileges unavailable")
    profile = OpenCodeSyncProfile(
        format="opencode-sync-v1",
        executable="C:/fake/opencode.exe",
        data_root=str(link),
        port=18791,
    )
    with pytest.raises(CapabilityUnavailable):
        OpenCodeSyncAdapter(engine=_ENGINE, profile=profile)


def test_export_collects_and_filters_events(tmp_path: Path, monkeypatch) -> None:
    sid = "ses_export1"
    http = _FakeHttp(
        [(200, {"id": sid}), _history_response(sid, ["hello durable input"])]
    )
    adapter = _adapter(_profile(tmp_path), http)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    archive = adapter.export_exact(sid)
    assert archive.native_id == sid
    assert set(archive.records) == {_EVENTS_RECORD}
    stored = json.loads(archive.records[_EVENTS_RECORD])
    assert len(stored) == 2  # unrelated aggregate filtered
    assert all(event["aggregateID"] == sid for event in stored)
    assert stored[1]["data"]["text"] == "hello durable input"
    # history request first, session lookup first
    # session lookup and history collect both inside the gated snapshot
    assert ("/session/" + sid, "GET", None) in http.requests
    assert ("/sync/history", "POST", {}) in http.requests


def test_import_replays_and_verifies(tmp_path: Path, monkeypatch) -> None:
    sid = "ses_import1"
    marks = ["admitted one", "admitted two"]
    http_a = _FakeHttp([(200, {"id": sid}), _history_response(sid, marks)])
    adapter_a = _adapter(_profile(tmp_path / "src"), http_a)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    archive = adapter_a.export_exact(sid)
    (tmp_path / "dst").mkdir()

    # B: replay accepted, identity echo, then verification reads
    http_b = _FakeHttp(
        [
            (200, {"sessionID": sid}),  # /sync/replay
            (200, {"id": sid}),  # /session/{id}
            _history_response(sid, marks),  # /sync/history
            (200, {"id": sid}),  # independent restart identity
            _history_response(sid, marks),  # independent restart full payload
        ]
    )
    adapter_b = _adapter(
        OpenCodeSyncProfile(
            format="opencode-sync-v1",
            executable="C:/fake/opencode.exe",
            data_root=str(tmp_path / "dst"),
            port=18791,
        ),
        http_b,
    )
    proof = adapter_b.import_exact(archive)
    assert proof.native_id == sid
    assert proof.restarted and proof.resumed
    assert (
        proof.record_hashes[_EVENTS_RECORD]
        == sha256(archive.records[_EVENTS_RECORD]).hexdigest()
    )
    replay_request = http_b.requests[0]
    assert replay_request[0] == "/sync/replay"
    sent = replay_request[2]
    assert sent is not None and len(sent["events"]) == len(marks) + 1
    assert all(event["aggregateID"] == sid for event in sent["events"])


def test_import_refuses_wrong_identity_echo(tmp_path: Path, monkeypatch) -> None:
    sid = "ses_wrong"
    http_a = _FakeHttp([(200, {"id": sid}), _history_response(sid, ["x"])])
    adapter_a = _adapter(_profile(tmp_path / "src"), http_a)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    archive = adapter_a.export_exact(sid)
    (tmp_path / "dst").mkdir()

    http_b = _FakeHttp([(200, {"sessionID": "ses_other"})])
    adapter_b = _adapter(
        OpenCodeSyncProfile(
            format="opencode-sync-v1",
            executable="C:/fake/opencode.exe",
            data_root=str(tmp_path / "dst"),
            port=18791,
        ),
        http_b,
    )
    with pytest.raises(CapabilityUnavailable, match="EXACT_RESTORE_UNVERIFIED"):
        adapter_b.import_exact(archive)


@pytest.mark.parametrize("tamper", ["order", "payload"])
@pytest.mark.parametrize("after_restart", [False, True])
def test_import_refuses_divergent_replay(
    tmp_path: Path, monkeypatch, tamper: str, after_restart: bool
) -> None:
    sid = "ses_order"
    marks = ["one", "two"]
    http_a = _FakeHttp([(200, {"id": sid}), _history_response(sid, marks)])
    adapter_a = _adapter(_profile(tmp_path / "src"), http_a)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    archive = adapter_a.export_exact(sid)
    (tmp_path / "dst").mkdir()

    # Preserve event identities when changing payload data: checking IDs
    # alone must not certify a corrupted original message/tool record.
    reordered = list(_history_response(sid, marks)[1])
    if tamper == "order":
        reordered.reverse()
    else:
        reordered[1]["data"] = {"text": "corrupted original content"}
    responses = [(200, {"sessionID": sid})]
    if after_restart:
        responses.extend([(200, {"id": sid}), _history_response(sid, marks)])
    responses.extend([(200, {"id": sid}), (200, reordered)])
    http_b = _FakeHttp(responses)
    adapter_b = _adapter(
        OpenCodeSyncProfile(
            format="opencode-sync-v1",
            executable="C:/fake/opencode.exe",
            data_root=str(tmp_path / "dst"),
            port=18791,
        ),
        http_b,
    )
    with pytest.raises(CapabilityUnavailable, match="EXACT_RESTORE_UNVERIFIED"):
        adapter_b.import_exact(archive)


def test_import_refuses_foreign_engine_archive(tmp_path: Path, monkeypatch) -> None:
    sid = "ses_foreign"
    http_a = _FakeHttp([(200, {"id": sid}), _history_response(sid, ["x"])])
    adapter_a = _adapter(_profile(tmp_path / "src"), http_a)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    archive = adapter_a.export_exact(sid)
    (tmp_path / "dst").mkdir()
    foreign = NativeArchive(
        engine="other-engine",
        logical_id=archive.logical_id,
        native_id=archive.native_id,
        generation=archive.generation,
        lineage=archive.lineage,
        records=archive.records,
        digest=archive.digest,
    )
    adapter_b = _adapter(
        OpenCodeSyncProfile(
            format="opencode-sync-v1",
            executable="C:/fake/opencode.exe",
            data_root=str(tmp_path / "dst"),
            port=18791,
        ),
        _FakeHttp([]),
    )
    with pytest.raises(CapabilityUnavailable, match="ENGINE_MISMATCH"):
        adapter_b.import_exact(foreign)


def test_import_refuses_foreign_aggregate_events(tmp_path: Path, monkeypatch) -> None:
    sid = "ses_agg"
    # archive contains events of a different aggregate
    data = json.dumps(_events("ses_someone_else", ["x"])).encode()
    archive = NativeArchive(
        engine=_ENGINE,
        logical_id=sid,
        native_id=sid,
        generation=0,
        lineage="opencode-sync-v1:" + _EVENTS_RECORD,
        records={_EVENTS_RECORD: data},
        digest="",
    )
    adapter = _adapter(_profile(tmp_path), _FakeHttp([]))
    with pytest.raises(CapabilityUnavailable, match="ARCHIVE_IDENTITY_INVALID"):
        adapter.import_exact(archive)


def test_reclaim_overwrite_replays_after_prior_match(
    tmp_path: Path, monkeypatch
) -> None:
    sid = "ses_reclaim"
    old_marks = ["old one"]
    new_marks = ["old one", "new event"]
    http_a = _FakeHttp([(200, {"id": sid}), _history_response(sid, old_marks)])
    adapter_a = _adapter(_profile(tmp_path / "src"), http_a)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    old_archive = adapter_a.export_exact(sid)
    http_a2 = _FakeHttp([(200, {"id": sid}), _history_response(sid, new_marks)])
    adapter_a2 = _adapter(_profile(tmp_path / "src2"), http_a2)
    new_archive = adapter_a2.export_exact(sid)
    (tmp_path / "dst").mkdir()

    http_b = _FakeHttp(
        [
            _history_response(sid, old_marks),  # current stream check
            (200, {"sessionID": sid}),  # replay
            (200, {"id": sid}),  # verify
            _history_response(sid, new_marks),
        ]
    )
    adapter_b = _adapter(
        OpenCodeSyncProfile(
            format="opencode-sync-v1",
            executable="C:/fake/opencode.exe",
            data_root=str(tmp_path / "dst"),
            port=18791,
        ),
        http_b,
    )
    hashes = adapter_b.import_overwrite(
        engine=_ENGINE,
        native_id=sid,
        records=new_archive.records,
        expected_prior_records=old_archive.records,
        generation=new_archive.generation,
        lineage=new_archive.lineage,
    )
    assert (
        hashes[_EVENTS_RECORD]
        == sha256(new_archive.records[_EVENTS_RECORD]).hexdigest()
    )


def test_reclaim_refuses_divergent_prior(tmp_path: Path, monkeypatch) -> None:
    sid = "ses_prior"
    http_a = _FakeHttp([(200, {"id": sid}), _history_response(sid, ["old one"])])
    adapter_a = _adapter(_profile(tmp_path / "src"), http_a)
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))
    old_archive = adapter_a.export_exact(sid)
    http_div = _FakeHttp(
        [(200, {"id": sid}), _history_response(sid, ["unrelated drift"])]
    )
    adapter_div = _adapter(_profile(tmp_path / "src2"), http_div)
    div_archive = adapter_div.export_exact(sid)
    (tmp_path / "dst").mkdir()

    # current stream differs from both prior and new
    http_b = _FakeHttp([_history_response(sid, ["totally different"])])
    adapter_b = _adapter(
        OpenCodeSyncProfile(
            format="opencode-sync-v1",
            executable="C:/fake/opencode.exe",
            data_root=str(tmp_path / "dst"),
            port=18791,
        ),
        http_b,
    )
    with pytest.raises(CapabilityUnavailable, match="OVERWRITE_PRIOR_MISMATCH"):
        adapter_b.import_overwrite(
            engine=_ENGINE,
            native_id=sid,
            records=div_archive.records,
            expected_prior_records=old_archive.records,
            generation=div_archive.generation,
            lineage=div_archive.lineage,
        )


@pytest.mark.parametrize(
    "directory",
    [None, "", 42, [], {}, "relative/project", "/project/../allowed", "/allowed\x00"],
)
def test_project_lookup_refuses_missing_or_malformed_binding(tmp_path, directory):
    sid = "ses_binding"
    info = {"id": sid}
    if directory is not None:
        info["directory"] = directory
    http = _FakeHttp([(200, info)])
    adapter = _adapter(_profile(tmp_path), http)
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        adapter.lookup_for_project(sid, PurePosixPath("/allowed"))
    assert info == (
        {"id": sid} if directory is None else {"id": sid, "directory": directory}
    )


@pytest.mark.parametrize("project", ["/allowed", "/allowed/child", "/foreign"])
def test_project_lookup_requires_exact_directory(tmp_path, project):
    sid = "ses_binding"
    info = {"id": sid, "directory": "/allowed"}
    http = _FakeHttp([(200, info)])
    adapter = _adapter(_profile(tmp_path), http)
    if project == "/allowed":
        assert adapter.lookup_for_project(sid, PurePosixPath(project)) == sid
    else:
        with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
            adapter.lookup_for_project(sid, PurePosixPath(project))
    assert info == {"id": sid, "directory": "/allowed"}
    assert http.requests == [(f"/session/{sid}", "GET", None)]


def test_project_lookup_preserves_windows_native_path(tmp_path):
    sid = "ses_windows"
    directory = "D:\\Projects\\Exact"
    info = {"id": sid, "directory": directory}
    adapter = _adapter(_profile(tmp_path), _FakeHttp([(200, info)]))
    assert adapter.lookup_for_project(sid, PureWindowsPath("d:/Projects/Exact")) == sid
    assert info["directory"] == directory


def test_project_lookup_refuses_unknown_session(tmp_path):
    adapter = _adapter(_profile(tmp_path), _FakeHttp([(404, None)]))
    with pytest.raises(CapabilityUnavailable, match="SESSION_PROJECT_REFUSED"):
        adapter.lookup_for_project("ses_missing", PurePosixPath("/allowed"))


def test_lookup_refuses_unknown_session(tmp_path: Path) -> None:
    adapter = _adapter(_profile(tmp_path), _FakeHttp([(404, None)]))
    with pytest.raises(CapabilityUnavailable, match="SESSION_NOT_FOUND"):
        adapter.lookup_exact("ses_missing")


@pytest.mark.parametrize("status", [200, 400])
@pytest.mark.parametrize("oversized", [False, True])
def test_http_response_size_bound(tmp_path, monkeypatch, status, oversized):
    limit = 16
    raw = b'{"ok":true}'.ljust(limit, b" ")
    if oversized:
        raw += b" " * 100
    reads = []

    class Response(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            assert size == limit + 1
            return super().read(size)

    response = Response(raw)

    class Opener:
        def open(self, request, *, timeout):
            assert timeout == 30
            if status != 200:
                raise urllib.error.HTTPError(
                    request.full_url, status, "Bad Request", {}, response
                )
            response.status = status
            return response

    monkeypatch.setattr(urllib.request, "build_opener", lambda *args: Opener())
    profile = OpenCodeSyncProfile.model_validate(
        {**_profile(tmp_path).model_dump(), "max_response_bytes": limit}
    )
    adapter = OpenCodeSyncAdapter(engine=_ENGINE, profile=profile)
    if oversized:
        with pytest.raises(CapabilityUnavailable, match="ARCHIVE_TOO_LARGE"):
            adapter._request("/sync/history", "POST", {})
    else:
        assert adapter._request("/sync/history", "POST", {}) == (status, {"ok": True})
    assert reads == [limit + 1]
    assert response.closed


@pytest.mark.parametrize("limit", [0, -1])
def test_response_size_limit_must_be_positive(tmp_path, limit):
    with pytest.raises(ValidationError, match="max_response_bytes"):
        OpenCodeSyncProfile.model_validate(
            {**_profile(tmp_path).model_dump(), "max_response_bytes": limit}
        )
