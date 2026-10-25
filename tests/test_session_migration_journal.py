"""Only fixture-scoped journals; no home-directory scans."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from untether.session_migration.journal import JournalError, ObservedJournal


def test_journal_snapshot_preserves_history_and_line_endings(tmp_path):
    writer = ObservedJournal(tmp_path / "journals")
    sid = str(uuid4())
    writer.append(sid, role="user", text="old\\path\r\nnext")
    writer.append(sid, role="assistant", text="hello")
    payload, digest = writer.snapshot(sid)
    assert json.loads(payload)["messages"] == [
        {"role": "user", "text": "old\\path\r\nnext"},
        {"role": "assistant", "text": "hello"},
    ]
    assert len(digest) == 64


def test_partial_write_fails_closed(tmp_path):
    writer = ObservedJournal(tmp_path / "journals")
    sid = str(uuid4())
    writer.append(sid, role="user", text="complete")
    with (writer.root / (sid + ".jsonl")).open("ab") as stream:
        stream.write(b'{"role":"user","text":"partial"}')
    with pytest.raises(JournalError, match="JOURNAL_INTERRUPTED"):
        writer.snapshot(sid)


def test_no_implicit_session_store_scan(tmp_path):
    writer = ObservedJournal(tmp_path / "journals")
    with pytest.raises(JournalError, match="JOURNAL_UNAVAILABLE"):
        writer.snapshot(str(uuid4()))


def test_invalid_session_and_privilege_data_refused(tmp_path):
    writer = ObservedJournal(tmp_path / "journals")
    with pytest.raises(JournalError, match="SESSION_ID_INVALID"):
        writer.append("../../home", role="user", text="test")
    with pytest.raises(JournalError, match="OBSERVATION_INVALID"):
        writer.append(str(uuid4()), role="tool", text="danger")
