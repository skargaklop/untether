from __future__ import annotations

import json
from pathlib import Path

import pytest

from untether.session_scanners import ClaudeScanner, CodexScanner, PiFamilyScanner


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )


def test_scanner_cache_survives_new_instances(tmp_path: Path, monkeypatch) -> None:
    session = tmp_path / "sessions" / "chat.jsonl"
    _write_jsonl(
        session,
        [{"type": "session", "id": "sid-1", "timestamp": "2026-09-27T10:00:00Z"}],
    )
    first = PiFamilyScanner(engine="pi", agent_dir=tmp_path)
    assert first.scan()[0].session_id == "sid-1"
    second = PiFamilyScanner(engine="pi", agent_dir=tmp_path)
    monkeypatch.setattr(
        second, "_parse", lambda: (_ for _ in ()).throw(AssertionError("reparsed"))
    )

    assert second.scan()[0].session_id == "sid-1"


def test_pi_scanner_reads_nested_omp_and_keeps_last_user_prompt(tmp_path: Path) -> None:
    session = tmp_path / "sessions" / "--proj--" / "chat.jsonl"
    _write_jsonl(
        session,
        [
            {
                "type": "session",
                "id": "01a0e1ed",
                "cwd": str(tmp_path / "proj"),
                "title": "Fix resume",
                "timestamp": "2026-09-27T10:00:00Z",
            },
            {
                "type": "message",
                "timestamp": "2026-09-27T10:01:00Z",
                "message": {"role": "user", "content": "first ask"},
            },
            {
                "type": "message",
                "timestamp": "2026-09-27T10:02:00Z",
                "message": {"role": "assistant", "content": [{"text": "ok"}]},
            },
            {
                "type": "message",
                "timestamp": "2026-09-27T10:03:00Z",
                "message": {"role": "user", "content": "check the latest errors"},
            },
        ],
    )

    found = PiFamilyScanner(engine="omp", agent_dir=tmp_path).scan()

    assert len(found) == 1
    assert found[0].session_id == "01a0e1ed"
    assert found[0].resume_value == "01a0e1ed"
    assert found[0].title == "Fix resume"
    assert found[0].first_user_prompt == "first ask"
    assert found[0].last_user_prompt == "check the latest errors"
    assert found[0].engine == "omp"


def test_pi_scanner_skips_symlink_bad_id_and_malformed_line(tmp_path: Path) -> None:
    good = tmp_path / "sessions" / "good.jsonl"
    _write_jsonl(
        good,
        [
            {"type": "session", "id": "safe-id", "timestamp": "2026-09-27T10:00:00Z"},
            {"not": "json"},
            {"type": "message", "message": {"role": "user", "content": "kept"}},
        ],
    )
    bad = tmp_path / "sessions" / "bad.jsonl"
    _write_jsonl(bad, [{"type": "session", "id": "../etc/passwd"}])
    outside = tmp_path / "outside.jsonl"
    outside.write_text("{}\n", encoding="utf-8")
    link = tmp_path / "sessions" / "link.jsonl"
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported")

    found = PiFamilyScanner(engine="pi", agent_dir=tmp_path).scan()

    assert [item.session_id for item in found] == ["safe-id"]


def test_scanner_cache_reuses_fingerprint_until_size_changes(tmp_path: Path) -> None:
    session = tmp_path / "sessions" / "chat.jsonl"
    _write_jsonl(
        session,
        [
            {"type": "session", "id": "abc", "timestamp": "2026-09-27T10:00:00Z"},
            {"type": "message", "message": {"role": "user", "content": "one"}},
        ],
    )
    scanner = PiFamilyScanner(engine="pi", agent_dir=tmp_path)
    first = scanner.scan()
    second = scanner.scan()
    assert first[0].last_user_prompt == "one"
    assert second is first

    with session.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {"type": "message", "message": {"role": "user", "content": "two"}}
            )
            + "\n"
        )
    third = scanner.scan()
    assert third is not first
    assert third[0].last_user_prompt == "two"


def test_claude_scanner_skips_tool_results_and_uses_last_user(tmp_path: Path) -> None:
    path = tmp_path / "projects" / "proj" / "sess.jsonl"
    _write_jsonl(
        path,
        [
            {
                "type": "user",
                "sessionId": "claude-1",
                "cwd": str(tmp_path / "repo"),
                "timestamp": "2026-09-27T10:00:00Z",
                "message": {"role": "user", "content": "first claude ask"},
            },
            {
                "type": "user",
                "timestamp": "2026-09-27T10:01:00Z",
                "message": {
                    "role": "user",
                    "content": [{"type": "tool_result", "content": "noise"}],
                },
            },
            {
                "type": "custom-title",
                "customTitle": "Claude title",
            },
            {
                "type": "user",
                "timestamp": "2026-09-27T10:02:00Z",
                "message": {"role": "user", "content": "latest claude ask"},
            },
        ],
    )

    found = ClaudeScanner(root=tmp_path).scan()

    assert len(found) == 1
    assert found[0].title == "Claude title"
    assert found[0].last_user_prompt == "latest claude ask"
    assert found[0].engine == "claude"


def test_codex_scanner_reads_content_and_string_user_message(tmp_path: Path) -> None:
    path = tmp_path / "sessions" / "2026" / "09" / "27" / "rollout-real.jsonl"
    _write_jsonl(
        path,
        [
            {
                "timestamp": "2026-09-27T10:00:00Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "first codex ask"}],
                },
            },
            {
                "timestamp": "2026-09-27T10:05:00Z",
                "payload": {"type": "user_message", "message": "latest codex ask"},
            },
        ],
    )

    found = CodexScanner(root=tmp_path).scan()

    assert found[0].session_id == "rollout-real"
    assert found[0].first_user_prompt == "first codex ask"
    assert found[0].last_user_prompt == "latest codex ask"


def test_codex_scanner_reads_nested_rollout(tmp_path: Path) -> None:
    path = tmp_path / "sessions" / "2026" / "09" / "27" / "rollout-abc.jsonl"
    _write_jsonl(
        path,
        [
            {
                "timestamp": "2026-09-27T10:00:00Z",
                "payload": {
                    "type": "session_meta",
                    "id": "codex-1",
                    "cwd": str(tmp_path),
                },
            },
            {
                "timestamp": "2026-09-27T10:05:00Z",
                "payload": {"type": "event_msg", "text": "latest codex ask"},
            },
        ],
    )

    found = CodexScanner(root=tmp_path).scan()

    assert len(found) == 1
    assert found[0].session_id == "codex-1"
    assert found[0].resume_value == "codex-1"
    assert found[0].last_user_prompt == "latest codex ask"
    assert found[0].engine == "codex"
