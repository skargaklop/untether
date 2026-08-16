#!/usr/bin/env python3
"""Real ACP stdio fixture agent (newline-delimited JSON-RPC 2.0).

This is a TEST DOUBLE, not production code: it is a small, self-contained
(standard-library-only) subprocess that speaks enough of the Agent Client
Protocol over stdio to drive ``AcpPeer``-level tests deterministically. The
scenario selected with ``--scenario`` changes the wire behavior so each test
can exercise a specific transport contract.

Scenarios:

* ``v1-agent``         — negotiate as ACP v1, then stream v1-shaped
                         ``session/update`` notifications and complete the turn
                         with a ``stopReason`` in the prompt response.
* ``v2-agent``         — negotiate as ACP v2, stream v2 ``sessionUpdate``
                         variants, then complete with an empty prompt
                         acknowledgement followed by an idle ``state_update``.
* ``v2-nonstreaming``  — ACP v2 flow, but full ``agent_message`` upserts only
                         (no chunks) — proves the non-streaming answer path.
* ``permission-request`` — mid-turn reverse ``session/request_permission`` with
                         two options; the client's selected/cancelled outcome
                         drives the turn's final ``stopReason``.
* ``elicitation-form`` — mid-turn reverse ``elicitation/create`` with a small
                         JSON Schema; accept/decline/cancel all end the turn
                         cleanly.
* ``malformed-line``   — prints a non-JSON line, then behaves like v1-agent.
* ``oversize-frame``   — prints a >10 MiB line, then behaves like v1-agent.
* ``v1-batch``         — after negotiating as v1, answers with a 2-element
                         JSON-RPC batch (must be rejected by the peer).
* ``notify-flood``     — emits 3 notifications back-to-back on the first
                         request (overflows a queue_size=1 reader).
* ``print-pid``        — prints its own PID to stderr, then acts as v1-agent.

Run under the same interpreter the tests spawn (``sys.executable``); the
fixture needs no third-party imports and works under any supported Python.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import json
import os
import sys
from typing import Any

# Force UTF-8 and LF-only newlines on all stdio streams. On Windows the
# console codepage would otherwise corrupt the JSON even when PYTHONUTF8 is
# not exported, and CRLF translation would make the wire bytes surprising.
_STREAM_KW = {"encoding": "utf-8", "errors": "replace"}
_stdin: Any = sys.stdin
_stdout: Any = sys.stdout
_stderr: Any = sys.stderr
for _stream in (_stdin, _stderr):
    with contextlib.suppress(AttributeError, ValueError):
        _stream.reconfigure(**_STREAM_KW)
with contextlib.suppress(AttributeError, ValueError):
    _stdout.reconfigure(**_STREAM_KW, newline="\n")

Json = dict[str, Any]


def _read() -> Json | None:
    """Read one newline-delimited JSON-RPC message; None on EOF."""
    line = sys.stdin.readline()
    if line == "":
        return None
    line = line.strip()
    if not line:
        return _read()
    return json.loads(line)


def _write(message: Json | list[Json]) -> None:
    """Write one newline-delimited JSON frame and flush (critical on Windows)."""
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _ok(request_id: Any, result: Json) -> None:
    _write({"jsonrpc": "2.0", "id": request_id, "result": result})


def _error(request_id: Any, code: int, message: str) -> None:
    _write(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }
    )


def _notify(method: str, params: Json) -> None:
    _write({"jsonrpc": "2.0", "method": method, "params": params})


def _reverse(request_id: Any, method: str, params: Json) -> None:
    _write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})


def _read_response() -> Json:
    """Wait for and return the client's result/error for a reverse request."""
    msg = _read()
    assert msg is not None, "client closed stream while awaiting reverse response"
    if "error" in msg:
        return {"outcome": "cancelled"}
    return msg.get("result") or {}


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


# ---------------------------------------------------------------------------
# update builders — v1 uses ``type``, v2 uses ``sessionUpdate``; both use the
# canonical snake_case kind names the reducer/plan speak.
# ---------------------------------------------------------------------------


def _u1(kind: str, **fields: Any) -> Json:
    return {"type": kind, **fields}


def _u2(kind: str, **fields: Any) -> Json:
    return {"sessionUpdate": kind, **fields}


# ---------------------------------------------------------------------------
# shared flows
# ---------------------------------------------------------------------------


_V1_UPDATES: tuple[Json, ...] = (
    _u1("agent_message_chunk", messageId="m1", role="assistant", content="Hello from "),
    _u1(
        "agent_message_chunk",
        messageId="m1",
        role="assistant",
        content="the fixture agent.",
    ),
    _u1(
        "tool_call",
        toolCallId="t1",
        title="List workspace",
        name="list_files",
        status="running",
    ),
    _u1("tool_call_content_chunk", toolCallId="t1", content="snippet"),
    _u1(
        "available_commands_update",
        availableCommands=[{"name": "help"}, {"name": "status"}],
    ),
    _u1("terminal_update", terminalId="term1", status="created"),
    _u1(
        "terminal_output_chunk",
        terminalId="term1",
        encoding="base64",
        data=_b64("ok\n"),
    ),
)

_V2_UPDATES: tuple[Json, ...] = (
    _u2("agent_thought_chunk", messageId="t", content="reasoning about the task"),
    _u2("agent_message_chunk", messageId="m1", role="assistant", content="Hello v2 "),
    _u2("agent_message_chunk", messageId="m1", role="assistant", content="world."),
    _u2("user_message_chunk", messageId="u1", role="user", content="user note"),
    _u2("tool_call", toolCallId="t1", title="Run tests", name="run", status="running"),
    _u2("tool_call_content_chunk", toolCallId="t1", content="progress"),
    _u2("terminal_update", terminalId="term1", status="started"),
    _u2(
        "terminal_output_chunk",
        terminalId="term1",
        encoding="base64",
        data=_b64("out\n"),
    ),
    _u2("terminal_update", terminalId="term1", status="exited", exitStatus=0),
    _u2("session_info_update", metadata={"title": "v2 session"}),
    _u2("available_commands_update", availableCommands=[{"name": "message:mode"}]),
    _u2(
        "config_option_update",
        configOptions=[{"configId": "mode", "options": [{"value": "plan"}]}],
    ),
    _u2("mode_update", mode="plan"),
)

_V2_NONSTREAMING_UPDATES: tuple[Json, ...] = (
    _u2(
        "agent_message",
        messageId="m1",
        role="assistant",
        content="Full non-streaming answer",
    ),
    _u2(
        "tool_call",
        toolCallId="t1",
        title="Edit file",
        name="write_file",
        status="completed",
        content="done",
    ),
)


def _initialize_result(version: int) -> Json:
    if version == 2:
        return {
            "protocolVersion": 2,
            "info": {"name": "acp-fixture", "version": "1.0.0"},
            "capabilities": {"prompt": {}},
        }
    return {
        "protocolVersion": 1,
        "agentInfo": {"name": "acp-fixture", "version": "1.0.0"},
        "agentCapabilities": {"prompt": {}},
    }


def _stream_updates(updates: tuple[Json, ...]) -> None:
    for update in updates:
        _notify("session/update", update)


def _serve_session(version: int, updates: tuple[Json, ...]) -> int:
    """Serve initialize/session/new/session/prompt with the given update stream."""
    session_id = f"sess-{version}"
    while True:
        msg = _read()
        if msg is None:
            return 0
        rid = msg.get("id")
        if rid is None or not isinstance(msg.get("method"), str):
            continue
        method: str = msg["method"]
        if method == "initialize":
            _ok(rid, _initialize_result(version))
        elif method == "session/new":
            _ok(rid, {"sessionId": session_id})
        elif method == "session/prompt":
            _stream_updates(updates)
            if version == 2:
                _complete_v2(rid)
            else:
                _complete_v1(rid)
        else:
            _error(rid, -32601, f"Method not found: {method}")


def _complete_v1(request_id: Any, stop_reason: str = "end_turn") -> None:
    _ok(request_id, {"stopReason": stop_reason})


def _complete_v2(request_id: Any, stop_reason: str = "end_turn") -> None:
    # v2: the prompt response is an empty acknowledgement; completion is
    # reported through an idle state_update notification carrying the reason.
    _ok(request_id, {})
    _notify("session/update", _u2("state_update", state="idle", stopReason=stop_reason))


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------


def scenario_v1() -> int:
    return _serve_session(1, _V1_UPDATES)


def scenario_v2() -> int:
    return _serve_session(2, _V2_UPDATES)


def scenario_v2_nonstreaming() -> int:
    return _serve_session(2, _V2_NONSTREAMING_UPDATES)


def scenario_print_pid() -> int:
    sys.stderr.write(f"acp-pid {os.getpid()}\n")
    sys.stderr.flush()
    return scenario_v1()


def scenario_oversize_frame() -> int:
    # A single line larger than the 10 MiB peer frame limit.
    sys.stdout.write("x" * (11 * 1024 * 1024) + "\n")
    sys.stdout.flush()
    return scenario_v1()


def scenario_v1_batch() -> int:
    session_id = "sess-batch"
    while True:
        msg = _read()
        if msg is None:
            return 0
        rid = msg.get("id")
        if rid is None or not isinstance(msg.get("method"), str):
            continue
        method = msg["method"]
        if method == "initialize":
            _ok(rid, _initialize_result(1))
        elif method == "session/new":
            _ok(rid, {"sessionId": session_id})
        elif method == "session/cancel":
            continue
        else:
            # Negotiated as v1: answer with a 2-element JSON-RPC batch.
            batch: list[Json] = [
                {"jsonrpc": "2.0", "method": "session/update", "params": {"n": 1}},
                {"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "end_turn"}},
            ]
            _write(batch)  # a 2-element JSON-RPC batch is a legal frame


def scenario_notify_flood() -> int:
    msg = _read()
    if msg is None:
        return 0
    for _ in range(3):
        _notify(
            "session/update", _u2("agent_message_chunk", messageId="m", content="x")
        )
    if msg.get("id") is not None:
        _ok(msg["id"], {})
    while _read() is not None:
        pass
    return 0


def scenario_permission() -> int:
    session_id = "sess-perm"
    while True:
        msg = _read()
        if msg is None:
            return 0
        rid = msg.get("id")
        if rid is None or not isinstance(msg.get("method"), str):
            continue
        method = msg["method"]
        if method == "initialize":
            _ok(rid, _initialize_result(2))
        elif method == "session/new":
            _ok(rid, {"sessionId": session_id})
        elif method == "session/prompt":
            _notify(
                "session/update",
                _u2("state_update", state="running"),
            )
            _notify(
                "session/update",
                _u2(
                    "agent_message_chunk",
                    messageId="m1",
                    content="Requesting permission...",
                ),
            )
            _reverse(
                "perm-1",
                "session/request_permission",
                {
                    "sessionId": session_id,
                    "options": [
                        {"optionId": "allow", "name": "Allow", "kind": "allow_once"},
                        {"optionId": "reject", "name": "Reject", "kind": "reject_once"},
                    ],
                    "title": "Allow file access?",
                },
            )
            outcome = _read_response()
            stop_reason = (
                "end_turn" if outcome.get("outcome") == "selected" else "cancelled"
            )
            _complete_v2(rid, stop_reason)
        else:
            _error(rid, -32601, f"Method not found: {method}")


def scenario_malformed_line() -> int:
    # A non-JSON line first: the peer reader must fail with a malformed-JSON
    # error before the (valid, following) response is processed.
    sys.stdout.write("this is not json\n")
    sys.stdout.flush()
    return scenario_v1()


def scenario_elicitation() -> int:
    session_id = "sess-elicit"
    while True:
        msg = _read()
        if msg is None:
            return 0
        rid = msg.get("id")
        if rid is None or not isinstance(msg.get("method"), str):
            continue
        method = msg["method"]
        if method == "initialize":
            _ok(rid, _initialize_result(2))
        elif method == "session/new":
            _ok(rid, {"sessionId": session_id})
        elif method == "session/prompt":
            _notify(
                "session/update",
                _u2(
                    "agent_message_chunk",
                    messageId="m1",
                    content="Please fill the form.",
                ),
            )
            _reverse(
                "elicit-1",
                "elicitation/create",
                {
                    "sessionId": session_id,
                    "form": {
                        "schema": {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                            "required": ["name"],
                        }
                    },
                },
            )
            outcome = _read_response()
            action = outcome.get("outcome", "cancel")
            if action == "accept":
                _notify(
                    "session/update",
                    _u2("agent_message_chunk", messageId="m1", content="Thanks!"),
                )
            _complete_v2(rid, "end_turn")
        else:
            _error(rid, -32601, f"Method not found: {method}")


_SCENARIOS: dict[str, Any] = {
    "v1-agent": scenario_v1,
    "v2-agent": scenario_v2,
    "v2-nonstreaming": scenario_v2_nonstreaming,
    "permission-request": scenario_permission,
    "elicitation-form": scenario_elicitation,
    "malformed-line": scenario_malformed_line,
    "oversize-frame": scenario_oversize_frame,
    "v1-batch": scenario_v1_batch,
    "notify-flood": scenario_notify_flood,
    "print-pid": scenario_print_pid,
}


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=sorted(_SCENARIOS), default="v1-agent")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    return _SCENARIOS[args.scenario]()


if __name__ == "__main__":
    raise SystemExit(main())
