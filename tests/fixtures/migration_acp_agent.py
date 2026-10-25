"""Persistent fake ACP agent for independent-process restart tests (no real provider)."""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
for line in sys.stdin:
    try:
        req = json.loads(line)
    except ValueError:
        break
    method = req.get("method")
    rid = req.get("id")
    params = req.get("params", {})
    if method == "initialize":
        value = {"protocolVersion": 2, "capabilities": {}}
    elif method == "session/new":
        sid = "fixture-" + uuid.uuid4().hex
        (root / (sid + ".json")).write_text(
            json.dumps({"id": sid, "cwd": params["cwd"], "messages": []}),
            encoding="utf-8",
        )
        value = {"sessionId": sid}
    elif method == "session/prompt":
        sid = params["sessionId"]
        path = root / (sid + ".json")
        state = json.loads(path.read_text(encoding="utf-8"))
        state["messages"].append(params["prompt"][0]["text"])
        path.write_text(json.dumps(state), encoding="utf-8")
        value = {"stopReason": "end_turn"}
    elif method == "session/resume":
        sid = params["sessionId"]
        path = root / (sid + ".json")
        if not path.is_file():
            sys.stdout.write(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": rid,
                        "error": {
                            "code": -32000,
                            "message": "no persisted session",
                        },
                    }
                )
                + "\n"
            )
            sys.stdout.flush()
            continue
        with (root / "resume.log").open("a", encoding="utf-8") as out:
            out.write(sid + "\n")
        value = {"sessionId": sid}
    else:
        value = {}
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": rid, "result": value}) + "\n")
    sys.stdout.flush()
