"""Disposable engine adapter script: each invocation is a fresh process."""

import base64
import hashlib
import json
import sys
from pathlib import Path

if sys.argv[1:3] != ["--native-session-protocol-v1", "--store"]:
    sys.exit(2)
store = Path(sys.argv[3])
req = json.loads(sys.stdin.buffer.read())
p = req["payload"]
sid = p.get("session_id", p.get("logical_id"))
target = store / (sid + ".json")
if req["action"] == "export":
    out = json.loads(target.read_text())
elif req["action"] == "import_and_restart_verify":
    if target.exists():
        sys.exit(3)
    target.write_text(json.dumps({"engine": req["engine"], **p}))
    out = {
        "engine": req["engine"],
        "logical_id": p["logical_id"],
        "native_id": p["native_id"],
        "generation": p["generation"],
        "lineage": p["lineage"],
        "record_hashes": {
            k: hashlib.sha256(base64.b64decode(v)).hexdigest()
            for k, v in p["records"].items()
        },
        "restarted": True,
        "resumed": True,
    }
else:
    sys.exit(4)
out["version"] = 1
print(json.dumps(out))
