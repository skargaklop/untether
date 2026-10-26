"""Two distinct source sessions through one fixed adapter, separate stores."""
from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from untether.session_migration.native_adapter import assert_exact_restore
from untether.session_migration.subprocess_adapter import ExecutablePersistenceAdapter

FIXTURE = Path(__file__).parent / "fixtures" / "native_persistence_engine.py"


@pytest.mark.skipif(os.name == "nt", reason="POSIX executable fixture")
def test_two_sessions_independent_processes(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    executable = tmp_path / "engine"
    executable.write_text("#!" + sys.executable + "\n" + FIXTURE.read_text())
    executable.chmod(0o700)
    exporter = ExecutablePersistenceAdapter("fixture", executable, source)
    importer = ExecutablePersistenceAdapter("fixture", executable, destination)
    for _ in range(2):
        sid = str(uuid4())
        original = {
            "engine": "fixture", "logical_id": sid, "native_id": sid,
            "generation": 2, "lineage": sid,
            "records": {
                "history/tool.ndjson": base64.b64encode(b"tool\r\nraw").decode(),
                "assets/a.bin": base64.b64encode(b"\x00\xff\xfe").decode(),
            },
        }
        (source / (sid + ".json")).write_text(json.dumps(original))
        exported = exporter.export_exact(sid)
        proof = importer.import_exact(exported)
        assert assert_exact_restore(exported, proof) == sid
        independently_loaded = importer.export_exact(sid)
        assert independently_loaded.records == exported.records
        assert independently_loaded.logical_id == sid
        assert independently_loaded.lineage == sid
