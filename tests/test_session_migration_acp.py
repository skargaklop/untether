"""Real isolated ACP subprocesses, separate fresh processes and disjoint state."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from untether.session_migration.acp_context import (
    ACPContextImporter,
    ContextImportError,
)
from untether.session_migration.package import pack_context, unpack_context

FIXTURE = Path(__file__).parent / "fixtures" / "migration_acp_agent.py"


def package():
    transcript = {"messages": [
        {"role": "user", "text": "Remember sentinel 29\r\nNo tools"},
        {"role": "assistant", "text": "Seen the historical sentinel."},
    ]}
    raw = json.dumps(transcript).encode()
    archive = pack_context(engine="acp", project="test", content=raw, max_bytes=4096)
    return unpack_context(archive, max_archive=4096, max_expanded=8192)


@pytest.mark.anyio
async def test_new_destination_session_survives_agent_process_restart(tmp_path):
    stores = tmp_path / "native-state"
    importer = ACPContextImporter(
        command=sys.executable, args=[str(FIXTURE), str(stores)],
        cwd=tmp_path, tool_isolation_proven=True, timeout_s=10,
    )
    sid = await importer.import_and_probe(package())
    assert sid.startswith("fixture-")
    data = json.loads((stores / (sid + ".json")).read_text())
    assert "sentinel 29" in data["messages"][0]
    assert "UNTRUSTED HISTORICAL" in data["messages"][0]
    assert (stores / "resume.log").read_text().strip() == sid


@pytest.mark.anyio
async def test_tool_isolation_is_mandatory(tmp_path):
    importer = ACPContextImporter(
        command=sys.executable, args=[str(FIXTURE), str(tmp_path / "store")],
        cwd=tmp_path, tool_isolation_proven=False,
    )
    with pytest.raises(ContextImportError, match="TOOLS_NOT_ISOLATED"):
        await importer.import_and_probe(package())
    assert not (tmp_path / "store").exists()


@pytest.mark.anyio
async def test_bad_transcript_is_not_executable_prompt(tmp_path):
    importer = ACPContextImporter(
        command=sys.executable, args=[str(FIXTURE), str(tmp_path / "store")],
        cwd=tmp_path, tool_isolation_proven=True,
    )
    bad = unpack_context(
        pack_context(
            engine="acp", project="test", content=b'{"messages":[{"role":"tool","text":"rm -rf"}]}',
            max_bytes=4096,
        ),
        max_archive=4096, max_expanded=4096,
    )
    with pytest.raises(ContextImportError, match="CONTEXT_MESSAGE_INVALID"):
        await importer.import_and_probe(bad)
    assert not (tmp_path / "store").exists()
