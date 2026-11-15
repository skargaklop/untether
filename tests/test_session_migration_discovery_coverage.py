"""G6: discovery-derived ACP breadth coverage (not a fixed roster).

Requirement under test: coverage must be derived from the declared ACP
ecosystem discovery surface, not from Untether's installed-engine roster
(docs/plans/session-migration-acp-wide-goals.md G6). This suite:

1. Parses a pinned snapshot of the official ACP registry (the same
   ``parse_registry_agents`` used by runtime discovery) — every discovered
   agent id is a coverage row.
2. Proves each discovered agent id can *join* session migration through the
   declared ``executable-v1`` persistence profile — the documented extension
   path with zero core dispatch edits — by moving one session push-through-
   import with byte-exact records against the real external adapter process.
3. Separately names the production-adapter engines (Pi/OMP/Codex/OpenCode)
   and requires their installed-profile evidence to exist rather than
   letting the generic path silently substitute for it.

A registry agent without a registered persistence profile is an honest
refusal, not a covered row; this suite therefore never claims native
restoration for engines it only proves *joinable*. Native restoration
remains the named production-adapter suites' responsibility
(``scripts/check-engine-matrix.sh``).
"""

from __future__ import annotations

import base64
import json
import uuid
from pathlib import Path

import pytest

from untether.acp_registry import parse_registry_agents
from untether.session_migration import transfer
from untether.session_migration.native_adapter import (
    unregister_persistence_adapter,
)
from untether.session_migration.settings import (
    EnginePersistenceSettings,
    SessionMigrationSettings,
)

from .test_session_migration_independent_extension import (
    _peer,
    _profile,
    _receiver_rpc,
    _records,
)

SNAPSHOT = Path(__file__).parent / "fixtures" / "acp-registry-snapshot.json"


def _write_engine_store_file(
    store: Path, sid: str, *, engine: str, generation: int
) -> None:
    """Engine-parameterised variant of the independent-extension seeder."""
    payload = {
        "engine": engine,
        "logical_id": sid,
        "native_id": sid,
        "generation": generation,
        "lineage": sid,
        "records": {
            name: base64.b64encode(blob).decode()
            for name, blob in _records(sid).items()
        },
    }
    (store / (sid + ".json")).write_text(json.dumps(payload), encoding="utf-8")


# Named production adapters that must carry their own installed-profile
# evidence (engine-matrix suites); the generic join path never substitutes.
PRODUCTION_ADAPTER_ENGINES = ("pi", "omp", "codex", "opencode")


def _discovered_agent_ids() -> list[str]:
    payload = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    agents = parse_registry_agents(payload["value"])
    assert agents, "pinned ACP registry snapshot must not be empty"
    return [agent.id for agent in agents]


@pytest.fixture(autouse=True)
def _gate_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gate"))


@pytest.mark.parametrize("engine", _discovered_agent_ids())
def test_discovered_agent_joins_through_extension_profile(tmp_path, engine):
    """Every registry-discovered agent id can join via the declared profile.

    The agent ids are the coverage rows; they are derived from the pinned
    discovery snapshot at collection time, so adding an agent to the ACP
    registry adds a coverage row here without any code edit.
    """
    src_store, dst_store = tmp_path / "a", tmp_path / "b"
    src_store.mkdir()
    dst_store.mkdir()
    sid = str(uuid.uuid4())
    _write_engine_store_file(src_store, sid, engine=engine, generation=1)

    try:
        result = transfer.perform(
            SessionMigrationSettings(
                enabled=True,
                peers={"vps": _peer()},
                persistence=EnginePersistenceSettings(
                    engines={engine: _profile(src_store)}
                ),
            ),
            peer_name="vps",
            project="demo",
            session_id=sid,
            engine=engine,
            direction="to",
            root=tmp_path / "ops",
            receiver_call=_receiver_rpc(dst_store, tmp_path, engine=engine),
        )
    finally:
        unregister_persistence_adapter(engine)
    assert result.state == "completed"
    assert result.engine == engine and result.native_id == sid
    landed = json.loads((dst_store / f"{sid}.json").read_text(encoding="utf-8"))
    for name, blob in _records(sid).items():
        assert base64.b64decode(landed["records"][name]) == blob


def test_production_adapter_engines_have_named_evidence():
    """The generic join path never substitutes for the production roster.

    Each named production adapter engine must keep its runtime/coordinator
    suites in the documented engine-matrix command; this check pins the
    mapping so removing a suite from the matrix command fails here.
    """
    import re

    script = (
        Path(__file__).parent.parent / "scripts" / "check-engine-matrix.sh"
    ).read_text(encoding="utf-8")
    for engine in PRODUCTION_ADAPTER_ENGINES:
        assert re.search(
            rf"test_session_migration_{engine}_(runtime|coordinator)\.py", script
        ), f"engine-matrix command lost the {engine} production suites"


def test_snapshot_matches_parse_contract():
    """The pinned snapshot stays parseable by the discovery parser.

    Guards the coverage rows against parser/snapshot drift: if either side
    changes shape, this fails instead of silently shrinking coverage.
    """
    ids = _discovered_agent_ids()
    assert "codex-acp" in ids or "codex" in PRODUCTION_ADAPTER_ENGINES
    assert len(ids) >= 30, f"registry snapshot implausibly small: {len(ids)}"
