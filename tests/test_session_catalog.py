from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from untether.session_catalog import SessionCatalog, SessionObservation


def _obs(**overrides: object) -> SessionObservation:
    data = SessionObservation(
        engine="omp",
        session_id="01a0e1ed",
        resume_value="01a0e1ed",
        project_path=r"D:\OSPanel\home\dolibarr.local",
        title="Fix resume",
        first_user_prompt="first ask",
        last_user_prompt="check the latest errors",
        created_at=10.0,
        updated_at=20.0,
        model="grok",
    )
    return replace(data, **overrides)


@pytest.mark.anyio
async def test_observe_then_recent_pages_newest_first(tmp_path: Path) -> None:
    catalog = SessionCatalog(tmp_path / "session_catalog_state.json")
    await catalog.observe(_obs(session_id="old", updated_at=1.0, title="Old"))
    await catalog.observe(_obs(session_id="new", updated_at=5.0, title="New"))
    await catalog.observe(
        _obs(engine="claude", session_id="other", updated_at=9.0, title="Other")
    )

    page = await catalog.recent(
        engine="omp",
        project_path=Path(r"D:\OSPanel\home\dolibarr.local"),
        offset=0,
        limit=1,
    )

    assert page.total == 2
    assert [item.session_id for item in page.items] == ["new"]
    assert page.items[0].last_user_prompt == "check the latest errors"


@pytest.mark.anyio
async def test_merge_keeps_nonempty_overlay_when_provider_fields_empty(
    tmp_path: Path,
) -> None:
    catalog = SessionCatalog(tmp_path / "session_catalog_state.json")
    await catalog.observe(_obs(title="Kept", last_user_prompt="real ask"))
    await catalog.merge_provider(
        [
            _obs(
                title=None,
                last_user_prompt=None,
                first_user_prompt=None,
                updated_at=30.0,
                source="provider",
            )
        ]
    )

    found = await catalog.get("omp", "01a0e1ed")

    assert found is not None
    assert found.title == "Kept"
    assert found.last_user_prompt == "real ask"
    assert found.updated_at == 30.0


@pytest.mark.anyio
async def test_corrupt_state_loads_empty(tmp_path: Path) -> None:
    path = tmp_path / "session_catalog_state.json"
    path.write_text("{not json", encoding="utf-8")
    catalog = SessionCatalog(path)

    page = await catalog.recent(engine=None, project_path=None, offset=0, limit=10)

    assert page.total == 0
