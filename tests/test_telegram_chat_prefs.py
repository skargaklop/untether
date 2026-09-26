import pytest

from untether.telegram.chat_prefs import ChatPrefsStore


@pytest.mark.anyio
async def test_chat_prefs_store_roundtrip(tmp_path) -> None:
    path = tmp_path / "telegram_chat_prefs_state.json"
    store = ChatPrefsStore(path)
    await store.set_default_engine(123, "codex")
    await store.set_trigger_mode(123, "mentions")
    await store.set_default_engine(123, "codex")
    await store.clear_default_engine(456)

    assert await store.get_default_engine(123) == "codex"
    assert await store.get_trigger_mode(123) == "mentions"

    store2 = ChatPrefsStore(path)
    assert await store2.get_default_engine(123) == "codex"
    assert await store2.get_trigger_mode(123) == "mentions"

    await store2.clear_default_engine(123)
    assert await store2.get_default_engine(123) is None
    assert await store2.get_trigger_mode(123) == "mentions"

    await store2.clear_trigger_mode(123)
    assert await store2.get_trigger_mode(123) is None


@pytest.mark.anyio
async def test_store_sees_external_rewrite_within_same_mtime_tick(tmp_path) -> None:
    """Two store instances on one file must not miss an external rewrite.

    On some filesystems (notably GitHub's windows-latest runners) two
    writes can land in the same ``st_mtime_ns`` tick. An mtime-only
    staleness check then skips the reload and serves stale state.
    """
    from untether.telegram.engine_overrides import EngineOverrides

    path = tmp_path / "prefs.json"
    store_a = ChatPrefsStore(path)
    store_b = ChatPrefsStore(path)

    await store_a.set_engine_override(123, "claude", EngineOverrides(model="x"))

    # store_b reloads on first access: sees the write.
    override = await store_b.get_engine_override(123, "claude")
    assert override is not None
    assert override.model == "x"

    # store_a rewrites the file; simulate the windows-latest pathology
    # where the rewrite lands in the same mtime tick as the previous write
    # (old mtime), while the file itself was genuinely replaced (new ino).
    old_mtime = (store_b._identity or (None, None, None))[0]
    await store_a.set_engine_override(123, "claude", EngineOverrides(model="y"))
    _mtime, ino, size = store_a._stat_signature()
    store_b._identity = (old_mtime, ino, size)  # colliding mtime, real replace
    override = await store_b.get_engine_override(123, "claude")
    assert override is not None
    assert override.model == "y", "store_b served stale state across same-mtick rewrite"
