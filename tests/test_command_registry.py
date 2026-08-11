import pytest

from tests.plugin_fixtures import FakeEntryPoint, install_entrypoints
from untether import commands, plugins
from untether.config import ConfigError


class DummyCommand:
    id = "hello"
    description = "Hello command"

    async def handle(self, ctx):
        _ = ctx
        return


@pytest.fixture
def command_entrypoints(monkeypatch):
    entrypoints = [
        FakeEntryPoint(
            "hello",
            "untether.commands.hello:BACKEND",
            plugins.COMMAND_GROUP,
            loader=DummyCommand,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)
    return entrypoints


def test_command_registry_lists_ids(command_entrypoints) -> None:
    ids = commands.list_command_ids()
    assert "hello" in ids


def test_command_registry_gets_command(command_entrypoints) -> None:
    backend = commands.get_command("hello")
    assert backend.id == "hello"


def test_command_registry_unknown(command_entrypoints) -> None:
    with pytest.raises(ConfigError, match="Unknown command"):
        commands.get_command("nope")


def test_command_registry_optional_missing(command_entrypoints) -> None:
    assert commands.get_command("nope", required=False) is None


def test_installed_registry_loads_health_backend() -> None:
    ids = commands.list_command_ids()
    backend = commands.get_command("health")

    assert "health" in ids
    assert backend.id == "health"


def test_every_installed_command_entrypoint_matches_backend_id() -> None:
    for entrypoint in plugins._select_entrypoints(plugins.COMMAND_GROUP):
        backend = entrypoint.load()
        assert entrypoint.name == backend.id


@pytest.mark.anyio
async def test_unknown_dispatched_command_returns_visible_error(monkeypatch) -> None:
    from types import SimpleNamespace

    from untether.runner_bridge import RunningTasks
    from untether.scheduler import ThreadScheduler
    from untether.telegram.commands import dispatch
    from untether.telegram.types import TelegramIncomingMessage

    sent: list[tuple[str, object, bool]] = []

    class Executor:
        def __init__(self, **_kwargs) -> None:
            pass

        async def send(self, text, *, reply_to, notify) -> None:
            sent.append((text, reply_to, notify))

    monkeypatch.setattr(dispatch, "_TelegramCommandExecutor", Executor)
    monkeypatch.setattr(dispatch, "get_command", lambda *_args, **_kwargs: None)
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(allowlist=None),
        exec_cfg=object(),
        show_resume_line=False,
    )
    msg = TelegramIncomingMessage(
        transport="telegram", chat_id=42, message_id=9, text="/missing",
        reply_to_message_id=None, reply_to_text=None, sender_id=None,
    )

    await dispatch._dispatch_command(
        cfg, msg, "/missing", "missing", "", RunningTasks(),
        SimpleNamespace(), None, False, None, None,
    )

    assert sent and sent[0][0] == "error: command unavailable"
    assert sent[0][1].message_id == 9
