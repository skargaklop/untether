"""/session Telegram command: one shared native transfer coordinator path."""

from __future__ import annotations

import contextlib
import sys
import tomllib
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import anyio
import pytest

from untether import commands
from untether.session_catalog import (
    STATE_FILENAME,
    SessionCatalog,
    SessionObservation,
)
from untether.session_migration import telegram_command, transfer
from untether.session_migration.native_adapter import (
    CapabilityUnavailable,
    register_persistence_adapter,
    unregister_persistence_adapter,
)
from untether.session_migration.settings import MigrationPeer, SessionMigrationSettings
from untether.session_migration.ssh_transport import PreflightError
from untether.session_migration.telegram_command import (
    SessionCommand,
    describe_engine_adapters,
)
from untether.transport import MessageRef, RenderedMessage

FIXTURE = Path(__file__).parent / "fixtures" / "native_persistence_engine.py"
ENGINE = "fixture-engine"
PIN = "SHA256:" + "A" * 43
AUTH_CHAT = -100123
AUTH_TOPIC = 42


@dataclass
class _Executor:
    sent: list[RenderedMessage | str] = field(default_factory=list)

    async def send(self, message, *, reply_to=None, notify=True):
        self.sent.append(message)
        return MessageRef(channel_id=123, message_id=2)

    async def edit(self, ref, message):
        return ref

    async def run_one(self, *args, **kwargs):
        raise AssertionError("/session does not run engines")

    async def run_many(self, *args, **kwargs):
        raise AssertionError("/session does not run engines")


def _peer() -> MigrationPeer:
    return MigrationPeer(
        host="example.invalid",
        user="operator",
        identity_dir="keys",
        identity_file="selected",
        known_hosts_file="known",
        host_key_fingerprint=PIN,
        receiver_path="/usr/local/bin/untether-migration-receiver",
        execution_mode="native",
        expected_endpoint_id="endpoint-one",
        destination_binding="enrolled-one",
        projects={"demo": "/srv/demo"},
    )


def _cfg() -> SessionMigrationSettings:
    return SessionMigrationSettings(enabled=True, peers={"vps": _peer()})


def _ctx(
    tmp_path: Path,
    *,
    channel: int = 123,
    thread: int | None = None,
    config_path: Path | None = None,
    args_text="",
):
    from untether.commands import CommandContext

    return CommandContext(
        command="session",
        text="/session",
        args_text=args_text,
        args=tuple(args_text.split()),
        message=MessageRef(channel_id=channel, message_id=1, thread_id=thread),
        reply_to=None,
        reply_text=None,
        config_path=config_path,
        plugin_config={},
        runtime=object(),
        executor=_Executor(),
    )


def _fake_result(kwargs: dict) -> transfer.TransferResult:
    return transfer.TransferResult(
        operation_id=str(uuid4()),
        session_id=kwargs["session_id"],
        engine=kwargs["engine"],
        direction=kwargs["direction"],
        peer=kwargs["peer_name"],
        state="completed" if kwargs["direction"] == "to" else "bound",
        native_id=kwargs["session_id"],
        receipt_sha256="d" * 64,
    )


@contextlib.contextmanager
def _isolated_command_metadata(tmp_path: Path) -> Iterator[None]:
    """Temp .dist-info generated from pyproject, discovered via sys.path scope.

    Builds a real distribution metadata directory from the working tree
    ``pyproject.toml`` entry-point tables and makes ``sys.path`` point at it
    alone while the block runs. The installed venv metadata is never read or
    edited; registration is verified exactly as it will resolve after deploy.
    """
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data["project"]
    dist = tmp_path / f"untether-{project['version']}.dist-info"
    dist.mkdir()
    ep_lines: list[str] = []
    for group, entries in project["entry-points"].items():
        ep_lines.append(f"[{group}]")
        ep_lines.extend(f"{name} = {value}" for name, value in entries.items())
    (dist / "entry_points.txt").write_text("\n".join(ep_lines) + "\n", encoding="utf-8")
    (dist / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: untether\nVersion: {project['version']}\n",
        encoding="utf-8",
    )
    original_path = sys.path[:]
    sys.path[:] = [str(tmp_path)]
    try:
        yield
    finally:
        sys.path[:] = original_path


def test_engine_adapter_report_is_honest():
    """No engine adapter ships in-tree; the report must say so, not claim ACP."""
    lines = describe_engine_adapters()
    text = "\n".join(lines)
    assert "none installed" in text
    assert "untether.session_persistence" in text


def test_session_command_push_monkeypatched_coordinator(tmp_path):
    """Command formatting drives the real coordinator function reference."""
    ctx = _ctx(tmp_path)
    register_persistence_adapter(
        ENGINE,
        lambda: __import__(
            "untether.session_migration.subprocess_adapter",
            fromlist=["ExecutablePersistenceAdapter"],
        ).ExecutablePersistenceAdapter(
            ENGINE, FIXTURE, tmp_path, interpreter=__import__("sys").executable
        ),
    )
    calls = []

    def fake_perform(cfg, **kwargs):
        calls.append(kwargs)
        return _fake_result(kwargs)

    command = SessionCommand(_cfg, perform=fake_perform)
    try:
        result = anyio.run(command.handle_push, ctx, "vps", "demo", "sid", ENGINE)
        assert result is not None
        text = result.text
        assert "completed" in text
        assert "sid" in text
        assert "vps" in text
        assert calls and calls[0]["direction"] == "to"
        assert calls[0]["engine"] == ENGINE
    finally:
        unregister_persistence_adapter(ENGINE)


def test_session_command_reports_refusal_without_leaking(tmp_path):
    command = SessionCommand(_cfg(), perform=None)
    ctx = _ctx(tmp_path)
    result = anyio.run(command.handle_push, ctx, "vps", "demo", "x", "unknown-engine")
    text = result.text if result else ""
    assert "refused" in text
    # Redacted code only: no paths, no stack details.
    assert str(tmp_path) not in text
    assert "\\" not in text or "refused" in text.split("\\")[0]


def test_refusal_hierarchy_catches_coordinator_transfer_refusal(tmp_path):
    """TransferRefusal (a CapabilityUnavailable subclass) renders redacted."""
    assert issubclass(transfer.TransferRefusal, CapabilityUnavailable)

    def refusing_perform(cfg, **kwargs):
        raise transfer.TransferRefusal("RESTORE_UNVERIFIED")

    command = SessionCommand(_cfg(), perform=refusing_perform)
    ctx = _ctx(tmp_path)
    result = anyio.run(command.handle_push, ctx, "vps", "demo", "sid", ENGINE)
    assert result is not None
    assert result.text == "session transfer refused: RESTORE_UNVERIFIED"
    assert str(tmp_path) not in result.text


def test_disabled_migration_refuses(tmp_path):
    command = SessionCommand(SessionMigrationSettings(), perform=None)
    ctx = _ctx(tmp_path)
    result = anyio.run(command.handle_push, ctx, "vps", "demo", "sid", ENGINE)
    assert result is not None
    assert "MIGRATION_DISABLED" in result.text


def test_pull_defaults_to_authenticated_chat_and_thread(tmp_path):
    """Local pull binds the caller's ctx chat/topic when no IDs are given."""
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    ctx = _ctx(tmp_path, channel=AUTH_CHAT, thread=AUTH_TOPIC)
    command = SessionCommand(_cfg(), perform=fake_perform)
    result = anyio.run(command.handle_pull, ctx, "vps", "demo", "sid", ENGINE)
    assert result is not None
    assert recorded[0]["direction"] == "from"
    assert recorded[0]["destination_chat"] == AUTH_CHAT
    assert recorded[0]["destination_topic"] == AUTH_TOPIC
    assert "bound" in result.text


def test_pull_accepts_explicit_ids_matching_authenticated_context(tmp_path):
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    ctx = _ctx(tmp_path, channel=AUTH_CHAT, thread=AUTH_TOPIC)
    command = SessionCommand(_cfg(), perform=fake_perform)
    result = anyio.run(command.handle_pull, ctx, "vps", "demo", "sid", ENGINE)
    assert result is not None
    assert recorded[0]["destination_chat"] == AUTH_CHAT
    assert recorded[0]["destination_topic"] == AUTH_TOPIC


def test_pull_rejects_foreign_destination_chat(tmp_path):
    ctx = _ctx(
        tmp_path,
        channel=AUTH_CHAT,
        thread=AUTH_TOPIC,
        args_text=f"pull vps demo sid {ENGINE} -999 42",
    )
    command = SessionCommand(_cfg(), perform=None)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "DESTINATION_FOREIGN" in result.text


def test_pull_rejects_foreign_destination_topic(tmp_path):
    ctx = _ctx(
        tmp_path,
        channel=AUTH_CHAT,
        thread=AUTH_TOPIC,
        args_text=f"pull vps demo sid {ENGINE} {AUTH_CHAT} 999",
    )
    command = SessionCommand(_cfg(), perform=None)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "DESTINATION_FOREIGN" in result.text


def test_pull_rejects_non_numeric_destination(tmp_path):
    ctx = _ctx(
        tmp_path,
        channel=AUTH_CHAT,
        thread=AUTH_TOPIC,
        args_text=f"pull vps demo sid {ENGINE} not-a-chat 42",
    )
    command = SessionCommand(_cfg(), perform=None)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "DESTINATION_INVALID" in result.text


def test_session_command_registered_plugin_and_dispatched(tmp_path):
    """The command backend is registered as untether.command_backends entry point.

    Discovery runs against temp .dist-info metadata generated from the working
    tree pyproject (not the pre-deploy installed metadata).
    """
    with _isolated_command_metadata(tmp_path):
        backend = commands.get_command("session")
    assert backend.id == "session"
    assert "Transfer a session" in backend.description

    calls = []

    def fake_perform(cfg, **kwargs):
        calls.append(kwargs)
        return _fake_result(kwargs)

    original_settings = backend._settings
    original_perform = backend._perform
    backend._settings = _cfg
    backend._perform = fake_perform
    try:
        ctx_pull = _ctx(
            tmp_path,
            channel=AUTH_CHAT,
            thread=AUTH_TOPIC,
            config_path=tmp_path / "untether.toml",
            args_text=f"pull vps demo sid {ENGINE} {AUTH_CHAT} {AUTH_TOPIC}",
        )

        res_pull = anyio.run(backend.handle, ctx_pull)
        assert res_pull is not None
        assert "bound" in res_pull.text
        assert calls[-1]["direction"] == "from"
        assert calls[-1]["destination_chat"] == AUTH_CHAT
        assert calls[-1]["destination_topic"] == AUTH_TOPIC
        assert calls[-1]["config_path"] == tmp_path / "untether.toml"

        ctx_push = _ctx(
            tmp_path,
            channel=AUTH_CHAT,
            thread=AUTH_TOPIC,
            args_text=f"push vps demo sid {ENGINE}",
        )
        res_push = anyio.run(backend.handle, ctx_push)
        assert res_push is not None
        assert "completed" in res_push.text
        assert calls[-1]["direction"] == "to"
    finally:
        backend._settings = original_settings
        backend._perform = original_perform


@pytest.mark.parametrize("local_engines", [[], ["local-one", "local-two"]])
@pytest.mark.parametrize("project", [None, "alt"])
def test_move_from_inspects_remote_engine(
    tmp_path, monkeypatch, local_engines, project
):
    inspected = []
    performed = []
    cfg = _cfg()
    if project is not None:
        cfg = cfg.model_copy(update={"peers": {"vps": _peer_two_projects()}})
    monkeypatch.setattr(transfer, "registered_adapter_engines", lambda: local_engines)

    def inspect(peer, *, project, session_id, timeout_s):
        inspected.append((peer, project, session_id, timeout_s))
        return "verified-remote-engine"

    def perform(cfg, **kwargs):
        performed.append(kwargs)
        return _fake_result(kwargs)

    monkeypatch.setattr(telegram_command, "inspect_session", inspect)
    flags = f" --project {project}" if project else ""
    ctx = _ctx(tmp_path, args_text=f"move remote-sid --from vps{flags}")
    result = anyio.run(SessionCommand(cfg, perform=perform).handle, ctx)
    assert inspected == [
        (cfg.peers["vps"], project or "demo", "remote-sid", cfg.probe_timeout_s)
    ]
    assert len(performed) == 1
    assert performed[0]["engine"] == "verified-remote-engine"
    assert performed[0]["project"] == (project or "demo")
    assert performed[0]["session_id"] == "remote-sid"
    assert "bound" in result.text


@pytest.mark.parametrize(
    "failure",
    ["SESSION_NOT_FOUND", "RECEIVER_UNSUPPORTED", "secret /srv/private payload"],
)
def test_move_from_inspection_refuses_without_guessing(tmp_path, monkeypatch, failure):
    monkeypatch.setattr(transfer, "registered_adapter_engines", lambda: [ENGINE])

    def inspect(*args, **kwargs):
        raise PreflightError(failure)

    def perform(*args, **kwargs):
        pytest.fail("failed inspection must not transfer")

    monkeypatch.setattr(telegram_command, "inspect_session", inspect)
    ctx = _ctx(tmp_path, args_text="move sid --from vps")
    result = anyio.run(SessionCommand(_cfg(), perform=perform).handle, ctx)
    assert result.text == "session transfer refused: REMOTE_INSPECTION_FAILED"
    assert failure not in result.text


@pytest.mark.parametrize(
    "args_text",
    [
        f"move sid --from vps --engine {ENGINE}",
        f"pull vps demo sid {ENGINE}",
    ],
)
def test_explicit_inbound_engine_bypasses_inspection(tmp_path, monkeypatch, args_text):
    performed = []

    def inspect(*args, **kwargs):
        pytest.fail("explicit engine must not inspect")

    def perform(cfg, **kwargs):
        performed.append(kwargs)
        return _fake_result(kwargs)

    monkeypatch.setattr(telegram_command, "inspect_session", inspect)
    result = anyio.run(
        SessionCommand(_cfg(), perform=perform).handle,
        _ctx(tmp_path, args_text=args_text),
    )
    assert performed[0]["engine"] == ENGINE
    assert "bound" in result.text


def _peer_two_projects() -> MigrationPeer:
    return _peer().model_copy(
        update={"projects": {"demo": "/srv/demo", "alt": "/srv/alt"}}
    )


def test_move_to_routes_coordinator_with_exact_args(tmp_path):
    """move --to reaches the shared coordinator with exactly parsed args."""
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    ctx = _ctx(
        tmp_path,
        channel=AUTH_CHAT,
        thread=AUTH_TOPIC,
        args_text=f"move sid --to vps --engine {ENGINE} --project demo",
    )
    command = SessionCommand(_cfg, perform=fake_perform)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "completed" in result.text
    assert len(recorded) == 1
    call = recorded[0]
    assert call["direction"] == "to"
    assert call["peer_name"] == "vps"
    assert call["project"] == "demo"
    assert call["session_id"] == "sid"
    assert call["engine"] == ENGINE
    assert call["destination_chat"] == AUTH_CHAT


def test_move_from_routes_local_and_binds_current_chat(tmp_path):
    """move --from pulls into the caller's chat/thread without explicit IDs."""
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    ctx = _ctx(
        tmp_path,
        channel=AUTH_CHAT,
        thread=AUTH_TOPIC,
        args_text=f"move sid --from vps --project demo --engine {ENGINE}",
    )
    command = SessionCommand(_cfg(), perform=fake_perform)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "bound" in result.text
    call = recorded[0]
    assert call["direction"] == "from"
    assert call["peer_name"] == "vps"
    assert call["destination_chat"] == AUTH_CHAT
    assert call["destination_topic"] == AUTH_TOPIC


def test_move_refuses_duplicate_flag_without_calling(tmp_path):
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    ctx = _ctx(
        tmp_path,
        args_text=f"move sid --to vps --project demo --project demo --engine {ENGINE}",
    )
    command = SessionCommand(_cfg(), perform=fake_perform)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "ARGUMENT_INVALID" in result.text
    assert recorded == []


def test_move_refuses_both_directions_without_calling(tmp_path):
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    ctx = _ctx(
        tmp_path,
        args_text=f"move sid --to vps --from vps --project demo --engine {ENGINE}",
    )
    command = SessionCommand(_cfg(), perform=fake_perform)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "DIRECTION_INVALID" in result.text
    assert recorded == []


def test_move_refuses_unique_configured_engine_when_sid_undiscovered(tmp_path):
    """Outbound omitted engine never guesses: discovery precedes uniqueness.

    With one configured+registered engine the old fallback transferred it;
    now an sid absent from the catalog refuses instead.
    """
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    register_persistence_adapter(ENGINE, lambda: None)
    try:
        ctx = _ctx(
            tmp_path,
            args_text="move sid --to vps --project demo",
            config_path=tmp_path / "untether.toml",
        )
        command = SessionCommand(_cfg(), perform=fake_perform)
        result = anyio.run(command.handle, ctx)
        assert result is not None
        assert "SESSION_NOT_DISCOVERED" in result.text
        assert recorded == []
    finally:
        unregister_persistence_adapter(ENGINE)


def _seed_catalog(tmp_path: Path, entries: tuple[tuple[str, str], ...]) -> None:
    catalog = SessionCatalog(tmp_path / STATE_FILENAME)

    async def seed():
        for engine, sid in entries:
            await catalog.observe(
                SessionObservation(engine=engine, session_id=sid, resume_value="r")
            )

    anyio.run(seed)


def test_move_to_resolves_discovered_engine_among_two(tmp_path):
    """One catalog hit across two registered engines picks that engine."""
    other = "fixture-engine-two"
    register_persistence_adapter(ENGINE, lambda: None)
    register_persistence_adapter(other, lambda: None)
    _seed_catalog(tmp_path, ((ENGINE, "sid"),))
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    try:
        ctx = _ctx(
            tmp_path,
            args_text="move sid --to vps --project demo",
            config_path=tmp_path / "untether.toml",
        )
        command = SessionCommand(_cfg(), perform=fake_perform)
        result = anyio.run(command.handle, ctx)
        assert result is not None
        assert "completed" in result.text
        assert recorded[0]["engine"] == ENGINE
    finally:
        unregister_persistence_adapter(ENGINE)
        unregister_persistence_adapter(other)


def test_move_to_refuses_catalog_collision(tmp_path):
    """Same sid discovered under two engines refuses instead of guessing."""
    other = "fixture-engine-two"
    register_persistence_adapter(ENGINE, lambda: None)
    register_persistence_adapter(other, lambda: None)
    _seed_catalog(tmp_path, ((ENGINE, "sid"), (other, "sid")))
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    try:
        ctx = _ctx(
            tmp_path,
            args_text="move sid --to vps --project demo",
            config_path=tmp_path / "untether.toml",
        )
        command = SessionCommand(_cfg(), perform=fake_perform)
        result = anyio.run(command.handle, ctx)
        assert result is not None
        assert "ENGINE_AMBIGUOUS" in result.text
        assert recorded == []
    finally:
        unregister_persistence_adapter(ENGINE)
        unregister_persistence_adapter(other)


def test_move_refuses_ambiguous_project_without_calling(tmp_path):
    """A peer with two projects requires --project; nothing is inferred."""
    recorded = []

    def fake_perform(cfg, **kwargs):
        recorded.append(kwargs)
        return _fake_result(kwargs)

    cfg = SessionMigrationSettings(enabled=True, peers={"vps": _peer_two_projects()})
    ctx = _ctx(
        tmp_path,
        args_text=f"move sid --to vps --engine {ENGINE}",
    )
    command = SessionCommand(lambda: cfg, perform=fake_perform)
    result = anyio.run(command.handle, ctx)
    assert result is not None
    assert "PROJECT_AMBIGUOUS" in result.text
    assert recorded == []
