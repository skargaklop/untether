"""Tests for the `/health` command (#348)."""

from __future__ import annotations

import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import anyio
import pytest

from untether.commands import CommandContext, RuntimeStatusSnapshot
from untether.telegram.commands import health
from untether.telegram.commands.health import (
    HealthCommand,
    SystemSnapshot,
    UsageSnapshot,
    _format_mb,
    _read_meminfo_fields,
    render_health_snapshot,
)
from untether.transport import MessageRef, RenderedMessage


@dataclass
class _Executor:
    sent: list[tuple[RenderedMessage | str, MessageRef | None, bool]] = field(
        default_factory=list
    )
    edits: list[tuple[MessageRef, RenderedMessage | str]] = field(default_factory=list)

    async def send(self, message, *, reply_to=None, notify=True):
        self.sent.append((message, reply_to, notify))
        return MessageRef(channel_id=123, message_id=2)

    async def edit(self, ref, message):
        self.edits.append((ref, message))
        return ref

    async def run_one(self, *args, **kwargs):
        raise AssertionError("health does not run engines")

    async def run_many(self, *args, **kwargs):
        raise AssertionError("health does not run engines")


def _make_ctx(
    *,
    trigger_manager=None,
    config_path: Path | None = None,
    executor: _Executor | None = None,
) -> CommandContext:
    return CommandContext(
        command="health",
        text="/health",
        args_text="",
        args=(),
        message=MessageRef(channel_id=123, message_id=1),
        reply_to=None,
        reply_text=None,
        config_path=config_path,
        plugin_config={},
        runtime=MagicMock(),
        executor=executor or _Executor(),
        trigger_manager=trigger_manager,
        runtime_status=RuntimeStatusSnapshot(2, 3, True, 1, 0),
    )


def test_command_id() -> None:
    assert HealthCommand().id == "health"


@pytest.mark.anyio
async def test_handle_sends_initial_summary_then_edits_same_message(tmp_path) -> None:
    ctx = _make_ctx(config_path=tmp_path / "untether.toml")
    result = await HealthCommand().handle(ctx)

    assert result is None
    executor = cast(_Executor, ctx.executor)
    assert len(executor.sent) == 1
    initial, reply_to, notify = executor.sent[0]
    assert isinstance(initial, RenderedMessage)
    assert initial.extra == {"parse_mode": "HTML"}
    assert reply_to == ctx.message
    assert notify is False
    assert "collecting diagnostics" in initial.text
    assert executor.edits and executor.edits[0][0].message_id == 2
    detail = executor.edits[0][1]
    assert isinstance(detail, RenderedMessage)
    assert "<b>Service</b>" in detail.text
    assert "active: 2" in detail.text
    assert "queued: 3" in detail.text


def test_initial_message_includes_uptime(tmp_path) -> None:
    initial = health._initial_message(_make_ctx(config_path=tmp_path / "untether.toml"))

    assert "uptime:" in initial.text


@pytest.mark.anyio
async def test_handle_keeps_send_failure_from_triggering_second_reply(tmp_path) -> None:
    class FailingExecutor(_Executor):
        async def send(self, message, *, reply_to=None, notify=True):
            self.sent.append((message, reply_to, notify))
            raise OSError("Telegram unavailable")

    executor = FailingExecutor()
    ctx = _make_ctx(config_path=tmp_path / "untether.toml", executor=executor)

    assert await HealthCommand().handle(ctx) is None
    assert len(executor.sent) == 1
    assert executor.edits == []


@pytest.mark.anyio
async def test_handle_isolates_collector_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        health,
        "_collect_system",
        lambda: SystemSnapshot(status="ok", text="RAM: healthy"),
    )
    monkeypatch.setattr(
        health,
        "_collect_process",
        lambda: (_ for _ in ()).throw(OSError("unsupported")),
    )

    monkeypatch.setattr(
        health,
        "_collect_usage",
        lambda _path: UsageSnapshot(status="ok", text="today's API cost: $1.00"),
    )

    ctx = _make_ctx(config_path=tmp_path / "untether.toml")
    await HealthCommand().handle(ctx)

    detail = cast(_Executor, ctx.executor).edits[0][1]
    assert isinstance(detail, RenderedMessage)
    assert "RAM: healthy" in detail.text
    assert "<b>Process</b>\n• unavailable" in detail.text
    assert "today's API cost: $1.00" in detail.text
    assert "process: unavailable" in detail.text
    assert "system: ok" in detail.text
    assert "usage: ok" in detail.text


@pytest.mark.anyio
async def test_handle_emits_sanitized_health_latency_events(tmp_path) -> None:
    from structlog.testing import capture_logs

    ctx = _make_ctx(config_path=tmp_path / "untether.toml")
    with capture_logs() as logs:
        await HealthCommand().handle(ctx)

    events = {record["event"] for record in logs}
    assert {
        "health.initial_send.completed",
        "health.collector.completed",
        "health.detail_collection.completed",
        "health.detail_edit.completed",
    } <= events
    assert all(
        "chat_id" not in record and "message_id" not in record for record in logs
    )


@pytest.mark.anyio
async def test_bounded_collect_propagates_cancellation() -> None:
    started = threading.Event()
    release = threading.Event()
    completed = False

    def blocking_collector() -> SystemSnapshot:
        started.set()
        release.wait()
        return SystemSnapshot(status="ok", text="finished")

    async def collect() -> None:
        nonlocal completed
        await health._bounded_collect(
            blocking_collector,
            SystemSnapshot(status="unavailable", text="unavailable"),
            name="system",
        )
        completed = True

    try:
        async with anyio.create_task_group() as group:
            group.start_soon(collect)
            with anyio.fail_after(1):
                while not started.is_set():
                    await anyio.sleep(0.01)
            group.cancel_scope.cancel()
    finally:
        release.set()

    assert completed is False


def test_render_includes_system_and_triggers(tmp_path) -> None:
    snapshot = render_health_snapshot(_make_ctx(config_path=tmp_path / "untether.toml"))
    assert "Untether health" in snapshot
    # Trigger line always present (even if "none configured" or "disabled")
    assert "triggers" in snapshot


def test_system_collector_includes_usage_and_swap(monkeypatch) -> None:
    monkeypatch.setattr(
        health,
        "_read_meminfo_fields",
        lambda _fields: {
            "MemTotal": 4 * 1024 * 1024,
            "MemAvailable": 2 * 1024 * 1024,
            "SwapTotal": 1024 * 1024,
            "SwapFree": 512 * 1024,
        },
    )

    snapshot = health._collect_system()

    assert snapshot.status == "ok"
    assert "50%" in snapshot.text
    assert "Swap: 512 MB / 1.0 GB" in snapshot.text


def test_render_handles_no_trigger_manager(tmp_path) -> None:
    snapshot = render_health_snapshot(_make_ctx(trigger_manager=None))
    assert "triggers: disabled" in snapshot


def test_render_handles_empty_trigger_manager(tmp_path) -> None:
    mgr = MagicMock()
    mgr.cron_ids.return_value = []
    mgr.webhook_ids.return_value = []
    snapshot = render_health_snapshot(_make_ctx(trigger_manager=mgr))
    assert "triggers: none configured" in snapshot


def test_render_counts_triggers(tmp_path) -> None:
    mgr = MagicMock()
    mgr.cron_ids.return_value = ["daily-review", "weekly-summary"]
    mgr.webhook_ids.return_value = ["deploy"]
    snapshot = render_health_snapshot(_make_ctx(trigger_manager=mgr))
    assert "2 crons" in snapshot
    assert "1 webhook" in snapshot


def test_render_pluralisation_single_cron(tmp_path) -> None:
    mgr = MagicMock()
    mgr.cron_ids.return_value = ["only-one"]
    mgr.webhook_ids.return_value = []
    snapshot = render_health_snapshot(_make_ctx(trigger_manager=mgr))
    assert "1 cron" in snapshot
    assert "1 crons" not in snapshot  # no incorrect pluralisation


def test_format_mb_gb_boundary() -> None:
    assert _format_mb(1024 * 1024) == "1.0 GB"
    assert _format_mb(512 * 1024) == "512 MB"
    assert _format_mb(512) == "512 KB"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux only")
def test_read_meminfo_fields_live() -> None:
    """Live /proc/meminfo read — expect MemTotal + MemAvailable present."""
    mem = _read_meminfo_fields(("MemTotal", "MemAvailable"))
    assert "MemTotal" in mem
    assert "MemAvailable" in mem
    assert mem["MemTotal"] > 0
    assert mem["MemAvailable"] > 0


def test_read_meminfo_fields_returns_empty_on_non_linux(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    assert _read_meminfo_fields(("MemTotal",)) == {}


def test_read_meminfo_fields_handles_missing_file(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    import builtins

    real_open = builtins.open

    def fake_open(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        if isinstance(path, str) and path == "/proc/meminfo":
            raise FileNotFoundError(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)
    assert _read_meminfo_fields(("MemTotal",)) == {}


def test_render_shows_ram_line_on_linux(tmp_path) -> None:
    """On Linux with a healthy host, the RAM line appears with percent used."""
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux only")
    snapshot = render_health_snapshot(_make_ctx(config_path=tmp_path / "untether.toml"))
    assert "RAM:" in snapshot
    # The line should include a percentage figure
    assert "%" in snapshot
