"""Opt-in real OMP → Pi handoff smoke with disposable stores and fake Telegram."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path
from typing import cast
from uuid import uuid4

import anyio
import msgspec
import pytest

from tests.telegram_fakes import FakeTransport, make_cfg
from untether.config import ProjectConfig, ProjectsConfig
from untether.model import CompletedEvent
from untether.router import AutoRouter, RunnerEntry
from untether.runner import Runner
from untether.runners.omp import OmpRunner
from untether.runners.pi import PiRunner, _default_pi_cmd
from untether.runners.run_options import EngineRunOptions, apply_run_options
from untether.settings import TelegramTopicsSettings
from untether.telegram.api_schemas import Update
from untether.telegram.chat_prefs import ChatPrefsStore, resolve_prefs_path
from untether.telegram.engine_overrides import EngineOverrides
from untether.telegram.loop import run_main_loop
from untether.telegram.parsing import parse_incoming_update
from untether.telegram.topic_state import TopicStateStore, resolve_state_path
from untether.telegram.types import TelegramCallbackQuery, TelegramIncomingMessage
from untether.transport_runtime import TransportRuntime
from untether.utils.paths import reset_run_base_dir, set_run_base_dir

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(
        os.environ.get("UNTETHER_HANDOFF_LIVE_SMOKE") != "1",
        reason="opt-in real model smoke; requires installed authenticated OMP and Pi",
    ),
]


async def _complete(runner, prompt, resume, model, cwd=None) -> CompletedEvent:
    final = None
    from untether.utils.paths import reset_run_base_dir, set_run_base_dir

    token = set_run_base_dir(cwd) if cwd is not None else None
    with apply_run_options(EngineRunOptions(model=model)):
        with anyio.fail_after(270):
            async for event in runner.run(prompt, resume):
                if isinstance(event, CompletedEvent):
                    final = event
    assert final is not None and final.ok and final.resume is not None, repr(final)
    if token is not None:
        reset_run_base_dir(token)
    return final


async def test_real_omp_to_pi_handoff_and_model_followup(tmp_path: Path) -> None:
    """Real binaries and models; simulated transport; never use a user session.

    Exercises JSON callback decoding, scoped model preferences, scheduler,
    source summary, destination seed, durable topic route and marker recall.
    Extensions/MCP/tools are disabled where supported to keep the blast radius
    to disposable stores. Provider access uses existing runtime configuration.
    """
    omp_store = tmp_path / "omp-sessions"
    pi_store = tmp_path / "pi-sessions"
    omp_store.mkdir()
    pi_store.mkdir()
    source_model = os.environ.get(
        "UNTETHER_SMOKE_SOURCE_MODEL", "omniroute/flash-combined"
    )
    destination_model = os.environ.get(
        "UNTETHER_SMOKE_DESTINATION_MODEL", "omniroute/flash-combined"
    )
    marker = "handoff-memory-" + uuid4().hex

    class DisposablePi(PiRunner):
        def _new_session_path(self) -> str:
            return str(pi_store / (uuid4().hex + ".jsonl"))

    omp = OmpRunner(
        extra_args=[
            "--session-dir",
            str(omp_store),
            "--no-extensions",
            "--no-skills",
            "--no-rules",
            "--no-context-files",
            "--no-title",
            "--no-lsp",
            "--no-pty",
            "--no-prewalk",
            "--no-tools",
            "--thinking",
            "off",
            "--max-time",
            "240",
        ],
        model=None,
        provider=None,
    )
    pi = DisposablePi(
        pi_cmd=_default_pi_cmd(),
        extra_args=[
            "--session-dir",
            str(pi_store),
            "--no-extensions",
            "--no-mcp",
            "--no-skills",
            "--no-context-files",
            "--no-prompt-templates",
            "--no-themes",
            "--no-tools",
        ],
        model=None,
        provider=None,
    )
    base_token = set_run_base_dir(tmp_path)
    try:
        seed = await _complete(
            omp,
            f"Disposable smoke test. Remember this exact marker: {marker}. "
            "The next agent must retain it. Reply with that marker only; no tools.",
            None,
            source_model,
        )
        assert marker in seed.answer
        source = seed.resume
        assert source is not None
        source_files = list(omp_store.rglob("*.jsonl"))
        assert len(source_files) == 1
        source_file = source_files[0]
        entries = [
            json.loads(line)
            for line in source_file.read_text(encoding="utf-8-sig").splitlines()
        ]
        changes = [entry for entry in entries if entry.get("type") == "model_change"]
        assert changes, "source must contain a real stored model selection"
        for entry in changes:
            entry["provider"] = "omniroute"
            entry["modelId"] = "retired-handoff-smoke-model"
        source_file.write_text(
            "\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries)
            + "\n",
            encoding="utf-8",
        )
        print(
            "SMOKE: real OMP source seeded; disposable stored model retired", flush=True
        )
        # Match production: the executor resolves project cwd after this
        # point, so the destination session inherits the harness cwd, not
        # tmp_path. Release our override so seed and follow-up share it.
        reset_run_base_dir(base_token)
        base_token = None
        config_path = tmp_path / "untether.toml"
        prefs = ChatPrefsStore(resolve_prefs_path(config_path))
        await prefs.set_engine_override(
            123, "omp", EngineOverrides(model="retired-chat-override-smoke-model")
        )
        await prefs.set_engine_override(
            123, "pi", EngineOverrides(model=destination_model)
        )
        topics = TopicStateStore(resolve_state_path(config_path))
        await topics.set_engine_override(
            123, 77, "omp", EngineOverrides(model=source_model)
        )
        await topics.set_session_resume(123, 77, source)
        runtime = TransportRuntime(
            router=AutoRouter(
                entries=[
                    RunnerEntry(engine="omp", runner=omp),
                    RunnerEntry(engine="pi", runner=cast(Runner, pi)),
                ],
                default_engine="omp",
            ),
            projects=ProjectsConfig(
                projects={
                    "smoke": ProjectConfig(
                        alias="smoke",
                        path=tmp_path,
                        worktrees_dir=Path(".worktrees"),
                        chat_id=123,
                    )
                },
                chat_map={123: "smoke"},
            ),
            config_path=config_path,
        )
        transport = FakeTransport()
        cfg = replace(
            make_cfg(transport),
            runtime=runtime,
            allowed_user_ids=(123,),
            topics=TelegramTopicsSettings(enabled=True, scope="main"),
        )

        async def poller(
            _cfg,
        ) -> AsyncIterator[TelegramIncomingMessage | TelegramCallbackQuery]:
            yield TelegramIncomingMessage(
                transport="telegram",
                chat_id=123,
                message_id=1,
                text="/handoff to pi Preserve the exact marker in one sentence only.",
                reply_to_message_id=99,
                reply_to_text=omp.format_resume(source),
                sender_id=123,
                thread_id=77,
            )
            with anyio.fail_after(15):
                while not any(
                    call["message"].extra.get("reply_markup", {}).get("inline_keyboard")
                    for call in transport.send_calls
                ):
                    await anyio.sleep(0.05)
            call = next(
                call
                for call in transport.send_calls
                if call["message"].extra.get("reply_markup", {}).get("inline_keyboard")
            )
            data = call["message"].extra["reply_markup"]["inline_keyboard"][0][0][
                "callback_data"
            ]
            update = msgspec.convert(
                {
                    "update_id": 2,
                    "callback_query": {
                        "id": "smoke",
                        "from": {"id": 123},
                        "data": data,
                        "message": {
                            "message_id": call["ref"].message_id,
                            "chat": {"id": 123, "type": "supergroup", "is_forum": True},
                            "message_thread_id": 77,
                        },
                    },
                },
                type=Update,
            )
            callback = parse_incoming_update(update, chat_id=123)
            assert isinstance(callback, TelegramCallbackQuery)
            yield callback
            with anyio.fail_after(330):
                confirm_ref = call["ref"]
                while not any(
                    edit["message"].text.startswith("completed — new session started")
                    for edit in transport.edit_calls
                ):
                    failures = [
                        edit["message"].text
                        for edit in transport.edit_calls
                        if edit["ref"] == confirm_ref
                        and not edit["message"].text.startswith(
                            (
                                "creating handoff summary",
                                "running summary",
                                "seeding destination",
                                "completed — new session started",
                            )
                        )
                    ]
                    assert not failures, failures
                    await anyio.sleep(0.1)
            print("SMOKE: handoff card terminal success", flush=True)
            await anyio.sleep(0.1)

        with anyio.fail_after(350):
            await run_main_loop(cfg, poller)
        stored = TopicStateStore(resolve_state_path(config_path))
        destination = await stored.get_session_resume(123, 77, "pi")
        assert destination is not None and destination.engine == "pi"
        assert await stored.get_session_resume(123, 77, "omp") == source
        result = await _complete(
            pi,
            "Reply with only the exact handoff-memory marker from your previous handoff summary. Do not invent a marker.",
            destination,
            destination_model,
        )
        assert marker in result.answer
        files = list(pi_store.glob("*.jsonl"))
        assert len(files) == 1
        assert marker in files[0].read_text(encoding="utf-8-sig")
        report = {
            "ok": True,
            "source": source.value,
            "destination": destination.value,
            "source_model": source_model,
            "destination_model": destination_model,
            "marker": marker,
            "transport": "simulated Telegram; real OMP and Pi",
            "checks": [
                "retired model override",
                "decoded topic callback",
                "source summary",
                "destination seed",
                "durable topic route",
                "model marker recall",
            ],
        }
        (tmp_path / "report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        print(f"SMOKE_PASS: {tmp_path / 'report.json'}", flush=True)
    finally:
        if base_token is not None:
            reset_run_base_dir(base_token)
