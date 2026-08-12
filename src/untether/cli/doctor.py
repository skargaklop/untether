from __future__ import annotations

import os
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import anyio
import typer

from ..config import ConfigError
from ..engines import list_backend_ids
from ..ids import RESERVED_CHAT_COMMANDS
from ..runtime_loader import resolve_plugins_allowlist
from ..settings import (
    TelegramTopicsSettings,
    TelegramTransportSettings,
    UntetherSettings,
)
from ..telegram.client import TelegramClient
from ..telegram.topics import _validate_topics_setup_for

DoctorStatus = Literal["ok", "warning", "error"]


@dataclass(frozen=True, slots=True)
class DoctorCheck:
    label: str
    status: DoctorStatus
    detail: str | None = None

    def render(self) -> str:
        if self.detail:
            return f"- {self.label}: {self.status} ({self.detail})"
        return f"- {self.label}: {self.status}"


def _doctor_file_checks(settings: UntetherSettings) -> list[DoctorCheck]:
    files = settings.transports.telegram.files
    if not files.enabled:
        return [DoctorCheck("file transfer", "ok", "disabled")]
    if files.allowed_user_ids:
        count = len(files.allowed_user_ids)
        detail = f"restricted to {count} user id(s)"
        return [DoctorCheck("file transfer", "ok", detail)]
    return [DoctorCheck("file transfer", "warning", "enabled for all users")]


def _doctor_voice_checks(settings: UntetherSettings) -> list[DoctorCheck]:
    if not settings.transports.telegram.voice_transcription:
        return [DoctorCheck("voice transcription", "ok", "disabled")]
    telegram = settings.transports.telegram
    providers = telegram.voice_transcription_providers
    checks: list[DoctorCheck] = []
    usable_count = 0

    for provider_id in providers:
        status, detail = _check_voice_provider(telegram, provider_id)
        if status == "ok":
            usable_count += 1
        checks.append(
            DoctorCheck(f"voice transcription [{provider_id}]", status, detail)
        )

    if not checks:
        return [DoctorCheck("voice transcription", "error", "no providers configured")]
    if usable_count == 0:
        return checks + [
            DoctorCheck("voice transcription", "error", "no usable providers")
        ]
    if usable_count < len(providers):
        return checks + [
            DoctorCheck(
                "voice transcription",
                "warning",
                f"{usable_count}/{len(providers)} providers usable",
            )
        ]
    return checks + [
        DoctorCheck(
            "voice transcription",
            "ok",
            f"{usable_count}/{len(providers)} providers usable",
        )
    ]


def _check_voice_provider(
    telegram: TelegramTransportSettings, provider_id: str
) -> tuple[Literal["ok", "error"], str]:
    """Check one voice provider without exposing credentials."""
    if provider_id == "avt":
        command = Path(telegram.voice_transcription_local_command)
        if command.is_file():
            return "ok", f"executable: {command}"
        return "error", f"avt executable not found: {command}"

    if provider_id == "groq":
        if telegram.voice_transcription_groq_api_key:
            return "ok", "groq API key set"
        if os.environ.get("GROQ_API_KEY"):
            return "ok", "GROQ_API_KEY set"
        return "error", "no Groq API key"

    if provider_id == "local":
        from ..telegram.voice_local import local_backend_available

        backend = telegram.voice_transcription_local_backend
        if local_backend_available(backend):
            if backend == "parakeet":
                import shutil

                if not shutil.which("ffmpeg"):
                    return "error", f"{backend} backend available but ffmpeg not found"
            return (
                "ok",
                f"backend={backend}, model={telegram.voice_transcription_local_model}",
            )
        extra = "whisper" if backend == "whisper" else "parakeet"
        return "error", (
            f"{backend} backend not available (install: pip install untether[{extra}])"
        )

    if provider_id == "openai":
        if telegram.voice_transcription_api_key:
            return "ok", "voice_transcription_api_key set"
        for env_key in ("OPENAI_API_KEY", "OPENAI_ADMIN_KEY"):
            if os.environ.get(env_key):
                return "ok", f"{env_key} set"
        return "error", "no OpenAI API key"

    return "error", f"unknown provider: {provider_id}"


async def _doctor_telegram_checks(
    token: str,
    chat_id: int,
    topics: TelegramTopicsSettings,
    project_chat_ids: tuple[int, ...],
) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []

    client_factory = cast(
        Callable[[str], TelegramClient],
        _resolve_cli_attr("TelegramClient") or TelegramClient,
    )
    validate_topics = cast(
        Callable[..., Awaitable[object]],
        _resolve_cli_attr("_validate_topics_setup_for") or _validate_topics_setup_for,
    )
    bot = client_factory(token)
    try:
        me = await bot.get_me()
        if me is None:
            checks.append(
                DoctorCheck("telegram token", "error", "failed to fetch bot info")
            )
            checks.append(DoctorCheck("chat_id", "error", "skipped (token invalid)"))
            if topics.enabled:
                checks.append(DoctorCheck("topics", "error", "skipped (token invalid)"))
            else:
                checks.append(DoctorCheck("topics", "ok", "disabled"))
            return checks
        bot_label = f"@{me.username}" if me.username else f"id={me.id}"
        checks.append(DoctorCheck("telegram token", "ok", bot_label))
        chat = await bot.get_chat(chat_id)
        if chat is None:
            checks.append(DoctorCheck("chat_id", "error", f"unreachable ({chat_id})"))
        else:
            checks.append(DoctorCheck("chat_id", "ok", f"{chat.type} ({chat_id})"))
        if topics.enabled:
            try:
                await validate_topics(
                    bot=bot,
                    topics=topics,
                    chat_id=chat_id,
                    project_chat_ids=project_chat_ids,
                )
                checks.append(DoctorCheck("topics", "ok", f"scope={topics.scope}"))
            except ConfigError as exc:
                checks.append(DoctorCheck("topics", "error", str(exc)))
        else:
            checks.append(DoctorCheck("topics", "ok", "disabled"))
    except Exception as exc:  # noqa: BLE001
        checks.append(DoctorCheck("telegram", "error", str(exc)))
    finally:
        await bot.close()
    return checks


def run_doctor(
    *,
    load_settings_fn: Callable[[], tuple[UntetherSettings, Path]],
    telegram_checks: Callable[
        [str, int, TelegramTopicsSettings, tuple[int, ...]],
        Awaitable[list[DoctorCheck]],
    ],
    file_checks: Callable[[UntetherSettings], list[DoctorCheck]],
    voice_checks: Callable[[UntetherSettings], list[DoctorCheck]],
) -> None:
    try:
        settings, config_path = load_settings_fn()
    except ConfigError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if settings.transport != "telegram":
        typer.echo(
            "error: untether doctor currently supports the telegram transport only.",
            err=True,
        )
        raise typer.Exit(code=1)

    allowlist = resolve_plugins_allowlist(settings)
    engine_ids = list_backend_ids(allowlist=allowlist)
    try:
        projects_cfg = settings.to_projects_config(
            config_path=config_path,
            engine_ids=engine_ids,
            reserved=RESERVED_CHAT_COMMANDS,
        )
    except ConfigError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    tg = settings.transports.telegram
    project_chat_ids = projects_cfg.project_chat_ids()
    telegram_checks_result = anyio.run(
        telegram_checks,
        # #196: unwrap SecretStr at the transport boundary.
        tg.bot_token.get_secret_value(),
        tg.chat_id,
        tg.topics,
        project_chat_ids,
    )
    if telegram_checks_result is None:
        telegram_checks_result = []
    checks = [
        *telegram_checks_result,
        *file_checks(settings),
        *voice_checks(settings),
    ]
    typer.echo("untether doctor")
    for check in checks:
        typer.echo(check.render())
    if any(check.status == "error" for check in checks):
        raise typer.Exit(code=1)


def _resolve_cli_attr(name: str) -> object | None:
    cli_module = sys.modules.get("untether.cli")
    if cli_module is None:
        return None
    return getattr(cli_module, name, None)
