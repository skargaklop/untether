from __future__ import annotations

import io
import ipaddress
import json
import os
import subprocess
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Protocol

import anyio
from openai import APIConnectionError, AsyncOpenAI, OpenAIError

from ..logging import get_logger
from ..triggers.ssrf import SSRFError, validate_url_with_dns
from ..utils.error_display import user_safe_error
from .client import BotClient
from .types import TelegramIncomingMessage

logger = get_logger(__name__)

__all__ = ["transcribe_voice", "OpenAIVoiceTranscriber", "AvtVoiceTranscriber"]

VOICE_TRANSCRIPTION_DISABLED_HINT = (
    "voice transcription is disabled. enable it in config:\n"
    "```toml\n"
    "[transports.telegram]\n"
    "voice_transcription = true\n"
    "```"
)
VOICE_TRANSCRIPTION_CONNECTION_HINT = (
    "couldn't reach the transcription service — transient network issue. "
    "please resend the voice note, or type your message instead."
)
_VOICE_MAX_RETRIES = 4
_GROQ_BASE_URL = "https://api.groq.com/openai/v1"
_GROQ_MODEL = "whisper-large-v3-turbo"
_AVT_OUTPUT_LIMIT = 64 * 1024


class VoiceTranscriber(Protocol):
    async def transcribe(
        self, *, model: str, audio_bytes: bytes, language: str | None = None
    ) -> str: ...


class OpenAIVoiceTranscriber:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
    ) -> None:
        self._base_url = base_url
        self._api_key = api_key

    async def transcribe(
        self, *, model: str, audio_bytes: bytes, language: str | None = None
    ) -> str:
        audio_file = io.BytesIO(audio_bytes)
        audio_file.name = "voice.ogg"
        async with AsyncOpenAI(
            base_url=self._base_url,
            api_key=self._api_key,
            timeout=120,
            max_retries=_VOICE_MAX_RETRIES,
        ) as client:
            kwargs: dict[str, object] = {"model": model, "file": audio_file}
            if language is not None:
                kwargs["language"] = language
            response = await client.audio.transcriptions.create(**kwargs)
        return response.text


class AvtVoiceTranscriber:
    def __init__(
        self, *, command: str, backend: str, model: str, timeout_s: float
    ) -> None:
        self._command = command
        self._backend = backend
        self._model = model
        self._timeout_s = timeout_s

    async def transcribe(
        self, *, model: str, audio_bytes: bytes, language: str | None = None
    ) -> str:
        _ = model, language
        path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as file:
                path = Path(file.name)
                file.write(audio_bytes)
            argv = [
                self._command,
                "--quiet",
                "transcribe",
                "--file",
                os.fspath(path),
                "--provider",
                "local",
                "--local-backend",
                self._backend,
            ]
            if self._backend == "whisper":
                argv.extend(["--local-model", self._model])
            proc = await anyio.open_process(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if proc.stdout is None or proc.stderr is None:
                raise RuntimeError("local transcription failed to open pipes")
            stdout = bytearray()
            stderr = bytearray()

            async def capture(stream, output: bytearray) -> None:
                while True:
                    chunk = await stream.receive(_AVT_OUTPUT_LIMIT - len(output) + 1)
                    if not chunk:
                        return
                    if len(output) + len(chunk) > _AVT_OUTPUT_LIMIT:
                        proc.terminate()
                        raise ValueError("local transcription output exceeded limit")
                    output.extend(chunk)

            try:
                with anyio.fail_after(self._timeout_s):
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(capture, proc.stdout, stdout)
                        tg.start_soon(capture, proc.stderr, stderr)
                        await proc.wait()
            finally:
                if proc.returncode is None:
                    with anyio.CancelScope(shield=True):
                        proc.terminate()
                        with anyio.move_on_after(5):
                            await proc.wait()
                        if proc.returncode is None:
                            proc.kill()
                            await proc.wait()
            if proc.returncode != 0:
                raise RuntimeError("local transcription failed")
            try:
                payload = json.loads(bytes(stdout))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("local transcription returned invalid output") from exc
            transcript = payload.get("transcript") if isinstance(payload, dict) else None
            if not isinstance(transcript, str) or not transcript.strip():
                raise ValueError("local transcription returned invalid output")
            return transcript
        finally:
            if path is not None:
                path.unlink(missing_ok=True)


async def transcribe_voice(
    *,
    bot: BotClient,
    msg: TelegramIncomingMessage,
    enabled: bool,
    model: str,
    max_bytes: int | None = None,
    reply: Callable[..., Awaitable[None]],
    transcriber: VoiceTranscriber | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    url_allowlist: Sequence[ipaddress.IPv4Network | ipaddress.IPv6Network] = (),
    language: str | None = None,
    provider: str = "openai",
    groq_api_key: str | None = None,
    local_command: str | None = None,
    local_backend: str = "whisper",
    local_model: str = "base",
    timeout_s: float = 180.0,
) -> str | None:
    voice = msg.voice
    if voice is None:
        return msg.text
    if not enabled:
        await reply(text=VOICE_TRANSCRIPTION_DISABLED_HINT)
        return None
    if (
        max_bytes is not None
        and voice.file_size is not None
        and voice.file_size > max_bytes
    ):
        await reply(text="voice message is too large to transcribe.")
        return None
    file_info = await bot.get_file(voice.file_id)
    if file_info is None:
        logger.warning("voice.file_info.failed", file_id=voice.file_id, file_size=voice.file_size)
        await reply(text="failed to fetch voice file.")
        return None
    audio_bytes = await bot.download_file(file_info.file_path)
    if audio_bytes is None:
        logger.warning(
            "voice.download.failed",
            file_id=voice.file_id,
            file_size=voice.file_size,
            file_path=file_info.file_path,
        )
        await reply(text="failed to download voice file.")
        return None
    if max_bytes is not None and len(audio_bytes) > max_bytes:
        await reply(text="voice message is too large to transcribe.")
        return None
    if provider == "openai" and base_url is not None:
        try:
            await validate_url_with_dns(base_url, allowlist=url_allowlist)
        except SSRFError as exc:
            logger.error(
                "voice.base_url.ssrf_blocked",
                error=str(exc),
                error_type=exc.__class__.__name__,
            )
            await reply(text="voice transcription endpoint is not permitted.")
            return None
    endpoint = base_url or "openai-default"
    actual_model = _GROQ_MODEL if provider == "groq" else model
    if transcriber is None:
        if provider == "groq":
            transcriber = OpenAIVoiceTranscriber(
                base_url=_GROQ_BASE_URL, api_key=groq_api_key
            )
            endpoint = _GROQ_BASE_URL
        elif provider == "local":
            transcriber = AvtVoiceTranscriber(
                command=local_command or "avt.exe",
                backend=local_backend,
                model=local_model,
                timeout_s=timeout_s,
            )
            actual_model = local_model
            endpoint = "local-avt"
        else:
            transcriber = OpenAIVoiceTranscriber(base_url=base_url, api_key=api_key)
    try:
        text = await transcriber.transcribe(
            model=actual_model, audio_bytes=audio_bytes, language=language
        )
        logger.debug(
            "voice.transcribe.success",
            model=actual_model,
            language=language,
            audio_size=len(audio_bytes),
        )
        return text
    except OpenAIError as exc:
        logger.error(
            "openai.transcribe.error",
            error=str(exc),
            error_type=exc.__class__.__name__,
            cause=repr(exc.__cause__) if exc.__cause__ is not None else None,
            endpoint=endpoint,
            file_id=voice.file_id,
            file_size=voice.file_size,
        )
        if isinstance(exc, APIConnectionError):
            await reply(text=VOICE_TRANSCRIPTION_CONNECTION_HINT)
            return None
        await reply(text=user_safe_error(exc, fallback="voice transcription failed"))
        return None
    except TimeoutError as exc:
        logger.error(
            "voice.transcribe.timeout",
            error=str(exc),
            endpoint=endpoint,
            file_id=voice.file_id,
            file_size=voice.file_size,
        )
        await reply(text="voice transcription timed out")
        return None
    except (RuntimeError, OSError, ValueError) as exc:
        logger.error(
            "voice.transcribe.error",
            error=str(exc),
            error_type=exc.__class__.__name__,
            endpoint=endpoint,
            file_id=voice.file_id,
            file_size=voice.file_size,
        )
        await reply(text=user_safe_error(exc, fallback="voice transcription failed"))
        return None
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "voice.transcribe.unexpected",
            error=str(exc),
            error_type=exc.__class__.__name__,
            endpoint=endpoint,
            file_id=voice.file_id,
            file_size=voice.file_size,
        )
        await reply(text=user_safe_error(exc, fallback="voice transcription failed"))
        return None
