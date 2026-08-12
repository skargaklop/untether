from __future__ import annotations

# ruff: noqa: I001
# Adapted from AI-Video-Transcriber (Apache-2.0) backend/groq_transcriber.py.
# Modified by Untether: native anyio integration, plain-text output, Untether user agent.
# See docs/ATTRIBUTION.md or ATTRIBUTION.md for third-party license details.


import json
import mimetypes
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio

from .. import __version__
from ..logging import get_logger

logger = get_logger(__name__)

GROQ_TRANSCRIPTIONS_ENDPOINT = "https://api.groq.com/openai/v1/audio/transcriptions"
DEFAULT_GROQ_MODEL = "whisper-large-v3-turbo"


class GroqTranscriptionError(Exception):
    """Raised when a Groq transcription cannot be completed."""


@dataclass(frozen=True)
class MultipartFile:
    filename: str
    content: bytes
    content_type: str = "application/octet-stream"


def prepare_groq_payload(
    audio_url: str = "",
    model: str = DEFAULT_GROQ_MODEL,
    language: str = "",
    prompt: str = "",
) -> dict[str, Any]:
    """Build the fields used by Groq's audio transcription endpoint."""
    payload: dict[str, Any] = {
        "model": model or DEFAULT_GROQ_MODEL,
        "response_format": "verbose_json",
        "temperature": "0",
        "timestamp_granularities[]": ["segment"],
    }
    if audio_url:
        payload["url"] = audio_url
    normalized_language = language.strip()
    if normalized_language and normalized_language.lower() not in {
        "auto",
        "auto-detect",
        "autodetect",
        "detect",
    }:
        payload["language"] = normalized_language
    if prompt.strip():
        payload["prompt"] = prompt.strip()
    return payload


def prepare_groq_file_payload(
    audio_file: str | Path,
    model: str = DEFAULT_GROQ_MODEL,
    language: str = "",
    prompt: str = "",
) -> dict[str, Any]:
    """Build multipart fields for a file on disk."""
    path = Path(audio_file)
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    payload = prepare_groq_payload(model=model, language=language, prompt=prompt)
    payload["file"] = MultipartFile(path.name, path.read_bytes(), content_type)
    return payload


def build_multipart_form_data(
    payload: dict[str, Any], boundary: str | None = None
) -> tuple[bytes, str]:
    """Encode fields and files as a multipart/form-data request body."""
    boundary = boundary or f"----UntetherGroq{uuid.uuid4().hex}"
    parts: list[bytes] = []
    for name, value in payload.items():
        values = value if isinstance(value, list) else [value]
        for item in values:
            parts.append(f"--{boundary}\r\n".encode())
            if isinstance(item, MultipartFile):
                parts.append(
                    f'Content-Disposition: form-data; name="{name}"; filename="{item.filename}"\r\n'.encode()
                )
                parts.append(f"Content-Type: {item.content_type}\r\n\r\n".encode())
                parts.append(item.content)
            else:
                parts.append(
                    f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
                )
                parts.append(str(item).encode())
            parts.append(b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


class GroqVoiceTranscriber:
    def __init__(
        self,
        *,
        api_key: str,
        model: str = DEFAULT_GROQ_MODEL,
        timeout_s: float = 180.0,
    ) -> None:
        self.api_key = api_key.strip()
        if not self.api_key:
            raise GroqTranscriptionError("Groq API key is required")
        self.model = model or DEFAULT_GROQ_MODEL
        self.timeout_s = timeout_s

    async def transcribe(
        self,
        *,
        model: str,
        audio_bytes: bytes,
        language: str | None = None,
    ) -> str:
        payload = prepare_groq_payload(
            model=model or self.model,
            language=language or "",
        )
        payload["file"] = MultipartFile("audio.ogg", audio_bytes, "audio/ogg")
        data = await anyio.to_thread.run_sync(  # ty: ignore[unresolved-attribute]
            self._post, payload
        )
        if not isinstance(data, dict):
            raise GroqTranscriptionError("Groq returned a malformed response")
        text = data.get("text")
        if not isinstance(text, str) or not text.strip():
            raise GroqTranscriptionError("Groq returned empty transcript")
        return text.strip()

    def _post(self, payload: dict[str, Any]) -> Any:
        encoded, content_type = build_multipart_form_data(payload)
        request = urllib.request.Request(
            GROQ_TRANSCRIPTIONS_ENDPOINT,
            data=encoded,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": content_type,
                "Accept": "application/json",
                "User-Agent": f"untether/{__version__}",
            },
        )
        try:
            # Endpoint is a fixed HTTPS constant; request URL is never
            # user-controlled, so there is no SSRF surface here (B310).
            with urllib.request.urlopen(  # nosec B310
                request, timeout=self.timeout_s
            ) as response:
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            message = self._extract_error(body) or f"Groq API error: HTTP {exc.code}"
            raise GroqTranscriptionError(message) from exc
        except urllib.error.URLError as exc:
            raise GroqTranscriptionError(
                f"Groq API request failed: {exc.reason}"
            ) from exc
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise GroqTranscriptionError("Groq returned a malformed response") from exc

    @staticmethod
    def _extract_error(body: str) -> str:
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return body[:500]
        error = data.get("error") if isinstance(data, dict) else None
        if isinstance(error, dict):
            return str(error.get("message") or error)
        if error:
            return str(error)
        return str(data)[:500]
