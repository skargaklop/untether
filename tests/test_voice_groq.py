from __future__ import annotations

import io
import urllib.error
import urllib.request
from email.message import Message
from typing import cast

import pytest

from untether.telegram.voice_groq import (
    GroqTranscriptionError,
    GroqVoiceTranscriber,
    build_multipart_form_data,
    prepare_groq_payload,
)


class _Response:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.body


@pytest.mark.anyio
async def test_groq_transcriber_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *args, **kwargs: _Response(b'{"text":"hello world"}'),
    )
    result = await GroqVoiceTranscriber(api_key="secret").transcribe(
        model="whisper-large-v3-turbo", audio_bytes=b"audio"
    )
    assert result == "hello world"


@pytest.mark.anyio
async def test_groq_transcriber_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise urllib.error.HTTPError(
            "https://example.test",
            401,
            "no",
            Message(),
            io.BytesIO(b'{"error":{"message":"bad key"}}'),
        )

    monkeypatch.setattr("urllib.request.urlopen", fail)
    with pytest.raises(GroqTranscriptionError, match="bad key"):
        await GroqVoiceTranscriber(api_key="secret").transcribe(
            model="m", audio_bytes=b"audio"
        )


@pytest.mark.anyio
async def test_groq_transcriber_url_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise urllib.error.URLError("offline")

    monkeypatch.setattr("urllib.request.urlopen", fail)
    with pytest.raises(GroqTranscriptionError, match="offline"):
        await GroqVoiceTranscriber(api_key="secret").transcribe(
            model="m", audio_bytes=b"audio"
        )


@pytest.mark.anyio
async def test_groq_transcriber_malformed_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "urllib.request.urlopen", lambda *a, **k: _Response(b"not json")
    )
    with pytest.raises(GroqTranscriptionError, match="malformed"):
        await GroqVoiceTranscriber(api_key="secret").transcribe(
            model="m", audio_bytes=b"audio"
        )


@pytest.mark.anyio
async def test_groq_transcriber_empty_transcript(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "urllib.request.urlopen", lambda *a, **k: _Response(b'{"text":""}')
    )
    with pytest.raises(GroqTranscriptionError, match="empty transcript"):
        await GroqVoiceTranscriber(api_key="secret").transcribe(
            model="m", audio_bytes=b"audio"
        )


@pytest.mark.anyio
async def test_groq_transcriber_with_language(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[urllib.request.Request] = []

    def capture(request: urllib.request.Request, **kwargs: object) -> _Response:
        seen.append(request)
        return _Response(b'{"text":"ok"}')

    monkeypatch.setattr("urllib.request.urlopen", capture)
    await GroqVoiceTranscriber(api_key="secret").transcribe(
        model="m", audio_bytes=b"audio", language="en"
    )
    request = seen[0]
    data = cast(bytes, request.data)
    assert data and b'name="language"' in data and b"en" in data


def test_groq_transcriber_auto_language_normalized() -> None:
    assert "language" not in prepare_groq_payload(language="auto")


def test_groq_transcriber_multipart_format() -> None:
    body, content_type = build_multipart_form_data(
        {"model": "m"}, boundary="----UntetherGroqabc"
    )
    assert content_type == "multipart/form-data; boundary=----UntetherGroqabc"
    assert body.startswith(b"------UntetherGroqabc")
    assert body.endswith(b"------UntetherGroqabc--\r\n")


@pytest.mark.anyio
async def test_groq_transcriber_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[urllib.request.Request] = []

    def capture(request: urllib.request.Request, **kwargs: object) -> _Response:
        seen.append(request)
        return _Response(b'{"text":"ok"}')

    monkeypatch.setattr("urllib.request.urlopen", capture)
    await GroqVoiceTranscriber(api_key="secret").transcribe(
        model="m", audio_bytes=b"audio"
    )
    user_agent = seen[0].get_header("User-agent")
    assert user_agent is not None
    assert user_agent.startswith("untether/")


def test_groq_transcriber_empty_api_key_raises() -> None:
    with pytest.raises(GroqTranscriptionError, match="API key is required"):
        GroqVoiceTranscriber(api_key=" ")


def test_groq_transcriber_credential_not_logged(
    capsys: pytest.CaptureFixture[str],
) -> None:
    GroqVoiceTranscriber(api_key="super-secret-key")
    assert "super-secret-key" not in capsys.readouterr().out
