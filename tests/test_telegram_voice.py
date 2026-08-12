from __future__ import annotations

import httpx
import pytest
from openai import APIConnectionError, APITimeoutError, OpenAIError

from untether.telegram.api_models import (
    Chat,
    ChatMember,
    File,
    ForumTopic,
    Message,
    Update,
    User,
)
from untether.telegram.client import BotClient
from untether.telegram.types import TelegramIncomingMessage, TelegramVoice
from untether.telegram.voice import (
    VOICE_TRANSCRIPTION_DISABLED_HINT,
    VOICE_TRANSCRIPTION_UNAVAILABLE,
    transcribe_voice,
)

_REQUEST = httpx.Request("POST", "https://api.groq.com/openai/v1/audio/transcriptions")


class _Bot(BotClient):
    def __init__(self, *, file_info: File | None, audio: bytes | None) -> None:
        self._file_info = file_info
        self._audio = audio

    async def close(self) -> None:
        return None

    async def get_updates(
        self,
        offset: int | None,
        timeout_s: int = 50,
        allowed_updates: list[str] | None = None,
    ) -> list[Update] | None:
        _ = offset, timeout_s, allowed_updates
        return []

    async def get_file(self, file_id: str) -> File | None:
        _ = file_id
        return self._file_info

    async def download_file(self, file_path: str) -> bytes | None:
        _ = file_path
        return self._audio

    async def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        disable_notification: bool | None = False,
        message_thread_id: int | None = None,
        entities: list[dict] | None = None,
        parse_mode: str | None = None,
        reply_markup: dict | None = None,
        *,
        replace_message_id: int | None = None,
    ) -> Message | None:
        _ = (
            reply_to_message_id,
            disable_notification,
            message_thread_id,
            entities,
            parse_mode,
            reply_markup,
            replace_message_id,
        )
        return Message(
            message_id=999,
            chat=Chat(id=chat_id, type="private"),
        )

    async def send_document(
        self,
        chat_id: int,
        filename: str,
        content: bytes,
        reply_to_message_id: int | None = None,
        message_thread_id: int | None = None,
        disable_notification: bool | None = False,
        caption: str | None = None,
    ) -> Message | None:
        _ = (
            chat_id,
            filename,
            content,
            reply_to_message_id,
            message_thread_id,
            disable_notification,
            caption,
        )
        raise AssertionError("send_document should not be called")

    async def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        entities: list[dict] | None = None,
        parse_mode: str | None = None,
        reply_markup: dict | None = None,
        *,
        wait: bool = True,
    ) -> Message | None:
        _ = (
            chat_id,
            message_id,
            text,
            entities,
            parse_mode,
            reply_markup,
            wait,
        )
        raise AssertionError("edit_message_text should not be called")

    async def delete_message(self, chat_id: int, message_id: int) -> bool:
        return True

    async def send_chat_action(self, chat_id: int, action: str = "typing") -> bool:
        return True

    async def set_my_commands(
        self,
        commands: list[dict],
        *,
        scope: dict | None = None,
        language_code: str | None = None,
    ) -> bool:
        _ = commands, scope, language_code
        raise AssertionError("set_my_commands should not be called")

    async def get_me(self) -> User | None:
        raise AssertionError("get_me should not be called")

    async def answer_callback_query(
        self,
        callback_query_id: str,
        text: str | None = None,
        show_alert: bool | None = None,
    ) -> bool:
        _ = callback_query_id, text, show_alert
        raise AssertionError("answer_callback_query should not be called")

    async def get_chat(self, chat_id: int) -> Chat | None:
        _ = chat_id
        raise AssertionError("get_chat should not be called")

    async def get_chat_member(self, chat_id: int, user_id: int) -> ChatMember | None:
        _ = chat_id, user_id
        raise AssertionError("get_chat_member should not be called")

    async def create_forum_topic(self, chat_id: int, name: str) -> ForumTopic | None:
        _ = chat_id, name
        raise AssertionError("create_forum_topic should not be called")

    async def edit_forum_topic(
        self, chat_id: int, message_thread_id: int, name: str
    ) -> bool:
        _ = chat_id, message_thread_id, name
        raise AssertionError("edit_forum_topic should not be called")


def _voice_message(*, file_size: int = 123) -> TelegramIncomingMessage:
    voice = TelegramVoice(
        file_id="voice-id",
        mime_type="audio/ogg",
        file_size=file_size,
        duration=1,
        raw={},
    )
    return TelegramIncomingMessage(
        transport="telegram",
        chat_id=1,
        message_id=1,
        text="",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=1,
        voice=voice,
        raw={},
    )


class _Transcriber:
    def __init__(self, *, result: str | None = None, error: Exception | None = None):
        self.calls: list[tuple[str, bytes]] = []
        self.languages: list[str | None] = []
        self._result = result
        self._error = error

    async def transcribe(
        self, *, model: str, audio_bytes: bytes, language: str | None = None
    ) -> str:
        self.calls.append((model, audio_bytes))
        self.languages.append(language)
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result


class _StatusBot(_Bot):
    """_Bot variant that records status-message lifecycle calls."""

    def __init__(
        self,
        *,
        file_info: File | None,
        audio: bytes | None,
        status_message_id: int = 999,
    ) -> None:
        super().__init__(file_info=file_info, audio=audio)
        self._status_message_id = status_message_id
        self.sent_messages: list[tuple[int, str]] = []
        self.deleted_messages: list[tuple[int, int]] = []
        self.chat_actions: list[tuple[int, str]] = []

    async def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        disable_notification: bool | None = False,
        message_thread_id: int | None = None,
        entities: list[dict] | None = None,
        parse_mode: str | None = None,
        reply_markup: dict | None = None,
        *,
        replace_message_id: int | None = None,
    ) -> Message | None:
        self.sent_messages.append((chat_id, text))
        return Message(
            message_id=self._status_message_id,
            chat=Chat(id=chat_id, type="private"),
        )

    async def delete_message(self, chat_id: int, message_id: int) -> bool:
        self.deleted_messages.append((chat_id, message_id))
        return True

    async def send_chat_action(self, chat_id: int, action: str = "typing") -> bool:
        self.chat_actions.append((chat_id, action))
        return True


@pytest.mark.anyio
async def test_transcribe_voice_status_message_sent_and_deleted_on_success() -> None:
    """When transcribing_status=True, a 'Transcribing…' message is sent
    before transcription and deleted after it succeeds."""
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(result="hello")
    bot = _StatusBot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
        transcribing_status=True,
    )

    assert result == "hello"
    assert len(bot.sent_messages) == 1
    assert bot.sent_messages[0] == (1, "🎙 Transcribing…")
    assert bot.chat_actions == [(1, "typing")]
    assert bot.deleted_messages == [(1, 999)]


@pytest.mark.anyio
async def test_transcribe_voice_status_message_deleted_on_failure() -> None:
    """The status message is deleted even when transcription fails."""
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(error=RuntimeError("provider down"))
    bot = _StatusBot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
        transcribing_status=True,
    )

    assert result is None
    assert len(bot.sent_messages) == 1
    assert bot.deleted_messages == [(1, 999)]


@pytest.mark.anyio
async def test_transcribe_voice_no_status_message_when_disabled() -> None:
    """When transcribing_status=False, no status message or chat action."""
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(result="hello")
    bot = _StatusBot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
        transcribing_status=False,
    )

    assert result == "hello"
    assert bot.sent_messages == []
    assert bot.chat_actions == []
    assert bot.deleted_messages == []


@pytest.mark.anyio
async def test_transcribe_voice_status_send_failure_doesnt_block() -> None:
    """If sending the status message fails (returns None), transcription
    proceeds normally."""
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    class _FailSendBot(_StatusBot):
        async def send_message(
            self,
            chat_id: int,
            text: str,
            reply_to_message_id: int | None = None,
            disable_notification: bool | None = False,
            message_thread_id: int | None = None,
            entities: list[dict] | None = None,
            parse_mode: str | None = None,
            reply_markup: dict | None = None,
            *,
            replace_message_id: int | None = None,
        ) -> Message | None:
            self.sent_messages.append((chat_id, text))
            return None  # Simulate network failure

    transcriber = _Transcriber(result="hello")
    bot = _FailSendBot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
        transcribing_status=True,
    )

    assert result == "hello"
    assert len(bot.sent_messages) == 1
    assert bot.deleted_messages == []


def test_voice_transcribing_status_default() -> None:
    from untether.settings import TelegramTransportSettings

    settings = TelegramTransportSettings(
        bot_token="token",
        chat_id=1,
        allow_any_user=True,
    )
    assert settings.voice_transcribing_status is True


@pytest.mark.anyio
async def test_transcribe_voice_disabled_replies_with_hint() -> None:
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(result="should-not-run")
    result = await transcribe_voice(
        bot=_Bot(file_info=None, audio=None),
        msg=_voice_message(),
        enabled=False,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
    )

    assert result is None
    assert replies[-1] == VOICE_TRANSCRIPTION_DISABLED_HINT
    assert transcriber.calls == []


@pytest.mark.anyio
async def test_transcribe_voice_handles_missing_file() -> None:
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    bot = _Bot(file_info=None, audio=None)
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(),
        enabled=True,
        model="whisper-1",
        reply=reply,
    )

    assert result is None
    assert replies[-1] == "failed to fetch voice file."


@pytest.mark.anyio
async def test_transcribe_voice_handles_missing_download() -> None:
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=None)
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(),
        enabled=True,
        model="whisper-1",
        reply=reply,
    )

    assert result is None
    assert replies[-1] == "failed to download voice file."


@pytest.mark.anyio
async def test_transcribe_voice_rejects_large_voice_without_downloading() -> None:
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    class _NoFetchBot(_Bot):
        async def get_file(self, file_id: str) -> File | None:  # type: ignore[override]
            _ = file_id
            raise AssertionError("get_file should not be called")

        async def download_file(self, file_path: str) -> bytes | None:  # type: ignore[override]
            _ = file_path
            raise AssertionError("download_file should not be called")

    bot = _NoFetchBot(file_info=None, audio=None)
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=10_000),
        enabled=True,
        model="whisper-1",
        max_bytes=100,
        reply=reply,
    )

    assert result is None
    assert replies[-1] == "voice message is too large to transcribe."


@pytest.mark.anyio
async def test_transcribe_voice_rejects_large_download() -> None:
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(result="should-not-run")
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"x" * 200)
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=10),
        enabled=True,
        model="whisper-1",
        max_bytes=100,
        reply=reply,
        transcriber=transcriber,
    )

    assert result is None
    assert replies[-1] == "voice message is too large to transcribe."
    assert transcriber.calls == []


@pytest.mark.anyio
async def test_transcribe_voice_handles_transcriber_error() -> None:
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(error=RuntimeError("boom"))
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
    )

    assert result is None
    assert replies[-1] == VOICE_TRANSCRIPTION_UNAVAILABLE
    assert transcriber.calls


@pytest.mark.anyio
async def test_transcribe_voice_success() -> None:
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(result="transcribed")
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
    )

    assert result == "transcribed"
    assert replies == []
    assert transcriber.calls
    # No language configured → no hint forwarded (auto-detect preserved)
    assert transcriber.languages == [None]


@pytest.mark.anyio
async def test_transcribe_voice_passes_language_hint() -> None:
    """#638: a configured voice_transcription_language is forwarded to the
    transcriber so Whisper-family models stop guessing the language on short
    utterances ('Continue' → '계속')."""
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(result="Continue")
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
        language="en",
    )

    assert result == "Continue"
    assert transcriber.languages == ["en"]


@pytest.mark.anyio
async def test_transcribe_voice_blocks_private_base_url(monkeypatch) -> None:
    """#381: a base_url pointing at a private/reserved address is blocked
    when the openai provider is reached in the chain (transcriber never runs)."""
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        providers=("openai",),
        base_url="http://127.0.0.1:8080/v1",
    )

    assert result is None
    assert replies[-1] == VOICE_TRANSCRIPTION_UNAVAILABLE
    assert len(replies) == 1


@pytest.mark.anyio
async def test_transcribe_voice_allows_allowlisted_base_url() -> None:
    """#381: an explicitly allowlisted private range is permitted."""
    from untether.triggers.ssrf import parse_networks

    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(result="transcribed")
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
        base_url="http://10.0.0.5:9000/v1",
        url_allowlist=parse_networks(["10.0.0.0/8"]),
    )

    assert result == "transcribed"
    assert replies == []
    assert transcriber.calls


@pytest.mark.anyio
async def test_transcribe_voice_connection_error_replies_with_hint() -> None:
    # #584: a transport-level APIConnectionError should surface an actionable
    # transient-network hint, not the opaque "Connection error." string.
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(error=APIConnectionError(request=_REQUEST))
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
    )

    assert result is None
    assert replies[-1] == VOICE_TRANSCRIPTION_UNAVAILABLE
    assert transcriber.calls


@pytest.mark.anyio
async def test_transcribe_voice_timeout_error_replies_with_hint() -> None:
    # #584: APITimeoutError is a subclass of APIConnectionError, so it should
    # take the same transient-network hint path.
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(error=APITimeoutError(_REQUEST))
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
    )

    assert result is None
    assert replies[-1] == VOICE_TRANSCRIPTION_UNAVAILABLE
    assert transcriber.calls


@pytest.mark.anyio
async def test_transcribe_voice_non_connection_openai_error_sanitised() -> None:
    # A non-connection OpenAIError still goes through user_safe_error so we
    # don't regress the #200 sanitisation path.
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(error=OpenAIError("model not found"))
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
    )

    assert result is None
    assert replies[-1] == VOICE_TRANSCRIPTION_UNAVAILABLE
    assert transcriber.calls


@pytest.mark.anyio
async def test_transcribe_voice_stdlib_timeout_branch_reachable() -> None:
    # #584: TimeoutError is a subclass of OSError; the dedicated timeout
    # handler must precede the OSError branch to stay reachable.
    replies: list[str] = []

    async def reply(**kwargs) -> None:
        replies.append(kwargs["text"])

    transcriber = _Transcriber(error=TimeoutError("slow"))
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=2),
        enabled=True,
        model="whisper-1",
        reply=reply,
        transcriber=transcriber,
    )

    assert result is None
    assert replies[-1] == VOICE_TRANSCRIPTION_UNAVAILABLE
    assert transcriber.calls


@pytest.mark.anyio
async def test_594_transcribe_error_log_includes_safe_metadata() -> None:
    from structlog.testing import capture_logs

    async def reply(**kwargs) -> None:
        pass

    transcriber = _Transcriber(error=RuntimeError("secret detail"))
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    with capture_logs() as logs:
        result = await transcribe_voice(
            bot=bot,
            msg=_voice_message(file_size=2),
            enabled=True,
            model="whisper-1",
            reply=reply,
            transcriber=transcriber,
        )
    assert result is None
    rec = next(r for r in logs if r["event"] == "voice.transcribe.error")
    assert rec["error_type"] == "RuntimeError"
    assert "secret detail" not in str(rec)


@pytest.mark.anyio
async def test_594_transcribe_error_log_default_endpoint_marker() -> None:
    """#594: the transcriber-seam error log records the error type without
    leaking error detail. With the chain, the seam path logs
    voice.transcribe.error (no endpoint field since the chain owns routing)."""
    from structlog.testing import capture_logs

    async def reply(**kwargs) -> None:
        pass

    transcriber = _Transcriber(error=OpenAIError("nope"))
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"ok")
    with capture_logs() as logs:
        result = await transcribe_voice(
            bot=bot,
            msg=_voice_message(file_size=2),
            enabled=True,
            model="whisper-1",
            reply=reply,
            transcriber=transcriber,
        )
    assert result is None
    rec = next(r for r in logs if r["event"] == "voice.transcribe.error")
    assert rec["error_type"] == "OpenAIError"
    assert "nope" not in str(rec)


@pytest.mark.anyio
async def test_groq_transcriber_uses_fixed_endpoint_and_model(monkeypatch) -> None:
    from untether.telegram.voice import OpenAIVoiceTranscriber

    calls = []

    class _Audio:
        async def create(self, **kwargs):
            calls.append(kwargs)
            return type("Response", (), {"text": "ok"})()

    class _Client:
        audio = type("Audio", (), {"transcriptions": _Audio()})()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    def fake_openai(**kwargs):
        calls.append({"client": kwargs})
        return _Client()

    monkeypatch.setattr("untether.telegram.voice.AsyncOpenAI", fake_openai)
    transcriber = OpenAIVoiceTranscriber(
        base_url="https://api.groq.com/openai/v1", api_key="secret"
    )
    assert (
        await transcriber.transcribe(model="whisper-large-v3-turbo", audio_bytes=b"x")
        == "ok"
    )
    assert calls[0]["client"]["base_url"] == "https://api.groq.com/openai/v1"
    assert calls[1]["model"] == "whisper-large-v3-turbo"


@pytest.mark.anyio
async def test_avt_transcriber_contract(monkeypatch) -> None:
    from pathlib import Path

    from untether.telegram.voice import AvtVoiceTranscriber

    seen = {}

    class _Stream:
        def __init__(self, chunk: bytes) -> None:
            self._chunk = chunk

        async def receive(self, max_bytes: int) -> bytes:
            chunk, self._chunk = self._chunk[:max_bytes], b""
            return chunk

    class _Process:
        returncode = None
        stdout = _Stream(b'{"transcript":"hello"}')
        stderr = _Stream(b"")

        async def wait(self):
            self.returncode = 0

        def terminate(self):
            self.returncode = 1

        def kill(self):
            self.returncode = 1

    async def fake_open_process(argv, **kwargs):
        seen["argv"] = argv
        seen["file_path"] = argv[4]
        seen["file_bytes"] = Path(argv[4]).read_bytes()
        return _Process()

    monkeypatch.setattr("untether.telegram.voice.anyio.open_process", fake_open_process)
    transcriber = AvtVoiceTranscriber(
        command="avt.exe", backend="whisper", model="base", timeout_s=1
    )
    assert (
        await transcriber.transcribe(model="ignored", audio_bytes=b"ogg-bytes")
        == "hello"
    )
    assert seen["argv"][0:4] == ["avt.exe", "--quiet", "transcribe", "--file"]
    assert seen["file_path"].endswith(".ogg")
    assert seen["file_bytes"] == b"ogg-bytes"
    assert seen["argv"][5:9] == ["--provider", "local", "--local-backend", "whisper"]
    assert seen["argv"][9:11] == ["--local-model", "base"]


@pytest.mark.anyio
async def test_avt_transcriber_nonzero_exit_raises(monkeypatch) -> None:
    """A non-zero AVT exit is surfaced as a bounded, safe RuntimeError."""
    from untether.telegram.voice import AvtVoiceTranscriber

    class _Stream:
        def __init__(self, chunk: bytes) -> None:
            self._chunk = chunk

        async def receive(self, max_bytes: int) -> bytes:
            chunk, self._chunk = self._chunk[:max_bytes], b""
            return chunk

    class _Process:
        returncode = None
        stdout = _Stream(b"")
        stderr = _Stream(b"boom")

        async def wait(self):
            self.returncode = 1

        def terminate(self):
            self.returncode = 1

        def kill(self):
            self.returncode = 1

    async def fake_open_process(argv, **kwargs):
        return _Process()

    monkeypatch.setattr("untether.telegram.voice.anyio.open_process", fake_open_process)
    transcriber = AvtVoiceTranscriber(
        command="avt.exe", backend="whisper", model="base", timeout_s=1
    )
    with pytest.raises(RuntimeError, match="local transcription failed"):
        await transcriber.transcribe(model="ignored", audio_bytes=b"ogg")


@pytest.mark.anyio
async def test_avt_transcriber_timeout_terminates_process(monkeypatch) -> None:
    """A bounded timeout is enforced and the hung process is terminated."""
    import anyio

    from untether.telegram.voice import AvtVoiceTranscriber

    terminated: list[bool] = []

    class _Stream:
        async def receive(self, max_bytes: int) -> bytes:
            return b""

    class _Process:
        returncode = None
        stdout = _Stream()
        stderr = _Stream()

        async def wait(self):
            while self.returncode is None:
                await anyio.sleep(0.001)

        def terminate(self):
            terminated.append(True)
            self.returncode = 1

        def kill(self):
            terminated.append(False)
            self.returncode = 1

    async def fake_open_process(argv, **kwargs):
        return _Process()

    monkeypatch.setattr("untether.telegram.voice.anyio.open_process", fake_open_process)
    transcriber = AvtVoiceTranscriber(
        command="avt.exe", backend="whisper", model="base", timeout_s=0.05
    )
    with pytest.raises(TimeoutError):
        await transcriber.transcribe(model="ignored", audio_bytes=b"ogg")
    assert terminated


@pytest.mark.anyio
async def test_avt_transcriber_invalid_json_raises(monkeypatch) -> None:
    """Non-JSON/empty AVT output is rejected rather than returned as a transcript."""
    from untether.telegram.voice import AvtVoiceTranscriber

    class _Stream:
        def __init__(self, chunk: bytes) -> None:
            self._chunk = chunk

        async def receive(self, max_bytes: int) -> bytes:
            chunk, self._chunk = self._chunk[:max_bytes], b""
            return chunk

    class _Process:
        returncode = None
        stdout = _Stream(b"not json")
        stderr = _Stream(b"")

        async def wait(self):
            self.returncode = 0

        def terminate(self):
            self.returncode = 1

        def kill(self):
            self.returncode = 1

    async def fake_open_process(argv, **kwargs):
        return _Process()

    monkeypatch.setattr("untether.telegram.voice.anyio.open_process", fake_open_process)
    transcriber = AvtVoiceTranscriber(
        command="avt.exe", backend="whisper", model="base", timeout_s=1
    )
    with pytest.raises(ValueError, match="invalid output"):
        await transcriber.transcribe(model="ignored", audio_bytes=b"ogg")


@pytest.mark.anyio
async def test_chain_avt_failure_falls_back_to_groq(monkeypatch) -> None:
    """The configured AvtVoiceTranscriber path fails and the chain falls back to
    Groq without re-downloading the audio."""
    from untether.telegram.voice import transcribe_voice

    tried: list[str] = []

    class _Stream:
        def __init__(self, chunk: bytes) -> None:
            self._chunk = chunk

        async def receive(self, max_bytes: int) -> bytes:
            chunk, self._chunk = self._chunk[:max_bytes], b""
            return chunk

    class _Process:
        returncode = None
        stdout = _Stream(b"")
        stderr = _Stream(b"avt boom")

        async def wait(self):
            self.returncode = 1

        def terminate(self):
            self.returncode = 1

        def kill(self):
            self.returncode = 1

    async def fake_open_process(argv, **kwargs):
        return _Process()

    class _FakeGroq:
        def __init__(self, **kwargs):
            tried.append("groq")

        async def transcribe(self, **kwargs):
            return "groq transcript"

    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setattr("untether.telegram.voice.anyio.open_process", fake_open_process)
    monkeypatch.setattr("untether.telegram.voice_groq.GroqVoiceTranscriber", _FakeGroq)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        providers=("avt", "groq"),
        reply=lambda **_: _done(),
    )
    assert result == "groq transcript"
    assert tried == ["groq"]


@pytest.mark.anyio
async def test_transcribe_voice_selects_fixed_groq_boundary(monkeypatch) -> None:
    from untether.telegram.voice_groq import DEFAULT_GROQ_MODEL

    calls: list[tuple[str, bytes, str | None]] = []

    async def fake_try_provider(**kwargs):
        calls.append((kwargs["model"], kwargs["audio_bytes"], kwargs["language"]))
        assert kwargs["provider_id"] == "groq"
        return "ok"

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try_provider)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="ignored",
        providers=("groq",),
        reply=lambda **_: _done(),
    )
    assert result == "ok"
    assert calls == [("ignored", b"audio", None)]
    assert DEFAULT_GROQ_MODEL == "whisper-large-v3-turbo"


@pytest.mark.anyio
async def test_chain_default_order_tries_each_provider(monkeypatch) -> None:
    """Default providers list tries each provider in order until one succeeds."""
    tried: list[str] = []

    async def fake_try(**kwargs):
        pid = kwargs["provider_id"]
        tried.append(pid)
        if pid == "local":
            return "transcribed by local"
        raise RuntimeError(f"{pid} unavailable")

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        reply=lambda **_: _done(),
    )
    assert result == "transcribed by local"
    assert tried == ["avt", "groq", "local"]


@pytest.mark.anyio
async def test_chain_short_circuit_on_first_success(monkeypatch) -> None:
    tried: list[str] = []

    async def fake_try(**kwargs):
        tried.append(kwargs["provider_id"])
        return "first wins"

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        reply=lambda **_: _done(),
    )
    assert result == "first wins"
    assert tried == ["avt"]


@pytest.mark.anyio
async def test_chain_exhaustion_single_reply(monkeypatch) -> None:
    tried: list[str] = []
    replies: list[str] = []

    async def fake_try(**kwargs):
        tried.append(kwargs["provider_id"])
        raise RuntimeError(f"{kwargs['provider_id']} failed")

    async def reply(**kwargs):
        replies.append(kwargs["text"])

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        reply=reply,
    )
    assert result is None
    assert len(replies) == 1
    assert replies[0] == VOICE_TRANSCRIPTION_UNAVAILABLE
    assert tried == ["avt", "groq", "local", "openai"]


@pytest.mark.anyio
async def test_chain_arbitrary_order(monkeypatch) -> None:
    tried: list[str] = []

    async def fake_try(**kwargs):
        tried.append(kwargs["provider_id"])
        return "ok"

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        providers=("openai", "groq"),
        reply=lambda **_: _done(),
    )
    assert result == "ok"
    assert tried == ["openai"]


@pytest.mark.anyio
async def test_chain_language_propagation(monkeypatch) -> None:
    received_languages: list[str | None] = []

    async def fake_try(**kwargs):
        received_languages.append(kwargs["language"])
        return "ok"

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        language="ja",
        reply=lambda **_: _done(),
    )
    assert received_languages == ["ja"]


@pytest.mark.anyio
async def test_chain_download_once(monkeypatch) -> None:
    download_count = 0

    async def fake_try(**kwargs):
        raise RuntimeError("fail")

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try)

    class _CountingBot(_Bot):
        async def download_file(self, file_path: str) -> bytes | None:
            nonlocal download_count
            download_count += 1
            return await super().download_file(file_path)

    bot = _CountingBot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        reply=lambda **_: _done(),
    )
    assert download_count == 1


@pytest.mark.anyio
async def test_chain_blank_transcript_treated_as_failure(monkeypatch) -> None:
    tried: list[str] = []

    async def fake_try(**kwargs):
        pid = kwargs["provider_id"]
        tried.append(pid)
        if pid == "openai":
            return "real text"
        return "   "

    monkeypatch.setattr("untether.telegram.voice._try_provider", fake_try)
    bot = _Bot(file_info=File(file_path="voice.ogg"), audio=b"audio")
    result = await transcribe_voice(
        bot=bot,
        msg=_voice_message(file_size=5),
        enabled=True,
        model="whisper-1",
        reply=lambda **_: _done(),
    )
    assert result == "real text"
    assert "openai" in tried


@pytest.mark.anyio
async def test_try_provider_groq_missing_key_raises(monkeypatch) -> None:
    from untether.telegram.voice import _try_provider

    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="Groq API key not configured"):
        await _try_provider(
            provider_id="groq",
            audio_bytes=b"audio",
            model="whisper-1",
            language=None,
            base_url=None,
            api_key=None,
            url_allowlist=(),
            groq_api_key=None,
            local_command=None,
            local_backend="whisper",
            local_model="base",
            timeout_s=10,
        )


@pytest.mark.anyio
async def test_try_provider_openai_ssrf_blocked(monkeypatch) -> None:
    from untether.telegram.voice import _try_provider

    with pytest.raises(RuntimeError, match="not permitted"):
        await _try_provider(
            provider_id="openai",
            audio_bytes=b"audio",
            model="whisper-1",
            language=None,
            base_url="http://127.0.0.1:8080/v1",
            api_key="key",
            url_allowlist=(),
            groq_api_key=None,
            local_command=None,
            local_backend="whisper",
            local_model="base",
            timeout_s=10,
        )


@pytest.mark.anyio
async def test_try_provider_local_dispatch(monkeypatch) -> None:
    """The local provider dispatches to get_local_transcriber."""
    import untether.telegram.voice_local as vlocal
    from untether.telegram.voice import _try_provider

    class _FakeLocal:
        async def transcribe(self, **kwargs):
            return "local result"

    monkeypatch.setattr(vlocal, "get_local_transcriber", lambda *a, **kw: _FakeLocal())
    result = await _try_provider(
        provider_id="local",
        audio_bytes=b"audio",
        model="base",
        language="en",
        base_url=None,
        api_key=None,
        url_allowlist=(),
        groq_api_key=None,
        local_command=None,
        local_backend="whisper",
        local_model="base",
        timeout_s=10,
    )
    assert result == "local result"


async def _done() -> None:
    return None
