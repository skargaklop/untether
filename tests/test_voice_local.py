"""Tests for the native local Whisper/Parakeet voice transcription adapter.

Adapted from AI-Video-Transcriber (Apache-2.0) test patterns.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from untether.telegram.voice_local import (
    DEFAULT_PARAKEET_MODEL,
    DEFAULT_WHISPER_MODEL,
    PARAKEET_MODEL_NAME_MAP,
    LocalTranscriptionError,
    LocalVoiceTranscriber,
    ensure_backend_audio_file,
    get_local_transcriber,
    local_backend_available,
    resolve_local_model_id,
)

# ── Model resolution ──────────────────────────────────────────────────


class TestResolveLocalModelId:
    def test_whisper_default(self) -> None:
        assert resolve_local_model_id("whisper") == DEFAULT_WHISPER_MODEL

    def test_parakeet_default(self) -> None:
        assert resolve_local_model_id("parakeet") == DEFAULT_PARAKEET_MODEL

    def test_preset_override(self) -> None:
        assert resolve_local_model_id("whisper", "small") == "small"

    def test_custom_model_id(self) -> None:
        assert resolve_local_model_id("whisper", "custom", "large-v3") == "large-v3"

    def test_custom_with_custom_preset(self) -> None:
        result = resolve_local_model_id("parakeet", "custom", "my-model")
        assert result == "my-model"

    def test_empty_backend_defaults_to_whisper(self) -> None:
        assert resolve_local_model_id("", "tiny") == "tiny"

    def test_preset_only_no_custom(self) -> None:
        assert resolve_local_model_id("whisper", "medium") == "medium"

    def test_custom_without_preset(self) -> None:
        assert resolve_local_model_id("whisper", "", "large-v3") == "large-v3"


# ── Parakeet model name mapping ───────────────────────────────────────


class TestParakeetModelNameMap:
    def test_v3_maps_to_nemo(self) -> None:
        assert PARAKEET_MODEL_NAME_MAP["nvidia/parakeet-tdt-0.6b-v3"] == (
            "nemo-parakeet-tdt-0.6b-v3"
        )

    def test_v2_maps_to_nemo(self) -> None:
        assert PARAKEET_MODEL_NAME_MAP["nvidia/parakeet-tdt-0.6b-v2"] == (
            "nemo-parakeet-tdt-0.6b-v2"
        )


# ── Dependency availability ───────────────────────────────────────────


class TestDependencyAvailability:
    def test_whisper_not_available(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: None if name == "faster_whisper" else object(),
        )
        assert local_backend_available("whisper") is False

    def test_whisper_available(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: object(),
        )
        assert local_backend_available("whisper") is True

    def test_parakeet_not_available(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: None,
        )
        assert local_backend_available("parakeet") is False

    def test_parakeet_partial_not_available(self, monkeypatch) -> None:
        """onnx_asr present but onnxruntime missing → not available."""

        def fake_find(name):
            return object() if name == "onnx_asr" else None

        monkeypatch.setattr("untether.telegram.voice_local._find_spec_safe", fake_find)
        assert local_backend_available("parakeet") is False

    def test_parakeet_fully_available(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: object(),
        )
        assert local_backend_available("parakeet") is True

    def test_unknown_backend_not_available(self) -> None:
        assert local_backend_available("unknown") is False


# ── Cache singleton ───────────────────────────────────────────────────


class TestCacheSingleton:
    def test_same_key_returns_same_instance(self) -> None:
        # Clear cache to isolate
        from untether.telegram import voice_local

        voice_local._LOCAL_CACHE.clear()
        a = get_local_transcriber("whisper", "base")
        b = get_local_transcriber("whisper", "base")
        assert a is b

    def test_different_backend_different_instance(self) -> None:
        from untether.telegram import voice_local

        voice_local._LOCAL_CACHE.clear()
        a = get_local_transcriber("whisper", "base")
        b = get_local_transcriber("parakeet", "nvidia/parakeet-tdt-0.6b-v3")
        assert a is not b

    def test_different_model_different_instance(self) -> None:
        from untether.telegram import voice_local

        voice_local._LOCAL_CACHE.clear()
        a = get_local_transcriber("whisper", "base")
        b = get_local_transcriber("whisper", "small")
        assert a is not b


# ── Constructor validation ────────────────────────────────────────────


class TestConstructor:
    def test_unsupported_backend_raises(self) -> None:
        with pytest.raises(LocalTranscriptionError, match="Unsupported"):
            LocalVoiceTranscriber(backend="wav2vec", model="base")

    def test_backend_normalized_to_lower(self) -> None:
        t = LocalVoiceTranscriber(backend="WHISPER", model="base")
        assert t._backend == "whisper"

    def test_model_stripped(self) -> None:
        t = LocalVoiceTranscriber(backend="whisper", model="  base  ")
        assert t._model == "base"


# ── Text extraction ───────────────────────────────────────────────────


class TestExtractText:
    def setup_method(self) -> None:
        self.t = LocalVoiceTranscriber(backend="whisper", model="base")

    def test_none_returns_empty(self) -> None:
        assert self.t._extract_text(None) == ""

    def test_string_returns_stripped(self) -> None:
        assert self.t._extract_text("  hello  ") == "hello"

    def test_dict_with_text_key(self) -> None:
        assert self.t._extract_text({"text": "hello"}) == "hello"

    def test_dict_with_pred_text_key(self) -> None:
        assert self.t._extract_text({"pred_text": "world"}) == "world"

    def test_dict_with_transcript_key(self) -> None:
        assert self.t._extract_text({"transcript": "test"}) == "test"

    def test_dict_no_keys_returns_empty(self) -> None:
        assert self.t._extract_text({"foo": "bar"}) == ""

    def test_object_with_text_attr(self) -> None:
        obj = types.SimpleNamespace(text="attr text")
        assert self.t._extract_text(obj) == "attr text"

    def test_bytes_decoded(self) -> None:
        assert self.t._extract_text(b"bytes text") == "bytes text"


# ── Flatten results ───────────────────────────────────────────────────


class TestFlattenResults:
    def setup_method(self) -> None:
        self.t = LocalVoiceTranscriber(backend="whisper", model="base")

    def test_none_returns_empty_list(self) -> None:
        assert self.t._flatten_results(None) == []

    def test_string_wrapped(self) -> None:
        assert self.t._flatten_results("hello") == ["hello"]

    def test_dict_wrapped(self) -> None:
        d = {"text": "hi"}
        assert self.t._flatten_results(d) == [d]

    def test_list_flattened(self) -> None:
        result = self.t._flatten_results([1, [2, 3], 4])
        assert result == [1, 2, 3, 4]

    def test_nested_tuple_flattened(self) -> None:
        result = self.t._flatten_results([(1, 2), (3, 4)])
        assert result == [1, 2, 3, 4]


# ── Whisper transcription (with fake module) ──────────────────────────


class TestWhisperTranscription:
    def test_missing_dependency_raises(self, monkeypatch) -> None:
        """Without faster_whisper, transcribe raises LocalTranscriptionError."""
        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: None,
        )
        t = LocalVoiceTranscriber(backend="whisper", model="base")
        with pytest.raises(LocalTranscriptionError, match="faster-whisper"):
            import anyio

            async def run() -> None:
                await t.transcribe(model="base", audio_bytes=b"fake audio")

            anyio.run(run)

    @pytest.mark.anyio
    async def test_whisper_success(self, monkeypatch) -> None:
        """Fake faster_whisper module → successful transcription."""

        class FakeSegment:
            def __init__(self, text: str) -> None:
                self.text = text
                self.start = 0.0
                self.end = 1.0

        class FakeInfo:
            language = "en"
            language_probability = 0.99

        class FakeModel:
            def transcribe(self, path, **kwargs):
                return (
                    [FakeSegment("hello"), FakeSegment("world")],
                    FakeInfo(),
                )

        fake_module = types.ModuleType("faster_whisper")
        fake_module.WhisperModel = lambda *a, **kw: FakeModel()  # ty: ignore[unresolved-attribute]

        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: object() if name == "faster_whisper" else None,
        )
        monkeypatch.setitem(sys.modules, "faster_whisper", fake_module)
        monkeypatch.setattr(
            "untether.telegram.voice_local.importlib.import_module",
            lambda name: fake_module if name == "faster_whisper" else None,
        )

        t = LocalVoiceTranscriber(backend="whisper", model="base")
        result = await t.transcribe(model="base", audio_bytes=b"fake ogg")
        assert result == "hello world"

    @pytest.mark.anyio
    async def test_whisper_cuda_runtime(self, monkeypatch) -> None:
        """With torch.cuda.is_available()=True, runtime is cuda."""

        class FakeCuda:
            @staticmethod
            def is_available() -> bool:
                return True

        fake_torch = types.ModuleType("torch")
        fake_torch.cuda = FakeCuda  # ty: ignore[unresolved-attribute]

        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: object() if name == "torch" else None,
        )
        monkeypatch.setitem(sys.modules, "torch", fake_torch)

        t = LocalVoiceTranscriber(backend="whisper", model="base")
        assert t.runtime == "cuda"

    @pytest.mark.anyio
    async def test_whisper_cpu_fallback(self, monkeypatch) -> None:
        """Without torch, runtime is cpu."""
        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: None,
        )
        t = LocalVoiceTranscriber(backend="whisper", model="base")
        assert t.runtime == "cpu"


# ── Parakeet transcription (with fake module) ─────────────────────────


class TestParakeetTranscription:
    def test_missing_dependency_raises(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: None,
        )
        t = LocalVoiceTranscriber(
            backend="parakeet", model="nvidia/parakeet-tdt-0.6b-v3"
        )
        with pytest.raises(LocalTranscriptionError, match="onnx-asr"):
            import anyio

            async def run() -> None:
                await t.transcribe(
                    model="nvidia/parakeet-tdt-0.6b-v3",
                    audio_bytes=b"RIFF\x00\x00\x00\x00WAVE",
                )

            anyio.run(run)

    @pytest.mark.anyio
    async def test_parakeet_success(self, monkeypatch) -> None:
        """Fake onnx_asr module → successful transcription."""

        class FakeModel:
            def recognize(self, path: str):
                return {"text": "parakeet result"}

            def with_timestamps(self):
                return self

            def with_vad(self, vad):
                return self

        fake_module = types.ModuleType("onnx_asr")
        fake_module.load_model = lambda *a, **kw: FakeModel()  # ty: ignore[unresolved-attribute]
        fake_module.load_vad = lambda *a, **kw: object()  # ty: ignore[unresolved-attribute]

        monkeypatch.setattr(
            "untether.telegram.voice_local._find_spec_safe",
            lambda name: object(),
        )
        monkeypatch.setitem(sys.modules, "onnx_asr", fake_module)
        monkeypatch.setattr(
            "untether.telegram.voice_local.importlib.import_module",
            lambda name: fake_module if name == "onnx_asr" else None,
        )

        t = LocalVoiceTranscriber(
            backend="parakeet", model="nvidia/parakeet-tdt-0.6b-v3"
        )
        # WAV input to avoid ffmpeg conversion
        result = await t.transcribe(
            model="ignored",
            audio_bytes=b"RIFF\x00\x00\x00\x00wave",
        )
        assert result == "parakeet result"

    def test_parakeet_model_name_resolution(self) -> None:
        t = LocalVoiceTranscriber(
            backend="parakeet", model="nvidia/parakeet-tdt-0.6b-v3"
        )
        assert t._resolve_parakeet_model_name() == "nemo-parakeet-tdt-0.6b-v3"

    def test_parakeet_unknown_model_passes_through(self) -> None:
        t = LocalVoiceTranscriber(backend="parakeet", model="custom-model")
        assert t._resolve_parakeet_model_name() == "custom-model"


# ── Audio conversion (Parakeet) ───────────────────────────────────────


class TestAudioConversion:
    def test_whisper_skips_conversion(self, tmp_path: Path) -> None:
        """Non-parakeet backends return the source unchanged."""
        source = tmp_path / "audio.ogg"
        source.write_bytes(b"fake")
        result = ensure_backend_audio_file(source, "whisper")
        assert result == source

    def test_parakeet_wav_skips_conversion(self, tmp_path: Path) -> None:
        """Already-WAV input is not converted."""
        source = tmp_path / "audio.wav"
        source.write_bytes(b"RIFFfake")
        result = ensure_backend_audio_file(source, "parakeet")
        assert result == source

    def test_parakeet_ogg_triggers_conversion(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Non-WAV input for parakeet triggers ffmpeg conversion."""
        source = tmp_path / "audio.ogg"
        source.write_bytes(b"fake ogg")

        captured: list[list[str]] = []

        class FakeResult:
            returncode = 0
            stdout = ""
            stderr = ""

        def fake_run(cmd, **kwargs):
            captured.append(cmd)
            return FakeResult()

        monkeypatch.setattr("untether.telegram.voice_local.subprocess.run", fake_run)
        monkeypatch.setattr(
            "untether.telegram.voice_local.shutil.which",
            lambda name: "/usr/bin/ffmpeg",
        )

        result = ensure_backend_audio_file(source, "parakeet")
        assert result != source
        assert result.name == "audio_parakeet.wav"
        assert len(captured) == 1
        cmd = captured[0]
        assert "-ac" in cmd
        assert "1" in cmd
        assert "-ar" in cmd
        assert "16000" in cmd


# ── Language extraction ───────────────────────────────────────────────


class TestLanguageExtraction:
    def setup_method(self) -> None:
        self.t = LocalVoiceTranscriber(backend="whisper", model="base")

    def test_dict_with_language(self) -> None:
        assert self.t._extract_language({"language": "en"}) == "en"

    def test_dict_with_lang(self) -> None:
        assert self.t._extract_language({"lang": "fr"}) == "fr"

    def test_dict_with_detected_language(self) -> None:
        assert self.t._extract_language({"detected_language": "de"}) == "de"

    def test_dict_no_language_keys(self) -> None:
        assert self.t._extract_language({"text": "hello"}) == ""

    def test_object_with_language_attr(self) -> None:
        obj = types.SimpleNamespace(language="es")
        assert self.t._extract_language(obj) == "es"

    def test_none_returns_empty(self) -> None:
        assert self.t._extract_language(None) == ""
