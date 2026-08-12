from __future__ import annotations

# Adapted from AI-Video-Transcriber (Apache-2.0):
#   backend/transcriber.py (WhisperLocalTranscriber)
#   backend/parakeet_transcriber.py (ParakeetLocalTranscriber)
#   backend/local_transcription.py (model resolution + audio preparation)
# Modified by Untether: native anyio integration, per-instance caching with locks,
# no runtime pip install, plain-text output, Untether cache directory.
# See docs/ATTRIBUTION.md or ATTRIBUTION.md for third-party license details.
import importlib
import importlib.util
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import anyio

from ..logging import get_logger

logger = get_logger(__name__)

_LOCAL_MODEL_CACHE_DIR = Path.home() / ".untether" / "models"
DEFAULT_LOCAL_BACKEND = "whisper"
DEFAULT_WHISPER_MODEL = "base"
DEFAULT_PARAKEET_MODEL = "nvidia/parakeet-tdt-0.6b-v3"
PARAKEET_MODEL_NAME_MAP = {
    "nvidia/parakeet-tdt-0.6b-v3": "nemo-parakeet-tdt-0.6b-v3",
    "nvidia/parakeet-tdt-0.6b-v2": "nemo-parakeet-tdt-0.6b-v2",
}
PARAKEET_MODEL_PRESETS = list(PARAKEET_MODEL_NAME_MAP)


class LocalTranscriptionError(RuntimeError):
    """Raised when a local transcription backend cannot run."""


class LocalVoiceTranscriber:
    def __init__(self, *, backend: str, model: str, timeout_s: float = 180.0) -> None:
        self._backend = (backend or DEFAULT_LOCAL_BACKEND).strip().lower()
        self._model = (model or "").strip()
        self._timeout_s = timeout_s
        self._model_obj: Any = None
        self._runtime: str | None = None
        self._lock = anyio.Lock()
        if self._backend not in {"whisper", "parakeet"}:
            raise LocalTranscriptionError(f"Unsupported local backend: {backend}")

    @property
    def runtime(self) -> str:
        if self._runtime is None:
            self._runtime = self._detect_runtime()
        return self._runtime

    @staticmethod
    def _find_spec(name: str) -> Any:
        try:
            return importlib.util.find_spec(name)
        except (ImportError, ValueError):
            return object() if name in sys.modules else None

    @staticmethod
    def whisper_dependency_available() -> bool:
        return _find_spec_safe("faster_whisper") is not None

    @staticmethod
    def parakeet_dependency_available() -> bool:
        return (
            _find_spec_safe("onnx_asr") is not None
            and _find_spec_safe("onnxruntime") is not None
        )

    def dependency_available(self) -> bool:
        return (
            whisper_available() if self._backend == "whisper" else parakeet_available()
        )

    def _detect_runtime(self) -> str:
        if self._backend == "parakeet":
            try:
                if _find_spec_safe("onnxruntime") is not None:
                    ort = importlib.import_module("onnxruntime")
                    providers = set(ort.get_available_providers())
                    if providers & {
                        "CUDAExecutionProvider",
                        "TensorrtExecutionProvider",
                        "DmlExecutionProvider",
                        "ROCMExecutionProvider",
                    }:
                        return "cuda"
            except Exception:  # noqa: BLE001
                logger.debug("local.ort_runtime.failed", exc_info=True)
        try:
            if _find_spec_safe("torch") is not None:
                torch = importlib.import_module("torch")
                if getattr(
                    getattr(torch, "cuda", None), "is_available", lambda: False
                )():
                    return "cuda"
        except Exception:  # noqa: BLE001
            logger.debug("local.torch_runtime.failed", exc_info=True)
        return "cpu"

    def _resolve_parakeet_model_name(self) -> str:
        return PARAKEET_MODEL_NAME_MAP.get(self._model, self._model)

    def _load_model(self) -> Any:
        if self._model_obj is not None:
            return self._model_obj
        if not self.dependency_available():
            package = (
                "faster-whisper" if self._backend == "whisper" else "onnx-asr[cpu,hub]"
            )
            raise LocalTranscriptionError(f"{package} is not installed")
        if self._backend == "whisper":
            module = importlib.import_module("faster_whisper")
            self._model_obj = module.WhisperModel(
                self._model or DEFAULT_WHISPER_MODEL,
                device=self.runtime,
                compute_type="float16" if self.runtime == "cuda" else "int8",
            )
            return self._model_obj
        module = importlib.import_module("onnx_asr")
        model_name = self._resolve_parakeet_model_name()
        cache = _LOCAL_MODEL_CACHE_DIR / "onnx_asr" / model_name.replace("/", "__")
        cache.parent.mkdir(parents=True, exist_ok=True)
        last_error: Exception | None = None
        for quantization in ("int8", None):
            try:
                kwargs = {"quantization": quantization} if quantization else {}
                self._model_obj = module.load_model(model_name, str(cache), **kwargs)
                try:
                    self._model_obj = self._model_obj.with_timestamps()
                except Exception:  # noqa: BLE001
                    logger.debug("local.timestamps.unavailable", exc_info=True)
                try:
                    self._model_obj = self._model_obj.with_vad(
                        module.load_vad("silero")
                    )
                except Exception:  # noqa: BLE001
                    logger.debug("local.vad.unavailable", exc_info=True)
                return self._model_obj
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise LocalTranscriptionError(
            f"Failed to load Parakeet model '{model_name}': {last_error}"
        )

    async def transcribe(
        self, *, model: str, audio_bytes: bytes, language: str | None = None
    ) -> str:
        _ = model
        async with self._lock:
            source: Path | None = None
            converted: Path | None = None
            try:
                suffix = ".wav" if audio_bytes[:4] == b"RIFF" else ".ogg"
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as file:
                    source = Path(file.name)
                    file.write(audio_bytes)
                audio_path = source
                if self._backend == "parakeet" and source.suffix.lower() != ".wav":
                    converted = await anyio.to_thread.run_sync(
                        ensure_backend_audio_file, source, self._backend
                    )
                    audio_path = converted
                model_obj = await anyio.to_thread.run_sync(self._load_model)
                if self._backend == "whisper":
                    result = await anyio.to_thread.run_sync(
                        lambda: model_obj.transcribe(
                            str(audio_path),
                            language=language or None,
                            beam_size=5,
                            best_of=5,
                            temperature=[0.0, 0.2, 0.4],
                            vad_filter=True,
                            vad_parameters={
                                "min_silence_duration_ms": 900,
                                "speech_pad_ms": 300,
                            },
                            no_speech_threshold=0.7,
                            compression_ratio_threshold=2.3,
                            log_prob_threshold=-1.0,
                            condition_on_previous_text=False,
                        )
                    )
                    segments = result[0] if isinstance(result, tuple) else result
                    return " ".join(
                        self._extract_text(item)
                        for item in segments
                        if self._extract_text(item)
                    ).strip()
                result = await anyio.to_thread.run_sync(
                    lambda: model_obj.recognize(str(audio_path))
                )
                return (
                    self._extract_text(result)
                    if not isinstance(result, Iterable)
                    or isinstance(result, (str, bytes, dict))
                    else " ".join(
                        filter(
                            None,
                            (
                                self._extract_text(x)
                                for x in self._flatten_results(result)
                            ),
                        )
                    ).strip()
                )
            finally:
                for path in (converted, source):
                    if path is not None:
                        path.unlink(missing_ok=True)

    def _flatten_results(self, result: Any) -> list[Any]:
        if result is None:
            return []
        if isinstance(result, (str, bytes, dict)) or hasattr(result, "__dict__"):
            return [result]
        if isinstance(result, Iterable):
            out: list[Any] = []
            for item in result:
                out.extend(
                    self._flatten_results(item)
                    if isinstance(item, (list, tuple))
                    else [item]
                )
            return out
        return [result]

    def _extract_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode(errors="replace").strip()
        if isinstance(value, str):
            return value.strip()
        keys = ("text", "pred_text", "transcript")
        if isinstance(value, dict):
            return next((str(value[k]).strip() for k in keys if value.get(k)), "")
        return next(
            (
                str(getattr(value, key)).strip()
                for key in keys
                if getattr(value, key, None)
            ),
            "",
        )

    def _normalize_result(self, result: Any) -> dict[str, Any]:
        entries = self._flatten_results(result)
        text = " ".join(
            filter(None, (self._extract_text(item) for item in entries))
        ).strip()
        return {
            "raw": {"text": text},
            "language": self._extract_language(result),
            "warnings": [],
            "timestamps_supported": False,
        }

    def _extract_language(self, value: Any) -> str:
        for key in ("language", "lang", "detected_language"):
            candidate = (
                value.get(key) if isinstance(value, dict) else getattr(value, key, None)
            )
            if candidate:
                return str(candidate).strip()
        return ""


def _find_spec_safe(name: str) -> Any:
    try:
        return importlib.util.find_spec(name)
    except (ImportError, ValueError):
        return object() if name in __import__("sys").modules else None


def whisper_available() -> bool:
    return _find_spec_safe("faster_whisper") is not None


def parakeet_available() -> bool:
    return (
        _find_spec_safe("onnx_asr") is not None
        and _find_spec_safe("onnxruntime") is not None
    )


def local_backend_available(backend: str) -> bool:
    normalized = (backend or DEFAULT_LOCAL_BACKEND).strip().lower()
    return (
        whisper_available()
        if normalized == "whisper"
        else parakeet_available()
        if normalized == "parakeet"
        else False
    )


def resolve_local_model_id(
    local_backend: str, local_model_preset: str = "", local_model_id: str = ""
) -> str:
    backend = (local_backend or DEFAULT_LOCAL_BACKEND).strip().lower()
    preset, custom = (local_model_preset or "").strip(), (local_model_id or "").strip()
    if preset == "custom" and custom:
        return custom
    if custom and not preset:
        return custom
    if preset and preset != "custom":
        return preset
    return DEFAULT_PARAKEET_MODEL if backend == "parakeet" else DEFAULT_WHISPER_MODEL


def ensure_backend_audio_file(source: Path, backend: str) -> Path:
    if (
        backend or DEFAULT_LOCAL_BACKEND
    ).strip().lower() != "parakeet" or source.suffix.lower() == ".wav":
        return source
    target = source.with_name(f"{source.stem}_parakeet.wav")
    ffmpeg = shutil.which("ffmpeg") or "ffmpeg"
    subprocess.run(
        [ffmpeg, "-y", "-i", str(source), "-ac", "1", "-ar", "16000", str(target)],
        check=True,
        capture_output=True,
    )
    return target


_LOCAL_CACHE: dict[tuple[str, str], LocalVoiceTranscriber] = {}


def get_local_transcriber(
    backend: str, model: str, *, timeout_s: float = 180.0
) -> LocalVoiceTranscriber:
    key = ((backend or DEFAULT_LOCAL_BACKEND).strip().lower(), (model or "").strip())
    if key not in _LOCAL_CACHE:
        _LOCAL_CACHE[key] = LocalVoiceTranscriber(
            backend=key[0], model=key[1], timeout_s=timeout_s
        )
    return _LOCAL_CACHE[key]
