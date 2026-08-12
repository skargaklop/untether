from __future__ import annotations

import importlib

import pytest

from untether import cli
from untether.config import ConfigError
from untether.lockfile import LockError
from untether.settings import UntetherSettings

doctor_module = importlib.import_module("untether.cli.doctor")


def _settings(overrides: dict | None = None) -> UntetherSettings:
    payload = {
        "transport": "telegram",
        "transports": {
            "telegram": {"bot_token": "token", "chat_id": 123, "allow_any_user": True}
        },
    }
    if overrides:
        payload.update(overrides)
    return UntetherSettings.model_validate(payload)


def test_parse_key_path_valid() -> None:
    assert cli._parse_key_path("transports.telegram.chat_id") == [
        "transports",
        "telegram",
        "chat_id",
    ]


def test_parse_key_path_invalid_segment() -> None:
    with pytest.raises(ConfigError):
        cli._parse_key_path("transports..chat_id")


def test_parse_value_toml_and_fallback() -> None:
    assert cli._parse_value("true") is True
    assert cli._parse_value("123") == 123
    assert cli._parse_value("not-toml") == "not-toml"


def test_toml_literal_and_error() -> None:
    assert cli._toml_literal("hello") == '"hello"'
    with pytest.raises(ConfigError):
        cli._toml_literal({"a": 1})


def test_flatten_config() -> None:
    flattened = cli._flatten_config(
        {"transports": {"telegram": {"chat_id": 123}}, "watch_config": True}
    )
    assert ("transports.telegram.chat_id", 123) in flattened
    assert ("watch_config", True) in flattened


def test_normalized_value_from_settings() -> None:
    settings = _settings()
    assert cli._normalized_value_from_settings(settings, ["transport"]) == "telegram"
    assert (
        cli._normalized_value_from_settings(
            settings, ["transports", "telegram", "chat_id"]
        )
        == 123
    )


def test_should_run_interactive(monkeypatch) -> None:
    class _Tty:
        def isatty(self) -> bool:
            return True

    class _NotTty:
        def isatty(self) -> bool:
            return False

    monkeypatch.setenv("TAKOPI_NO_INTERACTIVE", "1")
    assert cli._should_run_interactive() is False
    monkeypatch.delenv("TAKOPI_NO_INTERACTIVE")

    monkeypatch.setattr(cli.sys, "stdin", _Tty())
    monkeypatch.setattr(cli.sys, "stdout", _Tty())
    assert cli._should_run_interactive() is True

    monkeypatch.setattr(cli.sys, "stdin", _NotTty())
    monkeypatch.setattr(cli.sys, "stdout", _Tty())
    assert cli._should_run_interactive() is False


def test_resolve_transport_id_override(monkeypatch) -> None:
    assert cli._resolve_transport_id("  telegram ") == "telegram"
    with pytest.raises(ConfigError):
        cli._resolve_transport_id("   ")

    def _raise() -> None:
        raise ConfigError("boom")

    monkeypatch.setattr(cli, "load_or_init_config", _raise)
    assert cli._resolve_transport_id(None) == "telegram"


def test_doctor_file_checks() -> None:
    settings = _settings()
    checks = cli._doctor_file_checks(settings)
    assert checks[0].detail == "disabled"

    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "files": {"enabled": True},
                }
            }
        }
    )
    checks = cli._doctor_file_checks(settings)
    assert checks[0].status == "warning"


def test_doctor_voice_checks(monkeypatch) -> None:
    settings = _settings()
    assert cli._doctor_voice_checks(settings)[0].detail == "disabled"

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                    "voice_transcription_providers": ["openai"],
                }
            }
        }
    )
    checks = cli._doctor_voice_checks(settings)
    assert checks[0].status == "error"
    assert checks[0].detail == "no OpenAI API key"

    settings_with_key = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                    "voice_transcription_providers": ["openai"],
                    "voice_transcription_api_key": "secret",
                }
            }
        }
    )
    check = cli._doctor_voice_checks(settings_with_key)[0]
    assert check.status == "ok"
    assert check.detail == "voice_transcription_api_key set"


def test_doctor_voice_checks_local_provider(monkeypatch) -> None:
    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                    "voice_transcription_providers": ["local"],
                    "voice_transcription_local_backend": "whisper",
                    "voice_transcription_local_model": "small",
                }
            }
        }
    )
    monkeypatch.setattr(
        doctor_module, "_check_voice_provider", lambda *_: ("ok", "ready")
    )
    check = cli._doctor_voice_checks(settings)[0]
    assert check.label == "voice transcription [local]"
    assert check.status == "ok"


def test_doctor_voice_checks_default_providers(monkeypatch) -> None:
    monkeypatch.setattr(
        doctor_module, "_check_voice_provider", lambda *_: ("ok", "ready")
    )
    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                }
            }
        }
    )
    checks = cli._doctor_voice_checks(settings)
    assert [check.label for check in checks[:-1]] == [
        "voice transcription [avt]",
        "voice transcription [groq]",
        "voice transcription [local]",
        "voice transcription [openai]",
    ]
    assert checks[-1].detail == "4/4 providers usable"


def test_doctor_voice_checks_all_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        doctor_module, "_check_voice_provider", lambda *_: ("error", "missing")
    )
    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                }
            }
        }
    )
    checks = cli._doctor_voice_checks(settings)
    assert all(check.status == "error" for check in checks)
    assert checks[-1].detail == "no usable providers"


def test_doctor_voice_checks_partial_availability(monkeypatch) -> None:
    statuses = iter(
        [("error", "missing"), ("ok", "ready"), ("error", "missing"), ("ok", "ready")]
    )
    monkeypatch.setattr(
        doctor_module, "_check_voice_provider", lambda *_: next(statuses)
    )
    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                }
            }
        }
    )
    checks = cli._doctor_voice_checks(settings)
    assert checks[-1].status == "warning"
    assert checks[-1].detail == "2/4 providers usable"


def test_doctor_voice_checks_all_available(monkeypatch) -> None:
    monkeypatch.setattr(
        doctor_module, "_check_voice_provider", lambda *_: ("ok", "ready")
    )
    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                }
            }
        }
    )
    assert cli._doctor_voice_checks(settings)[-1].status == "ok"


def test_doctor_voice_checks_disabled() -> None:
    settings = _settings()
    assert cli._doctor_voice_checks(settings) == [
        cli.DoctorCheck("voice transcription", "ok", "disabled")
    ]


def test_doctor_voice_checks_never_prints_keys(monkeypatch) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "groq-secret")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-secret")
    settings = _settings(
        {
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 1,
                    "allow_any_user": True,
                    "voice_transcription": True,
                    "voice_transcription_providers": ["groq", "openai"],
                    "voice_transcription_groq_api_key": "explicit-groq-secret",
                    "voice_transcription_api_key": "explicit-openai-secret",
                }
            }
        }
    )
    details = [check.detail or "" for check in cli._doctor_voice_checks(settings)]
    assert all("secret" not in detail for detail in details)


def test_load_settings_optional(monkeypatch, tmp_path) -> None:
    def _raise() -> None:
        raise ConfigError("boom")

    monkeypatch.setattr(cli, "load_settings_if_exists", _raise)
    assert cli._load_settings_optional() == (None, None)

    monkeypatch.setattr(cli, "load_settings_if_exists", lambda: None)
    assert cli._load_settings_optional() == (None, None)

    settings = _settings()
    config_path = tmp_path / "untether.toml"
    monkeypatch.setattr(cli, "load_settings_if_exists", lambda: (settings, config_path))
    assert cli._load_settings_optional() == (settings, config_path)


def test_acquire_config_lock_reports_error(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "untether.toml"
    error = LockError(path=config_path, state="running")

    def _raise(*_args, **_kwargs):
        raise error

    messages: list[tuple[str, bool]] = []
    monkeypatch.setattr(cli, "acquire_lock", _raise)
    monkeypatch.setattr(
        cli.typer, "echo", lambda msg, err=False: messages.append((msg, err))
    )

    with pytest.raises(cli.typer.Exit) as exc:
        cli.acquire_config_lock(config_path, "token")

    assert exc.value.exit_code == 1
    assert any("already running" in msg for msg, _ in messages)
