from __future__ import annotations

from contextlib import suppress
from pathlib import Path

from untether.logging import setup_logging
from untether.settings import LoggingSettings


def test_setup_logging_uses_settings_and_env_precedence(
    tmp_path: Path, monkeypatch
) -> None:
    home = tmp_path / "home"
    (home / ".untether").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.setenv("TAKOPI_LOG_LEVEL", "error")
    monkeypatch.setenv("TAKOPI_LOG_FORMAT", "console")
    monkeypatch.setenv("TAKOPI_LOG_FILE", "env.log")
    setup_logging(
        settings=LoggingSettings(level="warning", file="toml.log", format="json")
    )
    from untether.logging import _MIN_LEVEL, _log_file_handle

    assert _MIN_LEVEL == 40
    assert _log_file_handle is not None
    assert Path(_log_file_handle.name) == home / ".untether" / "env.log"
    _log_file_handle.close()


def test_setup_logging_debug_wins_and_relative_file(
    tmp_path: Path, monkeypatch
) -> None:
    home = tmp_path / "home"
    (home / ".untether").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("TAKOPI_LOG_LEVEL", raising=False)
    monkeypatch.delenv("TAKOPI_LOG_FILE", raising=False)
    setup_logging(
        settings=LoggingSettings(level="error", file="events.log", format="json"),
        debug=True,
    )
    from untether.logging import _MIN_LEVEL, _log_file_handle

    assert _MIN_LEVEL == 10
    assert _log_file_handle is not None
    assert Path(_log_file_handle.name) == home / ".untether" / "events.log"
    _log_file_handle.close()


def test_cli_passes_nested_toml_logging_to_shared_setup(monkeypatch) -> None:
    from untether import cli
    from untether.config import ConfigError
    from untether.settings import UntetherSettings

    settings = UntetherSettings.model_validate(
        {
            "transport": "telegram",
            "transports": {
                "telegram": {
                    "bot_token": "token",
                    "chat_id": 123,
                    "allow_any_user": True,
                }
            },
            "logging": {"level": "warning", "file": "configured.log", "format": "json"},
        }
    )
    observed: dict[str, object] = {}
    monkeypatch.setattr(
        cli, "_load_settings_optional", lambda: (settings, Path("config.toml"))
    )
    monkeypatch.setattr(cli, "setup_logging", lambda **kwargs: observed.update(kwargs))
    monkeypatch.setattr(
        cli,
        "_resolve_setup_engine",
        lambda _override: (_ for _ in ()).throw(ConfigError("stop")),
    )
    with suppress(cli.typer.Exit):
        cli._run_auto_router(
            default_engine_override=None,
            transport_override=None,
            final_notify=True,
            debug=False,
            onboard=False,
        )
    assert observed["settings"] is settings.logging


def test_emitted_logs_redact_configured_tokens(capsys, monkeypatch) -> None:
    monkeypatch.delenv("TAKOPI_LOG_FILE", raising=False)
    monkeypatch.setenv("TAKOPI_LOG_FORMAT", "json")
    setup_logging()
    from untether.logging import get_logger

    secret = "sk-proj-AbC_dEf-GhI_jKl-MnO_pQr-StU_vWx-YzAbCdEfGh"
    get_logger("security").error("configured secret", token=secret)
    output = capsys.readouterr().out
    assert secret not in output
    assert "[REDACTED_KEY]" in output


def test_file_sink_redacts_provider_tokens(tmp_path: Path, monkeypatch) -> None:
    log_path = tmp_path / "secrets.log"
    monkeypatch.setenv("TAKOPI_LOG_FILE", str(log_path))
    monkeypatch.setenv("TAKOPI_LOG_FORMAT", "json")
    setup_logging()
    from untether.logging import _log_file_handle, get_logger

    telegram = "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"
    openai = "sk-proj-AbC_dEf-GhI_jKl-MnO_pQr-StU_vWx-YzAbCdEfGh"
    github = "ghp_1234567890_supersecretvalue123"
    get_logger("security").error(
        "configured credentials",
        telegram=telegram,
        openai=openai,
        github=github,
    )
    assert _log_file_handle is not None
    _log_file_handle.close()
    contents = log_path.read_text(encoding="utf-8")
    assert telegram not in contents
    assert openai not in contents
    assert github not in contents
    assert "[REDACTED_TOKEN]" in contents
    assert "[REDACTED_KEY]" in contents
