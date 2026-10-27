"""Session command alias resolving to session_migration.telegram_command."""

from ...session_migration.telegram_command import BACKEND, SessionCommand

__all__ = ["BACKEND", "SessionCommand"]
