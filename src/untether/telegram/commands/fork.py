from __future__ import annotations

import contextlib
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ...logging import get_logger
from ...model import ResumeToken
from ...runner import ForkableRunner, Runner

logger = get_logger(__name__)

FORK_HELP = """Fork a session into a new active session.

Reply `/fork` to an agent response, or use:
`/fork <engine> <session>`

By default, the fork stays in the source session's project folder.
To choose another folder:
`/fork <engine> <session> --cwd <destination-folder>`"""

if TYPE_CHECKING:
    from ..chat_sessions import ChatSessionStore
    from ..topic_state import TopicStateStore


@dataclass(frozen=True, slots=True)
class ForkRequest:
    runner: Runner
    engine: str
    source: ResumeToken
    destination_cwd: Path
    chat_id: int
    thread_id: int | None
    sender_id: int | None
    progress_ref: Any
    chat_store: ChatSessionStore | None
    chat_key: tuple[int, int | None] | None
    topic_store: TopicStateStore | None
    topic_key: tuple[int, int] | None
    expires_at: float


@dataclass(frozen=True, slots=True)
class ForkResult:
    ok: bool
    message: str
    token: ResumeToken | None = None


def fork_confirmation_markup(token: str) -> dict[str, list]:
    return {
        "inline_keyboard": [
            [
                {"text": "confirm", "callback_data": f"fork:{token}:confirm"},
                {"text": "cancel", "callback_data": f"fork:{token}:cancel"},
            ]
        ]
    }


def new_fork_token() -> str:
    return secrets.token_urlsafe(16)


def resolve_fork_cwd(
    runner: Runner, source: ResumeToken, explicit_cwd: str | None
) -> Path | None:
    if explicit_cwd:
        return Path(explicit_cwd).expanduser().resolve()
    session_cwd = getattr(runner, "session_cwd", None)
    return session_cwd(source.value) if callable(session_cwd) else None


async def handle_fork_callback(
    cfg: Any,
    update: Any,
    registry: dict[str, ForkRequest],
) -> None:
    from ...transport import RenderedMessage

    with contextlib.suppress(Exception):
        await cfg.bot.answer_callback_query(update.callback_query_id)
    parts = (update.data or "").split(":")
    if len(parts) != 3 or parts[0] != "fork":
        return
    _, token, action = parts
    record = registry.get(token)
    if (
        record is None
        or record.chat_id != update.chat_id
        or record.sender_id != update.sender_id
        or time.monotonic() >= record.expires_at
    ):
        return
    callback_thread = None
    if update.raw and isinstance(update.raw.get("message"), dict):
        callback_thread = update.raw["message"].get("message_thread_id")
    if callback_thread != record.thread_id:
        return
    registry.pop(token, None)
    if action == "cancel":
        await cfg.exec_cfg.transport.edit(
            ref=record.progress_ref,
            message=RenderedMessage(
                text="fork cancelled", extra={"reply_markup": {"inline_keyboard": []}}
            ),
        )
        return
    if action != "confirm":
        return
    result = await fork_session(
        runner=record.runner,
        engine=record.engine,
        explicit_session=record.source,
        chat_store=record.chat_store,
        chat_key=record.chat_key,
        topic_store=record.topic_store,
        topic_key=record.topic_key,
        destination_cwd=record.destination_cwd,
    )
    await cfg.exec_cfg.transport.edit(
        ref=record.progress_ref,
        message=RenderedMessage(
            text=result.message, extra={"reply_markup": {"inline_keyboard": []}}
        ),
    )


async def fork_session(
    *,
    runner: Runner,
    engine: str,
    explicit_session: ResumeToken | None = None,
    chat_store: ChatSessionStore | None = None,
    chat_key: tuple[int, int | None] | None = None,
    topic_store: TopicStateStore | None = None,
    topic_key: tuple[int, int] | None = None,
    destination_cwd: Path | None = None,
) -> ForkResult:
    source = explicit_session
    if source is None and topic_store is not None and topic_key is not None:
        source = await topic_store.get_session_resume(*topic_key, engine)
    if source is None and chat_store is not None and chat_key is not None:
        source = await chat_store.get_session_resume(*chat_key, engine)
    if source is None:
        return ForkResult(False, f"no stored {engine} session to fork")
    if not isinstance(runner, ForkableRunner):
        return ForkResult(False, f"fork is not supported by {engine}")
    try:
        fork_to = getattr(runner, "fork_to", None)
        destination = (
            await fork_to(source, destination_cwd)
            if destination_cwd is not None and callable(fork_to)
            else await runner.fork(source)
        )
    except NotImplementedError:
        return ForkResult(False, f"fork is not supported by {engine}")
    except Exception as exc:  # noqa: BLE001 - provider errors become safe replies
        logger.warning("session.fork.failed", engine=engine, error=str(exc))
        return ForkResult(False, f"could not fork {engine} session")
    if not isinstance(destination, ResumeToken) or destination.engine != engine:
        return ForkResult(False, f"{engine} fork did not return a valid session")
    if topic_store is not None and topic_key is not None:
        await topic_store.set_session_resume(*topic_key, destination)
    if chat_store is not None and chat_key is not None:
        await chat_store.set_session_resume(*chat_key, destination)
    resume_line = runner.format_resume(destination)
    return ForkResult(
        True,
        (
            f"forked {engine} session\n\n"
            f"session: {resume_line}\n\n"
            "This fork is now active. Send your next message normally."
        ),
        destination,
    )
