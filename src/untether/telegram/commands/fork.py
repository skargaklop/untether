from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ...logging import get_logger
from ...model import ResumeToken
from ...runner import ForkableRunner, Runner

logger = get_logger(__name__)

FORK_HELP = """Fork a session into a new active session.

Reply `/fork` to an agent response, or use:
`/fork <engine> <session>`

After a successful fork, send your next message normally; Untether continues the new session."""

if TYPE_CHECKING:
    from ..chat_sessions import ChatSessionStore
    from ..topic_state import TopicStateStore


@dataclass(frozen=True, slots=True)
class ForkResult:
    ok: bool
    message: str
    token: ResumeToken | None = None


async def fork_session(
    *,
    runner: Runner,
    engine: str,
    explicit_session: ResumeToken | None = None,
    chat_store: ChatSessionStore | None = None,
    chat_key: tuple[int, int | None] | None = None,
    topic_store: TopicStateStore | None = None,
    topic_key: tuple[int, int] | None = None,
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
        destination = await runner.fork(source)
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
