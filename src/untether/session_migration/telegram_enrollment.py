"""Explicit Telegram destination enrollment, never automatic chat takeover.

Enrollment is scoped to the destination chat/topic; it cannot transfer
messages or silently bind a different Telegram bot to an existing private chat.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path


class EnrollmentError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DestinationEnrollment:
    bot_id: int
    chat_id: int
    topic_id: int | None
    peer_name: str
    endpoint_id: str
    confirmed: bool


class EnrollmentStore:
    def __init__(self, file: Path) -> None:
        self.file = file

    def read(self) -> DestinationEnrollment | None:
        if self.file.is_symlink():
            raise EnrollmentError("ENROLLMENT_SYMLINK")
        if not self.file.is_file():
            return None
        try:
            data = json.loads(self.file.read_text(encoding="utf-8"))
            enrollment = DestinationEnrollment(**data)
        except (OSError, ValueError, TypeError) as exc:
            raise EnrollmentError("ENROLLMENT_CORRUPT") from exc
        if not enrollment.confirmed:
            raise EnrollmentError("ENROLLMENT_UNCONFIRMED")
        return enrollment

    def enroll(
        self,
        *,
        bot_id: int,
        chat_id: int,
        topic_id: int | None,
        peer_name: str,
        endpoint_id: str,
        confirmed: bool,
        source_bot_id: int,
    ) -> DestinationEnrollment:
        if (
            not confirmed
            or bot_id <= 0
            or not chat_id
            or bot_id == source_bot_id
            or topic_id is not None
            and topic_id <= 0
            or not peer_name
            or not endpoint_id
        ):
            raise EnrollmentError("ENROLLMENT_INVALID_OR_SAME_BOT")
        previous = self.read()
        desired = DestinationEnrollment(
            bot_id,
            chat_id,
            topic_id,
            peer_name,
            endpoint_id,
            True,
        )
        if previous is not None and previous != desired:
            raise EnrollmentError("DESTINATION_ALREADY_ENROLLED")
        if previous == desired:
            return previous
        if self.file.parent.is_symlink():
            raise EnrollmentError("ENROLLMENT_PARENT_SYMLINK")
        self.file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=self.file.parent,
            prefix=".enroll-",
            mode="w",
            encoding="utf-8",
            delete=False,
        ) as stream:
            tmp = Path(stream.name)
            os.chmod(tmp, 0o600)
            json.dump(asdict(desired), stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(tmp, self.file)
        except FileExistsError as exc:
            raise EnrollmentError("DESTINATION_ALREADY_ENROLLED") from exc
        finally:
            tmp.unlink(missing_ok=True)
        return desired

    def require_binding(
        self,
        *,
        bot_id: int,
        chat_id: int,
        topic_id: int | None,
        peer_name: str,
        endpoint_id: str,
    ) -> DestinationEnrollment:
        current = self.read()
        if current is None or (
            current.bot_id,
            current.chat_id,
            current.topic_id,
            current.peer_name,
            current.endpoint_id,
        ) != (bot_id, chat_id, topic_id, peer_name, endpoint_id):
            raise EnrollmentError("DESTINATION_NOT_ENROLLED")
        return current
