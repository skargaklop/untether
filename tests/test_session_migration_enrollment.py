"""Enrollment cannot silently reassign a private chat or trust another bot."""

from __future__ import annotations

import pytest

from untether.session_migration.telegram_enrollment import (
    EnrollmentError,
    EnrollmentStore,
)


def test_explicit_distinct_bot_enrollment_and_binding(tmp_path):
    store = EnrollmentStore(tmp_path / "enrolment.json")
    current = store.enroll(
        bot_id=22,
        chat_id=-100123,
        topic_id=42,
        peer_name="remote",
        endpoint_id="endpoint-a",
        confirmed=True,
        source_bot_id=11,
    )
    assert current.confirmed
    assert store.read() == current
    assert (
        store.require_binding(
            bot_id=22,
            chat_id=-100123,
            topic_id=42,
            peer_name="remote",
            endpoint_id="endpoint-a",
        )
        == current
    )
    with pytest.raises(EnrollmentError, match="DESTINATION_NOT_ENROLLED"):
        store.require_binding(
            bot_id=22,
            chat_id=-100123,
            topic_id=43,
            peer_name="remote",
            endpoint_id="endpoint-a",
        )


def test_same_bot_and_unconfirmed_destinations_refused(tmp_path):
    store = EnrollmentStore(tmp_path / "enrolment.json")
    with pytest.raises(EnrollmentError, match="SAME_BOT"):
        store.enroll(
            bot_id=11,
            chat_id=123,
            topic_id=None,
            peer_name="remote",
            endpoint_id="endpoint-a",
            confirmed=True,
            source_bot_id=11,
        )
    with pytest.raises(EnrollmentError, match="ENROLLMENT_INVALID"):
        store.enroll(
            bot_id=22,
            chat_id=123,
            topic_id=None,
            peer_name="remote",
            endpoint_id="endpoint-a",
            confirmed=False,
            source_bot_id=11,
        )
    assert not store.file.exists()


def test_no_silent_rebinding(tmp_path):
    store = EnrollmentStore(tmp_path / "enrolment.json")
    store.enroll(
        bot_id=22,
        chat_id=123,
        topic_id=None,
        peer_name="remote",
        endpoint_id="endpoint-a",
        confirmed=True,
        source_bot_id=11,
    )
    with pytest.raises(EnrollmentError, match="DESTINATION_ALREADY_ENROLLED"):
        store.enroll(
            bot_id=33,
            chat_id=123,
            topic_id=None,
            peer_name="remote",
            endpoint_id="endpoint-a",
            confirmed=True,
            source_bot_id=11,
        )
