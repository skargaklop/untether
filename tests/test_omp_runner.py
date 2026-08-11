"""Tests for the OMP runner: resume parsing, format, and event retagging."""

from __future__ import annotations

from untether.events import EventFactory
from untether.model import (
    ActionEvent,
    CompletedEvent,
    ResumeToken,
    StartedEvent,
)
from untether.runners.omp import (
    ENGINE,
    _retag_event,
    _retag_resume,
    _unquote_token,
)


class TestRetagResume:
    def test_none(self) -> None:
        assert _retag_resume(None) is None

    def test_already_omp(self) -> None:
        token = ResumeToken(engine="omp", value="sess-123")
        assert _retag_resume(token) == token

    def test_from_pi(self) -> None:
        token = ResumeToken(engine="pi", value="sess-456")
        retagged = _retag_resume(token)
        assert retagged is not None
        assert retagged.engine == "omp"
        assert retagged.value == "sess-456"


class TestRetagEvent:
    def test_started_event(self) -> None:
        factory = EventFactory("pi")
        token = ResumeToken(engine="pi", value="sess")
        event = factory.started(token, title="pi")
        retagged = _retag_event(event)
        assert isinstance(retagged, StartedEvent)
        assert retagged.engine == "omp"
        assert retagged.title == "omp"
        assert retagged.resume is not None
        assert retagged.resume.engine == "omp"

    def test_completed_event(self) -> None:
        factory = EventFactory("pi")
        token = ResumeToken(engine="pi", value="sess")
        event = factory.completed(ok=True, answer="ok", resume=token)
        retagged = _retag_event(event)
        assert isinstance(retagged, CompletedEvent)
        assert retagged.engine == "omp"
        assert retagged.resume is not None
        assert retagged.resume.engine == "omp"

    def test_action_event(self) -> None:
        factory = EventFactory("pi")
        event = factory.action_started(action_id="tc1", kind="tool", title="read file")
        retagged = _retag_event(event)
        assert isinstance(retagged, ActionEvent)
        assert retagged.engine == "omp"


class TestUnquoteToken:
    def test_plain(self) -> None:
        assert _unquote_token("sess-123") == "sess-123"

    def test_double_quotes(self) -> None:
        assert _unquote_token('"sess-123"') == "sess-123"

    def test_single_quotes(self) -> None:
        assert _unquote_token("'sess-123'") == "sess-123"

    def test_short_token_unchanged(self) -> None:
        assert _unquote_token("a") == "a"

    def test_empty(self) -> None:
        assert _unquote_token("") == ""


class TestOmpResumeParsing:
    def test_resume_re_matches_bare(self) -> None:
        from untether.runners.omp import _RESUME_RE

        match = _RESUME_RE.search("omp resume sess-123")
        assert match is not None
        assert match.group("token") == "sess-123"

    def test_resume_re_matches_double_dash(self) -> None:
        from untether.runners.omp import _RESUME_RE

        match = _RESUME_RE.search("omp --resume sess-456")
        assert match is not None
        assert match.group("token") == "sess-456"

    def test_resume_re_matches_slash_prefix(self) -> None:
        from untether.runners.omp import _RESUME_RE

        match = _RESUME_RE.search("/omp -r sess-789")
        assert match is not None
        assert match.group("token") == "sess-789"

    def test_resume_re_matches_quoted(self) -> None:
        from untether.runners.omp import _RESUME_RE

        match = _RESUME_RE.search('omp --resume "sess with spaces"')
        assert match is not None
        token = match.group("token")
        assert _unquote_token(token) == "sess with spaces"

    def test_engine_constant(self) -> None:
        assert ENGINE == "omp"

def test_omp_bare_503_remains_shared_transient_classifier_input() -> None:
    from untether.utils.transient_failures import classify_transient_failure

    failure = classify_transient_failure(
        "503 Chat admission capacity is temporarily unavailable."
    )
    assert failure is not None
    assert failure.http_status == 503
