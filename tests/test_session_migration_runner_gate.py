"""SessionLockMixin.run_with_resume_lock honors UNTETHER_SESSION_GATE_ROOT.

Also covers BaseRunner.run_locked new-session path: the ExitStack gate is
entered on StartedEvent and released after the stream ends.
"""

import pytest

from untether.events import EventFactory
from untether.model import ResumeToken, StartedEvent
from untether.runner import BaseRunner, SessionLockMixin
from untether.session_migration.session_gate import SourceBusy, session_gate


class _GateRunner(SessionLockMixin):
    engine = "gatedummy"


@pytest.mark.anyio
async def test_gate_busy_blocks_run(tmp_path, monkeypatch) -> None:
    """Held external gate -> SourceBusy, run_fn never entered."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path))
    runner = _GateRunner()
    token = ResumeToken(engine="gatedummy", value="tok-1")
    entered = False

    async def run_fn(prompt: str, resume: ResumeToken | None):
        nonlocal entered
        entered = True
        yield {"dummy": True}

    with (
        session_gate(root=tmp_path, engine_id="gatedummy", native_id="tok-1"),
        pytest.raises(SourceBusy),
    ):
        async for _ in runner.run_with_resume_lock("p", token, run_fn):
            pass
    assert not entered


@pytest.mark.anyio
async def test_gate_released_after_run(tmp_path, monkeypatch) -> None:
    """Clean run and mid-stream exception both release the gate for reacquire."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path))
    runner = _GateRunner()
    token = ResumeToken(engine="gatedummy", value="tok-2")
    closed = False

    async def run_fn(prompt: str, resume: ResumeToken | None):
        nonlocal closed
        try:
            yield {"dummy": True}
        finally:
            closed = True

    out = [evt async for evt in runner.run_with_resume_lock("p", token, run_fn)]
    assert out == [{"dummy": True}]
    assert closed
    with session_gate(root=tmp_path, engine_id="gatedummy", native_id="tok-2"):
        pass

    async def boom(prompt: str, resume: ResumeToken | None):
        yield {"dummy": True}
        raise RuntimeError("mid-stream")

    with pytest.raises(RuntimeError, match="mid-stream"):
        async for _ in runner.run_with_resume_lock("p", token, boom):
            pass
    with session_gate(root=tmp_path, engine_id="gatedummy", native_id="tok-2"):
        pass


class _NewSessionRunner(BaseRunner):
    """Minimal BaseRunner: run_impl yields factory.started(token) then a dummy."""

    engine = "gatedummy"

    def __init__(self, root, token: ResumeToken) -> None:
        self._root = root
        self._token = token
        self._factory = EventFactory(self.engine)
        self.gate_busy_during_run: bool | None = None

    async def run_impl(self, prompt: str, resume: ResumeToken | None):
        try:
            yield self._factory.started(self._token)
            yield {"dummy": True}
        finally:
            try:
                with session_gate(
                    root=self._root,
                    engine_id=self.engine,
                    native_id=self._token.value,
                ):
                    self.gate_busy_during_run = False
            except SourceBusy:
                self.gate_busy_during_run = True


@pytest.mark.anyio
async def test_new_session_gate_held_on_started_then_released(
    tmp_path, monkeypatch
) -> None:
    """New session: gate entered on StartedEvent, held mid-stream, released after."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path))
    token = ResumeToken(engine="gatedummy", value="tok-new-1")
    runner = _NewSessionRunner(tmp_path, token)

    out = [evt async for evt in runner.run("p", None)]

    assert isinstance(out[0], StartedEvent)
    assert out[0].resume == token
    assert runner.gate_busy_during_run is True  # still held inside run_impl
    with session_gate(root=tmp_path, engine_id="gatedummy", native_id="tok-new-1"):
        pass  # released after the stream ends


@pytest.mark.anyio
async def test_preheld_gate_refuses_new_session_before_started_delivered(
    tmp_path, monkeypatch
) -> None:
    """Externally pre-held gate: SourceBusy on StartedEvent, never delivered."""
    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path))
    token = ResumeToken(engine="gatedummy", value="tok-new-2")
    runner = _NewSessionRunner(tmp_path, token)
    seen: list[object] = []

    with (
        session_gate(root=tmp_path, engine_id="gatedummy", native_id="tok-new-2"),
        pytest.raises(SourceBusy),
    ):
        seen.extend([evt async for evt in runner.run("p", None)])

    assert seen == []
