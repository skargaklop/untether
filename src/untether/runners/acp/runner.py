from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio

from ...events import EventFactory
from ...model import EngineId, ResumeToken
from ...runner import ResumeTokenMixin
from .peer import AcpPeer
from .protocol import negotiate
from .state import AcpSessionState
from .turn import AcpTurnControl


@dataclass(slots=True)
class AcpRunner(ResumeTokenMixin):
    engine: EngineId
    command: str
    args: list[str] = field(default_factory=list)
    cwd: str | None = None
    env: dict[str, str] | None = None
    protocol: str = "auto"
    turn_timeout_s: float = 1800.0
    request_timeout_s: float = 60.0
    close_timeout_s: float = 5.0
    peer_factory: Callable[[], Any] | None = None
    resume_re: re.Pattern[str] = field(
        default=re.compile(
            r"(?im)^\s*`?[\w-]+\s+(?:resume\s+)?(?P<token>[\w.-]+)`?\s*$"
        ),
        repr=False,
    )

    def _peer(self) -> Any:
        if self.peer_factory is not None:
            return self.peer_factory()
        return AcpPeer(
            self.command,
            self.args,
            cwd=self.cwd,
            env=self.env,
            request_timeout_s=self.request_timeout_s,
        )

    async def run(self, prompt: str, resume: ResumeToken | None) -> Any:
        async for event in self._run(prompt, resume):
            yield event

    async def _run(self, prompt: str, resume: ResumeToken | None):
        factory = EventFactory(self.engine)
        peer = self._peer()
        state = AcpSessionState(_factory=factory)
        token: ResumeToken | None = None
        started = False
        try:
            await peer.start()
            init = await peer.request(
                "initialize",
                negotiate(self.protocol, {"protocolVersion": 1}).initialize_params(),
            )
            adapter = negotiate(self.protocol, init)
            method = "session/resume" if resume is not None else "session/new"
            params = (
                {"sessionId": resume.value}
                if resume is not None
                else {"cwd": self.cwd or str(Path.cwd())}
            )
            created = await peer.request(method, params)
            sid = created.get("sessionId")
            if not isinstance(sid, str) or not sid:
                raise RuntimeError("ACP session creation returned no sessionId")
            token = ResumeToken(self.engine, sid)
            notify = getattr(peer, "notify", None)
            control = AcpTurnControl(notify, sid) if callable(notify) else None
            meta = {"acp_protocol": adapter.version}
            if control is not None:
                meta["control"] = control
            yield factory.started(token, title=str(self.engine), meta=meta)
            started = True
            response = await peer.request(
                "session/prompt", adapter.prompt_params(sid, prompt)
            )
            while True:
                try:
                    with anyio.move_on_after(0.01) as scope:
                        notification = await peer.next_notification()
                    if scope.cancel_called:
                        break
                except RuntimeError:
                    break
                if notification.get("method") == "session/update":
                    params = notification.get("params", {})
                    for action in state.apply(
                        params if isinstance(params, dict) else {}
                    ):
                        yield action
            answer = state.answer
            reason = str(response.get("stopReason", ""))
            ok = reason not in {"error", "failed", "cancelled", "canceled"}
            if ok:
                with suppress(Exception):
                    await peer.request("session/close", {"sessionId": sid})
            yield factory.completed(
                ok=ok,
                answer=answer,
                resume=token,
                error=None if ok else reason,
                usage=state.usage or None,
            )
        except BaseException as exc:  # noqa: BLE001 - completion must cover cancellation
            if started:
                yield factory.completed_error(
                    error=str(exc),
                    answer=state.answer,
                    resume=token,
                    usage=state.usage or None,
                )
            else:
                yield factory.completed_error(error=str(exc))
        finally:
            with anyio.move_on_after(self.close_timeout_s):
                await peer.close()
