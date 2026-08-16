from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
from structlog import get_logger

from ...events import EventFactory
from ...model import EngineId, ResumeToken
from ...runner import ResumeTokenMixin
from ..run_options import get_run_options
from .facilities import AcpClientFacilities
from .interactions import InteractionBroker
from .peer import AcpPeer
from .protocol import Json, ProtocolAdapter, V1Adapter, V2Adapter, negotiate
from .state import AcpSessionState
from .turn import AcpTurnControl

logger = get_logger(__name__)


@dataclass(slots=True)
class AcpRunner(ResumeTokenMixin):
    engine: EngineId
    command: str
    args: list[str] = field(default_factory=list)
    cwd: str | None = None
    env: dict[str, str] | None = None
    protocol: str = "auto"
    allow_v1: bool = True
    auth_method: str | None = None
    auto_auth: bool = False
    turn_timeout_s: float = 1800.0
    cancel_grace_s: float = 5.0
    request_timeout_s: float = 60.0
    startup_timeout_s: float = 30.0
    close_timeout_s: float = 5.0
    mcp_servers: list[dict] = field(default_factory=list)
    config_option_map: dict[str, str] = field(default_factory=dict)
    peer_factory: Callable[[], Any] | None = None
    broker: InteractionBroker = field(default_factory=InteractionBroker)
    facilities: AcpClientFacilities | None = None
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
        agen = self._run(prompt, resume)
        try:
            async for event in agen:
                yield event
        finally:
            # Forward aclose() to the inner generator so its teardown
            # (owner-cancel + peer close) runs even when this wrapper is
            # closed mid-yield.
            await agen.aclose()

    async def _stream_turn(
        self,
        peer: Any,
        adapter: ProtocolAdapter,
        sid: str,
        prompt: str,
        state: AcpSessionState,
        turn_outcome: dict[str, Any],
        extra_events: asyncio.Queue,
    ) -> AsyncIterator[Any]:
        """Stream ActionEvents as the prompt request arrives, mid-turn.

        Async generators cannot return values, so completion is reported through
        ``turn_outcome["response"]``. ``extra_events`` (an ``asyncio.Queue`` fed
        by reverse-handler side effects such as permission requests) joins the
        wait set via a standing ``extra_events.get()`` task so those events
        surface mid-turn; each is yielded as-is and the task re-armed. The v2
        acceptance gate and the cancel-grace path are preserved.
        """

        async def request_prompt() -> Any:
            return await peer.request(
                "session/prompt",
                adapter.prompt_params(sid, prompt),
                timeout_s=(
                    self.request_timeout_s
                    if adapter.version == 2
                    else self.turn_timeout_s
                ),
            )

        def finish(response: Json) -> None:
            turn_outcome["response"] = response

        def apply_actions(item: Any) -> list[Any]:
            if item.get("method") == "session/update":
                params = item.get("params", {})
                if isinstance(params, dict):
                    return state.apply(params)
            return []

        _absent = object()
        request = asyncio.create_task(request_prompt())
        notifications: set[asyncio.Task[Any]] = set()
        extra_tasks: set[asyncio.Task[Any]] = set()
        extra_open = True
        deadline = asyncio.get_running_loop().time() + self.turn_timeout_s

        def arm_extra() -> None:
            if extra_open:
                extra_tasks.add(asyncio.create_task(extra_events.get()))

        def take_extra(task: asyncio.Task[Any]) -> Any:
            """Consume a finished extra_events task; return its event or _absent."""
            nonlocal extra_open
            if task not in extra_tasks:
                return _absent
            extra_tasks.discard(task)
            if task.exception() is not None:
                extra_open = False
                return _absent
            arm_extra()
            return task.result()

        arm_extra()
        try:
            notifications.add(asyncio.create_task(peer.next_notification()))
            while True:
                timeout = max(0.0, deadline - asyncio.get_running_loop().time())
                done, _ = await asyncio.wait(
                    {request, *notifications, *extra_tasks},
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    await peer.notify("session/cancel", {"sessionId": sid})
                    cancel_deadline = (
                        asyncio.get_running_loop().time() + self.cancel_grace_s
                    )
                    while True:
                        remaining = max(
                            0.0,
                            cancel_deadline - asyncio.get_running_loop().time(),
                        )
                        if remaining == 0:
                            raise TimeoutError(
                                f"ACP cancel grace expired after {self.cancel_grace_s}s"
                            )
                        done, _ = await asyncio.wait(
                            {request, *notifications, *extra_tasks},
                            timeout=remaining,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if not done:
                            raise TimeoutError(
                                f"ACP cancel grace expired after {self.cancel_grace_s}s"
                            )
                        for task in done:
                            extra = take_extra(task)
                            if extra is not _absent:
                                if extra is not None:
                                    yield extra
                                continue
                            if task is request:
                                finish(task.result())
                                return
                            notifications.discard(task)
                            if task.exception() is not None:
                                continue
                            notifications.add(
                                asyncio.create_task(peer.next_notification())
                            )
                            for action in apply_actions(task.result()):
                                yield action
                for task in done:
                    extra = take_extra(task)
                    if extra is not _absent:
                        if extra is not None:
                            yield extra
                        continue
                    if task is request:
                        for notification in notifications:
                            if (
                                notification.done()
                                and not notification.cancelled()
                                and notification.exception() is None
                            ):
                                for action in apply_actions(notification.result()):
                                    yield action
                        response = task.result()
                        if adapter.version != 2 or response.get("stopReason"):
                            finish(response)
                            return
                        # ACP v2 acknowledges prompt acceptance separately; only a
                        # post-acceptance running -> idle transition completes it.
                        saw_running = False
                        for notification in list(notifications):
                            if (
                                notification.done()
                                and not notification.cancelled()
                                and notification.exception() is None
                            ):
                                item = notification.result()
                                notifications.discard(notification)
                                for action in apply_actions(item):
                                    yield action
                        if state.foreground_state == "running":
                            saw_running = True
                        if not notifications:
                            notifications.add(
                                asyncio.create_task(peer.next_notification())
                            )
                        if (
                            (saw_running or state.has_been_running)
                            and state.foreground_state == "idle"
                            and state.stop_reason is not None
                        ):
                            finish(response)
                            return
                        while True:
                            remaining = max(
                                0.0,
                                deadline - asyncio.get_running_loop().time(),
                            )
                            if not remaining:
                                raise TimeoutError(
                                    f"ACP turn timeout after {self.turn_timeout_s}s"
                                )
                            done, _ = await asyncio.wait(
                                {*notifications, *extra_tasks},
                                timeout=remaining,
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if not done:
                                raise TimeoutError(
                                    f"ACP turn timeout after {self.turn_timeout_s}s"
                                )
                            for notification in done:
                                extra = take_extra(notification)
                                if extra is not _absent:
                                    if extra is not None:
                                        yield extra
                                    continue
                                notifications.discard(notification)
                                if notification.exception() is not None:
                                    continue
                                item = notification.result()
                                notifications.add(
                                    asyncio.create_task(peer.next_notification())
                                )
                                if item.get("method") != "session/update":
                                    continue
                                params = item.get("params", {})
                                if not isinstance(params, dict):
                                    continue
                                for action in state.apply(params):
                                    yield action
                                if state.foreground_state == "running":
                                    saw_running = True
                                elif (
                                    (saw_running or state.has_been_running)
                                    and state.foreground_state == "idle"
                                    and state.stop_reason is not None
                                ):
                                    finish(response)
                                    return
                    notifications.discard(task)
                    if task.exception() is not None:
                        if request.done():
                            finish(request.result())
                            return
                        continue
                    notifications.add(asyncio.create_task(peer.next_notification()))
                    for action in apply_actions(task.result()):
                        yield action
        finally:
            if not request.done():
                request.cancel()
            for task in notifications:
                if not task.done():
                    task.cancel()
            # Reap any armed-but-unconsumed extra_events getters: a get() task
            # may have completed concurrently with the prompt response and had
            # its event consumed from the queue without being yielded. Return
            # those events via turn_outcome so run() emits them before completion.
            for task in extra_tasks:
                if task.done() and not task.cancelled() and task.exception() is None:
                    turn_outcome.setdefault("missed_extra_events", []).append(
                        task.result()
                    )
                elif not task.done():
                    task.cancel()
            await asyncio.gather(
                request, *notifications, *extra_tasks, return_exceptions=True
            )

    async def _apply_run_options(
        self,
        peer: Any,
        adapter: ProtocolAdapter,
        session_id: str,
        session: Json,
        resumed: bool,
    ) -> None:
        options = get_run_options()
        if options is None:
            return
        overrides: dict[str, str] = {}
        if options.model:
            overrides["model"] = options.model
        if options.reasoning:
            overrides["reasoning"] = options.reasoning
        if options.permission_mode:
            overrides["permission_mode"] = options.permission_mode
        if options.plan:
            overrides["plan"] = "plan"
        available = adapter.config_options(session)
        by_category = {
            str(item.get("category")): item
            for item in available
            if isinstance(item, dict)
        }
        for name, value in overrides.items():
            config_id = self.config_option_map.get(name)
            if config_id is None:
                if name == "model":
                    item = by_category.get("model_config")
                elif name == "reasoning":
                    item = by_category.get("thought_level")
                else:
                    item = None
                config_id = str(item.get("id")) if item and item.get("id") else None
            if config_id is None:
                raise RuntimeError(f"configure: ACP cannot map {name} override")
            item = next(
                (
                    candidate
                    for candidate in available
                    if candidate.get("id") == config_id
                ),
                None,
            )
            values = item.get("options", []) if item else []
            if resumed and name == "model" and value not in values:
                raise RuntimeError("configure: resume model override is unavailable")
            if values and value not in values:
                raise RuntimeError(f"configure: ACP rejected {name} override")
            await peer.request(
                "session/set_config_option",
                {
                    "sessionId": session_id,
                    "configId": adapter.config_key(config_id),
                    "value": value,
                },
            )

    async def _run(self, prompt: str, resume: ResumeToken | None):
        factory = EventFactory(self.engine)
        if resume is not None and resume.engine != self.engine:
            yield factory.completed_error(
                error=(
                    f"ACP resume token is for engine {resume.engine!r}, "
                    f"not {self.engine!r}"
                )
            )
            return
        peer = self._peer()
        state = AcpSessionState(_factory=factory)
        token: ResumeToken | None = None
        started = False
        try:
            await peer.start()
            requested_adapter = (
                V2Adapter()
                if self.protocol == "auto"
                else (V1Adapter() if self.protocol == "1" else V2Adapter())
            )
            client_capabilities = (
                self.facilities.capabilities(requested_adapter.version)
                if self.facilities is not None
                else {}
            )
            try:
                init = await peer.request(
                    "initialize",
                    requested_adapter.initialize_params(client_capabilities),
                    timeout_s=self.startup_timeout_s,
                )
            except (RuntimeError, TimeoutError) as exc:
                message = str(exc).lower()
                can_fallback = (
                    self.protocol == "auto"
                    and self.allow_v1
                    and any(
                        marker in message
                        for marker in (
                            "unsupported protocol",
                            "protocol version",
                            "rejected",
                            "reached eof",
                        )
                    )
                )
                if not can_fallback:
                    raise
                await peer.close()
                peer = self._peer()
                await peer.start()
                init = await peer.request(
                    "initialize",
                    V1Adapter().initialize_params(
                        self.facilities.capabilities(1)
                        if self.facilities is not None
                        else {}
                    ),
                    timeout_s=self.startup_timeout_s,
                )
            adapter = negotiate(self.protocol, init, allow_v1=self.allow_v1)
            peer.allow_batches = adapter.version == 2
            advertised = init.get("authMethods", init.get("auth_methods", []))
            auth_ids = (
                {
                    str(item.get("id", item.get("method", "")))
                    if isinstance(item, dict)
                    else str(item)
                    for item in advertised
                    if isinstance(item, (dict, str))
                }
                if isinstance(advertised, list)
                else set()
            )
            selected_auth = self.auth_method
            if selected_auth is not None and selected_auth not in auth_ids:
                raise RuntimeError(
                    f"authenticate: ACP auth method unavailable: {selected_auth}"
                )
            if selected_auth is not None:
                await peer.request(
                    adapter.authenticate_method(), {"methodId": selected_auth}
                )
            method = (
                adapter.resume_method(init) if resume is not None else "session/new"
            )
            params: dict[str, Any] = (
                {"sessionId": resume.value}
                if resume is not None
                else {"cwd": self.cwd or str(Path.cwd())}
            )
            if resume is None and self.mcp_servers:
                params["mcpServers"] = self.mcp_servers
            try:
                created = await peer.request(method, params)
            except RuntimeError as exc:
                if not self.auto_auth or "auth_required" not in str(exc):
                    raise
                if len(auth_ids) != 1:
                    raise RuntimeError(
                        f"authenticate: ACP auth required; eligible methods: {sorted(auth_ids)}"
                    ) from exc
                await peer.request(
                    adapter.authenticate_method(), {"methodId": next(iter(auth_ids))}
                )
                created = await peer.request(method, params)
            sid = created.get("sessionId")
            if not isinstance(sid, str) or not sid:
                raise RuntimeError("ACP session creation returned no sessionId")
            token = ResumeToken(self.engine, sid)
            await self._apply_run_options(
                peer, adapter, sid, created, resume is not None
            )
            if resume is not None:
                state.begin_prompt(state.answer)
            notify = getattr(peer, "notify", None)
            control = AcpTurnControl(notify, sid) if callable(notify) else None
            extra_events: asyncio.Queue = asyncio.Queue()
            turn_outcome: dict[str, Any] = {}

            async def request_permission(params: Json) -> Json:
                options = [
                    opt
                    for opt in params.get("options", [])
                    if isinstance(opt, dict) and opt.get("optionId")
                ]
                pending = await self.broker.open(sid, "permission", params)
                buttons = [
                    {
                        "text": str(opt.get("name") or opt["optionId"]),
                        "callback_data": f"acp_control:{pending.nonce}:{opt['optionId']}",
                    }
                    for opt in options
                ]
                extra_events.put_nowait(
                    factory.action_started(
                        action_id=pending.nonce,
                        kind="tool",
                        title=str(params.get("title") or "Permission requested"),
                        detail={
                            "nonce": pending.nonce,
                            "options": options,
                            "inline_keyboard": {"buttons": [[b] for b in buttons]},
                        },
                    )
                )
                try:
                    result = await pending.wait()
                except TimeoutError:
                    logger.warning(
                        "acp.permission_timeout",
                        engine=str(self.engine),
                        session=sid,
                        nonce=pending.nonce,
                    )
                    extra_events.put_nowait(
                        factory.action_completed(
                            action_id=pending.nonce,
                            kind="tool",
                            title="Permission requested",
                            ok=False,
                            message="timed out",
                        )
                    )
                    return {"outcome": "cancelled"}
                if isinstance(result, dict) and result.get("outcome") == "selected":
                    outcome = {
                        "outcome": "selected",
                        "optionId": str(result.get("optionId", "")),
                    }
                    ok = True
                else:
                    outcome = {"outcome": "cancelled"}
                    ok = result is not None
                    result = None
                extra_events.put_nowait(
                    factory.action_completed(
                        action_id=pending.nonce,
                        kind="tool",
                        title="Permission requested",
                        ok=ok,
                    )
                )
                return outcome

            register_handler = getattr(peer, "register_handler", None)
            if callable(register_handler):
                register_handler("session/request_permission", request_permission)
            meta = {"acp_protocol": adapter.version}
            meta["acp_commands"] = sorted(state.available_commands)
            if control is not None:
                meta["control"] = control
            yield factory.started(token, title=str(self.engine), meta=meta)
            started = True
            async for event in self._stream_turn(
                peer, adapter, sid, prompt, state, turn_outcome, extra_events
            ):
                yield event
            for missed in turn_outcome.pop("missed_extra_events", []):
                yield missed
            while not extra_events.empty():
                yield extra_events.get_nowait()
            response = turn_outcome["response"]
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
            if control is not None:
                control.can_steer = any(
                    command.startswith("message:")
                    for command in state.available_commands
                )
            answer = state.answer
            reason = str(response.get("stopReason", ""))
            ok = reason not in {"error", "failed"}
            if ok and adapter.supports_session_close(init):
                with suppress(Exception):
                    await peer.request(
                        "session/close",
                        {"sessionId": sid},
                        timeout_s=self.close_timeout_s,
                    )
            yield factory.completed(
                ok=ok,
                answer=answer,
                resume=token,
                error=None if ok else reason,
                usage=state.usage or None,
            )
        except GeneratorExit:
            # aclose() mid-run: teardown must not attempt further yields
            # (async generators must not ignore GeneratorExit).
            raise
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
            # Teardown must not suspend between GeneratorExit and completion:
            # use the synchronous owner-cancel so pending interactions are
            # resolved (reverse handlers wake and flush a -32800/-32603 error
            # response to the agent), then close the peer.
            self.broker.cancel_owner_nowait(sid if token is not None else "")
            await peer.close()
