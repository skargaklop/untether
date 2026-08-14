from __future__ import annotations

import asyncio
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
from ..run_options import get_run_options
from .interactions import InteractionBroker
from .peer import AcpPeer
from .protocol import Json, ProtocolAdapter, V1Adapter, V2Adapter, negotiate
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
    allow_v1: bool = True
    auth_method: str | None = None
    auto_auth: bool = False
    turn_timeout_s: float = 1800.0
    cancel_grace_s: float = 5.0
    request_timeout_s: float = 60.0
    close_timeout_s: float = 5.0
    config_option_map: dict[str, str] = field(default_factory=dict)
    peer_factory: Callable[[], Any] | None = None
    broker: InteractionBroker = field(default_factory=InteractionBroker)
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

    async def _prompt_with_updates(
        self,
        peer: Any,
        adapter: ProtocolAdapter,
        sid: str,
        prompt: str,
        state: AcpSessionState,
    ) -> tuple[Any, list[Any]]:
        """Keep consuming duplex traffic while the prompt request is pending."""

        async def request_prompt() -> Any:
            try:
                return await peer.request(
                    "session/prompt",
                    adapter.prompt_params(sid, prompt),
                    timeout_s=self.turn_timeout_s,
                )
            except TypeError as exc:
                if "timeout_s" not in str(exc):
                    raise
                return await peer.request(
                    "session/prompt", adapter.prompt_params(sid, prompt)
                )

        request = asyncio.create_task(request_prompt())
        notifications: set[asyncio.Task[Any]] = set()
        actions: list[Any] = []
        deadline = asyncio.get_running_loop().time() + self.turn_timeout_s
        try:
            notifications.add(asyncio.create_task(peer.next_notification()))
            while True:
                timeout = max(0.0, deadline - asyncio.get_running_loop().time())
                done, _ = await asyncio.wait(
                    {request, *notifications},
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
                            {request, *notifications},
                            timeout=remaining,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if not done:
                            raise TimeoutError(
                                f"ACP cancel grace expired after {self.cancel_grace_s}s"
                            )
                        for task in done:
                            if task is request:
                                return task.result(), actions
                            notifications.discard(task)
                            if task.exception() is not None:
                                continue
                            notifications.add(
                                asyncio.create_task(peer.next_notification())
                            )
                            item = task.result()
                            if item.get("method") == "session/update":
                                params = item.get("params", {})
                                actions.extend(
                                    state.apply(
                                        params if isinstance(params, dict) else {}
                                    )
                                )
                for task in done:
                    if task is request:
                        for notification in notifications:
                            if (
                                notification.done()
                                and not notification.cancelled()
                                and notification.exception() is None
                            ):
                                item = notification.result()
                                if item.get("method") == "session/update":
                                    params = item.get("params", {})
                                    actions.extend(
                                        state.apply(
                                            params if isinstance(params, dict) else {}
                                        )
                                    )
                        response = task.result()
                        if adapter.version != 2 or response.get("stopReason"):
                            return response, actions
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
                                if item.get("method") == "session/update":
                                    params = item.get("params", {})
                                    if isinstance(params, dict):
                                        actions.extend(state.apply(params))
                        if state.foreground_state == "running":
                            saw_running = True
                        if not notifications:
                            notifications.add(
                                asyncio.create_task(peer.next_notification())
                            )
                        if (
                            saw_running
                            and state.foreground_state == "idle"
                            and state.stop_reason is not None
                        ):
                            return response, actions
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
                                notifications,
                                timeout=remaining,
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if not done:
                                raise TimeoutError(
                                    f"ACP turn timeout after {self.turn_timeout_s}s"
                                )
                            for notification in done:
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
                                actions.extend(state.apply(params))
                                if state.foreground_state == "running":
                                    saw_running = True
                                elif (
                                    saw_running
                                    and state.foreground_state == "idle"
                                    and state.stop_reason is not None
                                ):
                                    return response, actions
                    notifications.discard(task)
                    if task.exception() is not None:
                        if request.done():
                            return request.result(), actions
                        continue
                    notifications.add(asyncio.create_task(peer.next_notification()))
                    item = task.result()
                    if item.get("method") == "session/update":
                        params = item.get("params", {})
                        actions.extend(
                            state.apply(params if isinstance(params, dict) else {})
                        )
        finally:
            if not request.done():
                request.cancel()
            for task in notifications:
                if not task.done():
                    task.cancel()
            await asyncio.gather(request, *notifications, return_exceptions=True)

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
            try:
                init = await peer.request(
                    "initialize",
                    requested_adapter.initialize_params(),
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
                init = await peer.request("initialize", V1Adapter().initialize_params())
            adapter = negotiate(self.protocol, init, allow_v1=self.allow_v1)
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
            params = (
                {"sessionId": resume.value}
                if resume is not None
                else {"cwd": self.cwd or str(Path.cwd())}
            )
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
            reverse_actions: list[Any] = []

            async def request_permission(params: Json) -> Json:
                pending = await self.broker.open(sid, "permission", params)
                reverse_actions.append(
                    factory.action_started(
                        action_id=pending.nonce,
                        kind="tool",
                        title="Permission requested",
                        detail={
                            "nonce": pending.nonce,
                            "options": params.get("options", []),
                        },
                    )
                )
                result = await pending.wait()
                return (
                    result if isinstance(result, dict) else {"approved": bool(result)}
                )

            register_handler = getattr(peer, "register_handler", None)
            if callable(register_handler):
                register_handler("session/request_permission", request_permission)
            meta = {"acp_protocol": adapter.version}
            if control is not None:
                meta["control"] = control
            yield factory.started(token, title=str(self.engine), meta=meta)
            started = True
            response, pending_actions = await self._prompt_with_updates(
                peer, adapter, sid, prompt, state
            )
            for action in reverse_actions + pending_actions:
                yield action
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
                await self.broker.cancel_owner(sid if token is not None else "")
            await peer.close()
