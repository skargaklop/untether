"""Capability-gated ACP compaction over the new AcpPeer stack.

Replaces the legacy ``runners/_acp.py`` transport: compaction resumes the
session, waits for the agent to advertise its commands, and only issues the
``/compact`` prompt when the agent actually advertised a ``compact`` command.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from ...compact import CompactSupport, compact_prompt
from ...events import EventFactory
from ...model import EngineId, ResumeToken, UntetherEvent
from .protocol import ProtocolAdapter, V1Adapter, V2Adapter, negotiate
from .runner import AcpRunner


class AcpCompactMixin:
    """Compact via ACP ``session/prompt`` after capability-gating."""

    compact_accepts_instructions: bool = True

    def compact_support(self) -> CompactSupport:
        return CompactSupport(
            mode="acp",
            accepts_instructions=self.compact_accepts_instructions,  # type: ignore[attr-defined]
            true_compaction=True,
            note="ACP compact requires advertised compact command",
        )

    async def compact(
        self: AcpRunner,
        resume: ResumeToken,
        instructions: str | None = None,
    ) -> AsyncIterator[UntetherEvent]:
        engine: EngineId = self.engine  # type: ignore[attr-defined]
        factory = EventFactory(engine)
        if resume.engine != engine:
            yield factory.completed(
                ok=False,
                answer="",
                resume=resume,
                error=f"resume token engine {resume.engine!r} != runner engine {engine!r}",
            )
            return
        yield factory.started(
            resume,
            title=f"{engine} compact",
            meta={"compact": {"mode": "acp", "true_compaction": True}},
        )
        peer = self._peer()
        try:
            await peer.start()
            requested: ProtocolAdapter = (
                V2Adapter()
                if self.protocol == "auto"
                else (V1Adapter() if self.protocol == "1" else V2Adapter())  # type: ignore[attr-defined]
            )
            init = await peer.request(
                "initialize",
                requested.initialize_params(
                    self.facilities.capabilities(requested.version)  # type: ignore[attr-defined]
                    if self.facilities is not None  # type: ignore[attr-defined]
                    else {}
                ),
                timeout_s=self.startup_timeout_s,  # type: ignore[attr-defined]
            )
            adapter = negotiate(
                self.protocol,
                init,
                allow_v1=self.allow_v1,  # type: ignore[attr-defined]
            )
            peer.allow_batches = adapter.version == 2
            method = adapter.resume_method(init)
            created = await peer.request(
                method,
                {"sessionId": resume.value},
                timeout_s=self.request_timeout_s,  # type: ignore[attr-defined]
            )
            sid = created.get("sessionId")
            if not isinstance(sid, str) or not sid:
                raise RuntimeError("ACP session resume returned no sessionId")
            # Wait (bounded by the notification machinery) for the agent to
            # advertise its commands, then gate on a real "compact" command.
            commands: set[str] = set()
            while True:
                notification = await peer.next_notification()
                if notification is None:
                    raise RuntimeError("ACP agent did not advertise available commands")
                if notification.get("method") != "session/update":
                    continue
                params = notification.get("params", {})
                update = (
                    params.get("update", params) if isinstance(params, dict) else {}
                )
                if not isinstance(update, dict):
                    continue
                if update.get("sessionUpdate") not in {
                    "available_commands_update",
                    "availableCommands",
                }:
                    continue
                raw = update.get("availableCommands", [])
                if isinstance(raw, list):
                    for item in raw:
                        if isinstance(item, dict) and isinstance(item.get("name"), str):
                            commands.add(item["name"])
                break
            if "compact" not in commands:
                raise RuntimeError("ACP agent did not advertise 'compact'")
            await peer.request(
                "session/prompt",
                adapter.prompt_params(sid, compact_prompt(instructions)),
                timeout_s=self.turn_timeout_s,  # type: ignore[attr-defined]
            )
            yield factory.completed_ok(
                answer=f"{engine} compaction completed.",
                resume=resume,
            )
        except Exception as exc:  # noqa: BLE001 - completion must cover failure
            yield factory.completed(
                ok=False,
                answer="",
                resume=resume,
                error=str(exc),
            )
        finally:
            await peer.close()


__all__ = ["AcpCompactMixin", "negotiate"]
