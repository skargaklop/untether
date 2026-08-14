from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any

from ...events import EventFactory
from ...model import Action, ActionEvent, ActionKind, ActionPhase


@dataclass(slots=True)
class AcpSessionState:
    """Bounded, ordered projection of ACP session updates."""

    max_output: int = 64_000
    answer: str = ""
    usage: dict[str, Any] = field(default_factory=dict)
    foreground_state: str | None = None
    stop_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    messages: dict[str, dict[str, Any]] = field(default_factory=dict)
    unknown_updates: list[dict[str, Any]] = field(default_factory=list)
    actions: dict[str, Action] = field(default_factory=dict)
    _output: dict[str, str] = field(default_factory=dict)
    _replayed_answer: str = ""
    _factory: EventFactory = field(default_factory=lambda: EventFactory("acp"))

    def begin_prompt(self, replayed_answer: str = "") -> None:
        """Reset the answer while filtering history replayed by a resume."""
        self._replayed_answer = replayed_answer
        self.answer = ""
        self.foreground_state = None
        self.stop_reason = None

    def apply(self, update: dict[str, Any]) -> list[ActionEvent]:
        kind = update.get("sessionUpdate") or update.get("type") or update.get("update")
        if not isinstance(kind, str):
            return []
        if kind in {"state_update", "state", "session_state"}:
            state = update.get("state", update.get("currentState"))
            if isinstance(state, str):
                self.foreground_state = state
            reason = update.get("stopReason", update.get("stop_reason"))
            if reason is not None:
                self.stop_reason = str(reason)
            return []
        if kind in {"agent_message_chunk", "message", "text", "assistant_message"}:
            ident = str(
                update.get("messageId") or update.get("message_id") or "current"
            )
            text = self._text(update.get("content", update.get("text", "")))
            message = self.messages.setdefault(
                ident, {"content": "", "role": "assistant"}
            )
            message["role"] = update.get("role", message.get("role", "assistant"))
            if update.get("replace") or update.get("contentType") == "replace":
                message["content"] = text
            else:
                message["content"] = str(message.get("content", "")) + text
            if text and message["role"] == "assistant":
                if self._replayed_answer.startswith(text):
                    self._replayed_answer = self._replayed_answer[len(text) :]
                else:
                    self.answer += text
            return []
        if kind in {"metadata", "session_metadata"}:
            value = update.get("metadata", update.get("value", {}))
            if isinstance(value, dict):
                self.metadata.update(value)
            return []
        if kind in {"tool_call", "tool_call_update", "tool"}:
            ident = self._id(update, "toolCallId", "callId", "id")
            title = str(update.get("title") or update.get("name") or ident)
            detail = dict(update)
            action_kind: ActionKind = "file_change" if update.get("diff") else "tool"
            phase = (
                "completed"
                if update.get("status") in {"completed", "failed", "error"}
                else ("updated" if ident in self.actions else "started")
            )
            event = self._event(
                f"tool:{ident}",
                action_kind,
                title,
                detail,
                phase,
                phase != "completed",
                update,
            )
            return [event]
        if kind in {"plan", "plan_update"}:
            ident = str(update.get("planId") or "current")
            return [
                self._event(
                    f"plan:{ident}",
                    "turn",
                    "Plan",
                    dict(update),
                    "updated",
                    None,
                    update,
                )
            ]
        if kind in {"terminal_output", "terminal_chunk", "terminal"}:
            ident = self._id(update, "terminalId", "id")
            data = update.get("data", update.get("text", ""))
            if (
                update.get("encoding") == "base64"
                or update.get("dataEncoding") == "base64"
            ):
                try:
                    data = base64.b64decode(str(data)).decode("utf-8", "replace")
                except (ValueError, TypeError):
                    data = ""
            output = (self._output.get(ident, "") + str(data))[-self.max_output :]
            self._output[ident] = output
            return [
                self._event(
                    f"terminal:{ident}",
                    "command",
                    "Terminal",
                    {"output": output},
                    "updated",
                    None,
                    update,
                )
            ]
        if kind in {"usage_update", "usage"}:
            raw = update.get("usage")
            if isinstance(raw, dict):
                self.usage.setdefault("usage", {}).update(raw)
            cost = update.get("cost", update.get("totalCostUsd"))
            if isinstance(cost, (int, float)):
                self.usage["total_cost_usd"] = float(cost)
            return []
        if kind in {"session_end", "stop", "turn_complete", "error"}:
            return []
        self.unknown_updates.append(dict(update))
        return []

    def _event(
        self,
        ident: str,
        action_kind: ActionKind,
        title: str,
        detail: dict[str, Any],
        phase: ActionPhase,
        ok: bool | None,
        update: dict[str, Any],
    ) -> ActionEvent:
        if ident in self.actions:
            phase = "updated"
        action = Action(id=ident, kind=action_kind, title=title, detail=detail)
        self.actions[ident] = action
        return ActionEvent(
            engine=self._factory.engine,
            action=action,
            phase=phase,
            ok=(ok if phase == "completed" else None),
        )

    @staticmethod
    def _id(update: dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = update.get(key)
            if value is not None:
                return str(value)
        return "unknown"

    @staticmethod
    def _text(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            return str(value.get("text") or value.get("value") or "")
        return ""
