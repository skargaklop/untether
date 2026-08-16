from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any

from ...events import EventFactory
from ...model import Action, ActionEvent, ActionKind, ActionPhase
from .peer import AcpProtocolError

_absent = object()


@dataclass(slots=True)
class AcpMessageLedger:
    """Bounded single-source ledger for ordered assistant message projections."""

    max_answer: int = 64_000
    max_message_content: int = 64_000
    max_messages: int = 256
    messages: dict[str, dict[str, Any]] = field(default_factory=dict)
    answer_parts: dict[str, str] = field(default_factory=dict)

    @property
    def answer(self) -> str:
        return "".join(self.answer_parts.values())[-self.max_answer :]

    def update(
        self,
        ident: str,
        role: Any,
        text: str,
        *,
        replace: bool = False,
        include_answer: bool = True,
        patch: dict[str, Any] | None = None,
    ) -> None:
        """Record a message projection part.

        Chunk (default) mode appends *text*; full mode (``replace=True``) swaps it.
        *patch* applies ACP v2 merge semantics to the message dict: omitted key
        leaves the field unchanged, explicit ``None`` clears it, any other value
        sets it. Exactly one of *patch* and ``text`` is the active operation.
        """
        message = self.messages.setdefault(ident, {"content": "", "role": "assistant"})
        message["role"] = role
        if patch is not None:
            content_value = patch.get("content", _absent)
            if content_value is not _absent:
                content = "" if content_value is None else str(content_value)
                content = content[-self.max_message_content :]
                message["content"] = content
                if role == "assistant":
                    self.answer_parts[ident] = content[-self.max_answer :]
                else:
                    self.answer_parts.pop(ident, None)
            if "role" in patch and patch["role"] is not None:
                message["role"] = patch["role"]
            self._check_capacity()
            return
        previous = str(message.get("content", ""))
        content = text if replace else previous + text
        message["content"] = content[-self.max_message_content :]
        if role == "assistant" and include_answer:
            previous_part = self.answer_parts.get(ident, "")
            part = text if replace else previous_part + text
            self.answer_parts[ident] = part[-self.max_answer :]
        elif role != "assistant":
            self.answer_parts.pop(ident, None)
        self._check_capacity()

    def _check_capacity(self) -> None:
        if len(self.messages) > self.max_messages:
            raise AcpProtocolError("ACP state aggregate overflow: messages")


@dataclass(slots=True)
class AcpSessionState:
    """Bounded, ordered projection of ACP session updates."""

    max_output: int = 64_000
    max_answer: int = 64_000
    max_message_content: int = 64_000
    max_messages: int = 256
    max_actions: int = 256
    max_unknown_updates: int = 64
    answer: str = ""
    usage: dict[str, Any] = field(default_factory=dict)
    foreground_state: str | None = None
    has_been_running: bool = False
    stop_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    messages: dict[str, dict[str, Any]] = field(default_factory=dict)
    unknown_updates: list[dict[str, Any]] = field(default_factory=list)
    actions: dict[str, Action] = field(default_factory=dict)
    _output: dict[str, str] = field(default_factory=dict)
    _replayed_answer: str = ""
    available_commands: set[str] = field(default_factory=set)
    config_options: list[dict[str, Any]] = field(default_factory=list)
    mode: str | None = None
    _tool_content: dict[str, str] = field(default_factory=dict)
    _thought_parts: dict[str, str] = field(default_factory=dict)
    _message_ledger: AcpMessageLedger = field(init=False)
    _factory: EventFactory = field(default_factory=lambda: EventFactory("acp"))

    def __post_init__(self) -> None:
        self._message_ledger = AcpMessageLedger(
            max_answer=self.max_answer,
            max_message_content=self.max_message_content,
            max_messages=self.max_messages,
            messages=self.messages,
        )

    def begin_prompt(self, replayed_answer: str = "") -> None:
        """Reset the answer while filtering history replayed by a resume."""
        self._replayed_answer = replayed_answer
        self.answer = ""
        self._message_ledger.answer_parts.clear()
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
                if state == "running":
                    self.has_been_running = True
            reason = update.get("stopReason", update.get("stop_reason"))
            if reason is not None:
                self.stop_reason = str(reason)
            return []
        if kind in {
            "agent_message",
            "user_message",
            "agent_message_chunk",
            "user_message_chunk",
            "message",
            "text",
            "assistant_message",
        }:
            ident = str(
                update.get("messageId") or update.get("message_id") or "current"
            )
            text = self._text(update.get("content", update.get("text", "")))
            role = update.get(
                "role", self.messages.get(ident, {}).get("role", "assistant")
            )
            is_user = kind in {"user_message", "user_message_chunk"} or (
                isinstance(role, str) and role == "user"
            )
            if is_user:
                role = "user"
            if kind == "agent_message":
                patch = {"content": text} if "content" in update else {}
                self._message_ledger.update(ident, role, "", patch=patch)
            else:
                replacing = (
                    update.get("replace") or update.get("contentType") == "replace"
                )
                include_answer = True
                if (
                    text
                    and role == "assistant"
                    and self._replayed_answer.startswith(text)
                ):
                    self._replayed_answer = self._replayed_answer[len(text) :]
                    include_answer = False
                self._message_ledger.update(
                    ident,
                    role,
                    text,
                    replace=replacing,
                    include_answer=include_answer,
                )
            self.messages = self._message_ledger.messages
            self.answer = self._message_ledger.answer
            return []
        if kind in {"agent_thought", "agent_thought_chunk"}:
            ident = str(
                update.get("messageId") or update.get("message_id") or "thought"
            )
            text = self._text(update.get("content", update.get("text", "")))
            if kind == "agent_thought":
                part = text
            else:
                part = (self._thought_parts.get(ident, "") + text)[-self.max_output :]
            self._thought_parts[ident] = part
            return [
                self._event(
                    f"thought:{ident}",
                    "note",
                    part or "Thinking",
                    {"messageId": ident, "content": part}
                    if part
                    else {"messageId": ident},
                    "updated" if f"thought:{ident}" in self.actions else "started",
                    None,
                    update,
                )
            ]
        if kind in {"metadata", "session_metadata"}:
            value = update.get("metadata", update.get("value", {}))
            if isinstance(value, dict):
                self.metadata.update(value)
            return []
        if kind in {"tool_call", "tool_call_update", "tool"}:
            if not (
                update.get("toolCallId") or update.get("callId") or update.get("id")
            ):
                raise AcpProtocolError(f"ACP malformed {kind}: missing toolCallId")
            ident = self._id(update, "toolCallId", "callId", "id")
            action_id = f"tool:{ident}"
            existing = self.actions.get(action_id)
            if "title" in update:
                title = str(update["title"]) if update["title"] is not None else ident
            elif "name" in update and update["name"] is not None:
                title = str(update["name"])
            elif existing is not None:
                title = existing.title
            else:
                title = ident
            detail = dict(update)
            phase: ActionPhase
            if update.get("status") in {"completed", "failed", "error"}:
                phase = "completed"
            else:
                phase = "updated" if existing is not None else "started"
            action_kind: ActionKind = "file_change" if detail.get("diff") else "tool"
            event = self._event(
                action_id,
                action_kind,
                title,
                detail,
                phase,
                phase != "completed",
                update,
            )
            return [event]
        if kind == "tool_call_content_chunk":
            if not (
                update.get("toolCallId") or update.get("callId") or update.get("id")
            ):
                raise AcpProtocolError(
                    "ACP malformed tool_call_content_chunk: missing toolCallId"
                )
            ident = self._id(update, "toolCallId", "callId", "id")
            action_id = f"tool:{ident}"
            existing = self.actions.get(action_id)
            content = self._text(update.get("content", ""))
            acc = (self._tool_content.get(ident, "") + content)[-self.max_output :]
            self._tool_content[ident] = acc
            if existing is None:
                detail = {"title": ident, "content": acc}
            else:
                detail = dict(existing.detail)
                detail["content"] = acc
            return [
                self._event(
                    action_id,
                    "tool",
                    str(detail.get("title") or ident),
                    detail,
                    "updated" if existing is not None else "started",
                    None,
                    update,
                )
            ]
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
        if kind in {
            "terminal_output",
            "terminal_chunk",
            "terminal",
            "terminal_output_chunk",
        }:
            if kind == "terminal_output_chunk" and not (
                update.get("terminalId") or update.get("id")
            ):
                raise AcpProtocolError(
                    "ACP malformed terminal_output_chunk: missing terminalId"
                )
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
        if kind == "terminal_update":
            if not (update.get("terminalId") or update.get("id")):
                raise AcpProtocolError(
                    "ACP malformed terminal_update: missing terminalId"
                )
            ident = self._id(update, "terminalId", "id")
            action_id = f"terminal:{ident}"
            phase = (
                "completed"
                if update.get("status") == "exited"
                else ("updated" if action_id in self.actions else "started")
            )
            return [
                self._event(
                    action_id,
                    "command",
                    "Terminal",
                    dict(update),
                    phase,
                    phase != "completed",
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
        if kind == "session_info_update":
            meta = update.get("metadata")
            if isinstance(meta, dict):
                self.metadata.update(meta)
            title = update.get("title")
            if title is not None:
                self.metadata["title"] = str(title)
            return []
        if kind == "available_commands_update":
            commands = update.get("availableCommands", [])
            if isinstance(commands, list):
                for item in commands:
                    if isinstance(item, dict) and isinstance(item.get("name"), str):
                        self.available_commands.add(item["name"])
            return []
        if kind == "config_option_update":
            options = update.get("configOptions")
            if isinstance(options, list):
                self.config_options.extend(
                    dict(item) for item in options if isinstance(item, dict)
                )
            else:
                self.config_options.append(dict(update))
            if len(self.config_options) > self.max_unknown_updates:
                del self.config_options[: -self.max_unknown_updates]
            return []
        if kind in {"mode_update", "current_mode_update"}:
            mode = update.get("mode", update.get("currentMode", update.get("value")))
            if mode is not None and not isinstance(mode, (dict, list)):
                self.mode = str(mode)
            return []
        self.unknown_updates.append(dict(update))
        if len(self.unknown_updates) > self.max_unknown_updates:
            raise AcpProtocolError("ACP state aggregate overflow: unknown_updates")
        return []

    def _trim_actions(self) -> None:
        if len(self.actions) <= self.max_actions:
            return
        raise AcpProtocolError("ACP state aggregate overflow: actions")

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
        if ident in self.actions and phase != "completed":
            phase = "updated"
        action = Action(id=ident, kind=action_kind, title=title, detail=detail)
        self.actions[ident] = action
        self._trim_actions()
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
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                if isinstance(item, dict):
                    block = item.get("text") or item.get("value")
                    if block is not None:
                        parts.append(str(block))
                elif item is not None:
                    parts.append(str(item))
            return "".join(parts)
        return ""
