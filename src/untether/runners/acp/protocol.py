from __future__ import annotations

from dataclasses import dataclass
from typing import Any

Json = dict[str, Any]


class ProtocolNegotiationError(RuntimeError):
    """Raised when an agent selects a disallowed ACP version."""


class ProtocolAdapter:
    version: int

    def initialize_params(self) -> Json:
        raise NotImplementedError

    def authenticate_method(self) -> str:
        return "authenticate" if self.version == 1 else "auth/login"

    def prompt_params(self, session_id: str, text: str) -> Json:
        return {"sessionId": session_id, "prompt": [{"type": "text", "text": text}]}

    def config_key(self, name: str) -> str:
        return name

    def config_options(self, session: Json) -> list[Json]:
        value = session.get("configOptions", session.get("config_options", []))
        return value if isinstance(value, list) else []

    def responses(self, message: list[Json] | Json) -> dict[Any, Json]:
        items = message if isinstance(message, list) else [message]
        return {
            item["id"]: item["result"]
            for item in items
            if "id" in item and isinstance(item.get("result"), dict)
        }


@dataclass(frozen=True)
class V1Adapter(ProtocolAdapter):
    version: int = 1

    def initialize_params(self) -> Json:
        return {
            "protocolVersion": 1,
            "clientInfo": {"name": "untether", "version": "0"},
            "clientCapabilities": {},
        }


@dataclass(frozen=True)
class V2Adapter(ProtocolAdapter):
    version: int = 2

    def initialize_params(self) -> Json:
        return {
            "protocolVersion": 2,
            "info": {"name": "untether", "version": "0"},
            "capabilities": {},
        }

    def config_key(self, name: str) -> str:
        return "configId" if name else name


def negotiate(mode: str, result: Json, *, allow_v1: bool = True) -> ProtocolAdapter:
    selected = result.get("protocolVersion")
    if mode == "auto":
        if selected == 2:
            return V2Adapter()
        if selected == 1 and allow_v1:
            return V1Adapter()
    elif mode == "1" and selected == 1:
        return V1Adapter()
    elif mode == "2" and selected == 2:
        return V2Adapter()
    raise ProtocolNegotiationError(
        f"ACP selected unsupported or disallowed protocol version: {selected}"
    )
