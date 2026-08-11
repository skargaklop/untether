from __future__ import annotations

from typing import cast

import pytest

from untether import engines
from untether.backends import EngineBackend
from untether.config import ConfigError
from untether.runner import Runner


class _EntryPoint:
    name = "demo"
    value = "package:BACKEND"


def _backend(engine_id: str = "demo") -> EngineBackend:
    return EngineBackend(
        id=engine_id,
        build_runner=lambda _config, _path: cast(Runner, object()),
        cli_cmd="demo",
        install_cmd="install demo",
    )


def test_validate_engine_backend_rejects_wrong_type_and_id() -> None:
    with pytest.raises(TypeError, match="EngineBackend"):
        engines._validate_engine_backend(object(), _EntryPoint())
    with pytest.raises(ValueError, match="does not match"):
        engines._validate_engine_backend(_backend("other"), _EntryPoint())


def test_get_backend_rejects_reserved_id(monkeypatch) -> None:
    monkeypatch.setattr(engines, "RESERVED_ENGINE_IDS", frozenset({"health"}))

    with pytest.raises(ConfigError, match="reserved"):
        engines.get_backend("HEALTH")


def test_get_backend_delegates_plugin_loading(monkeypatch) -> None:
    calls: list[tuple[object, ...]] = []

    def load(group, engine_id, **kwargs):
        calls.append((group, engine_id, kwargs["allowlist"], kwargs["kind_label"]))
        return _backend()

    monkeypatch.setattr(engines, "load_plugin_backend", load)
    backend = engines.get_backend("demo", allowlist=["demo"])
    assert backend.id == "demo"
    assert calls == [(engines.ENGINE_GROUP, "demo", ["demo"], "engine")]


def test_list_backends_skips_invalid_and_requires_one(monkeypatch) -> None:
    monkeypatch.setattr(engines, "list_backend_ids", lambda **_kwargs: ["good", "bad"])

    def get(engine_id, **_kwargs):
        if engine_id == "bad":
            raise ConfigError("invalid")
        return _backend("good")

    monkeypatch.setattr(engines, "get_backend", get)

    listed = engines.list_backends()
    assert [backend.id for backend in listed] == ["good"]

    monkeypatch.setattr(engines, "list_backend_ids", lambda **_kwargs: [])
    with pytest.raises(ConfigError, match="No engine"):
        engines.list_backends()


def test_list_backend_ids_passes_registry_constraints(monkeypatch) -> None:
    calls: list[tuple[object, ...]] = []

    def list_ids(group, **kwargs):
        calls.append((group, kwargs["allowlist"], kwargs["reserved_ids"]))
        return ["demo"]

    monkeypatch.setattr(engines, "list_ids", list_ids)

    assert engines.list_backend_ids(allowlist=["demo"]) == ["demo"]
    assert calls == [(engines.ENGINE_GROUP, ["demo"], engines.RESERVED_ENGINE_IDS)]
