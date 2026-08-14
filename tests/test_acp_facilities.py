from __future__ import annotations

import pytest

from untether.runners.acp.facilities import (
    AcpClientFacilities,
    RootFilesystem,
    TerminalExecutor,
)
from untether.runners.acp.interactions import InteractionBroker


def test_root_filesystem_reads_and_atomically_writes_only_inside_root(tmp_path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    fs = RootFilesystem([root])
    fs.write_text("file.txt", "hello")
    assert fs.read_text("file.txt") == "hello"
    with pytest.raises(PermissionError):
        fs.read_text("../outside.txt")
    with pytest.raises(PermissionError):
        fs.write_text(str(tmp_path / "escape.txt"), "no")
    assert not list(root.glob("*.tmp"))


@pytest.mark.anyio
async def test_terminal_is_argv_only_bounded_and_root_restricted(tmp_path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    terminal = TerminalExecutor([root], max_output=4)
    result = await terminal.run(["python", "-c", "print('0123456789')"], cwd=".")
    assert result.output == "0123"
    with pytest.raises(TypeError):
        await terminal.run("python -c pass", cwd=".")  # ty: ignore[invalid-argument-type]
    with pytest.raises(PermissionError):
        await terminal.run(["python", "-c", "pass"], cwd="..")


def test_facilities_advertise_enabled_v1_only() -> None:
    facilities = AcpClientFacilities(
        filesystem=RootFilesystem([]),
        terminal=TerminalExecutor([]),
        elicitation=True,
        broker=InteractionBroker(),
    )
    assert facilities.capabilities(1) == {
        "fs": {"readTextFile": True, "writeTextFile": True},
        "terminal": True,
        "elicitation": {"form": True, "url": True},
    }
    assert facilities.capabilities(2) == {}
    assert AcpClientFacilities().capabilities(1) == {}


@pytest.mark.anyio
async def test_elicitation_routes_through_broker() -> None:
    broker = InteractionBroker(timeout_s=1)
    facilities = AcpClientFacilities(broker=broker, elicitation=True)
    waiter = await facilities.elicit("owner", {"title": "Question"})
    assert broker.pending_count == 1
    await broker.resolve("owner", waiter.nonce, {"answer": "yes"})
    assert await waiter.wait() == {"answer": "yes"}
