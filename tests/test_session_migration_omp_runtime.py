"""Installed OMP 18.1.6 SDK persistence/cold-load and adapter roundtrip evidence.

OMP v3 is not Pi v3: its title slot, sibling artifacts and global blob refs
require a separate transfer contract. This text/tool-only probe tests no asset
transfer. Both processes use disposable state and no agent/model/provider API.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path

import pytest

BUN = Path(r"C:\Users\DELL E5570\.bun\bin\bun.exe")
PACKAGE = Path(
    r"C:\Users\DELL E5570\.bun\install\global\node_modules"
    r"\@oh-my-pi\pi-coding-agent"
)
PROBE = Path(__file__).parent / "fixtures" / "omp_persistence_probe.ts"
TIMEOUT_S = 25
OUTPUT_LIMIT = 64 * 1024


def _environment(root: Path) -> dict[str, str]:
    # Allowlist rather than inherit credentials, profiles, preload options or
    # agent-dir overrides. dirs.ts uses PI_CONFIG_DIR + os.homedir(), honours
    # PI_CODING_AGENT_DIR only in the default OMP_PROFILE/PI_PROFILE, and uses
    # XDG roots on Unix. There is no OMP_AGENT_DIR override in this version.
    env = {
        key: os.environ[key]
        for key in ("PATH", "SYSTEMROOT", "WINDIR", "COMSPEC")
        if key in os.environ
    }
    for key, relative in {
        "HOME": "home",
        "USERPROFILE": "home",
        "XDG_CONFIG_HOME": "xdg-config",
        "XDG_DATA_HOME": "xdg-data",
        "XDG_STATE_HOME": "xdg-state",
        "XDG_CACHE_HOME": "xdg-cache",
        "APPDATA": "appdata",
        "LOCALAPPDATA": "localappdata",
        "TEMP": "tmp",
        "TMP": "tmp",
        "PI_CODING_AGENT_DIR": "agent",
    }.items():
        directory = root / relative
        directory.mkdir(exist_ok=True)
        env[key] = str(directory)
    env.update(PI_CONFIG_DIR=".omp", OMP_PROFILE="", PI_PROFILE="", PI_OFFLINE="1")
    return env


def _run(mode: str, root: Path, env: dict[str, str], *args: str) -> dict:
    """Cap each pipe while draining; timeout/overflow kills only this child."""
    command = [str(BUN), str(PROBE), mode, str(PACKAGE), str(root), *args]
    chunks = [bytearray(), bytearray()]
    overflow = threading.Event()
    with subprocess.Popen(
        command,
        cwd=root,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as child:

        def drain(stream, output: bytearray) -> None:
            while data := stream.read(4096):
                remaining = OUTPUT_LIMIT - len(output)
                output.extend(data[:remaining])
                if len(data) > remaining:
                    overflow.set()
                    child.kill()
                    break

        assert child.stdout is not None and child.stderr is not None
        readers = [
            threading.Thread(target=drain, args=(stream, output), daemon=True)
            for stream, output in zip((child.stdout, child.stderr), chunks, strict=True)
        ]
        for reader in readers:
            reader.start()
        timed_out = False
        try:
            child.wait(timeout=TIMEOUT_S)
        except subprocess.TimeoutExpired:
            timed_out = True
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            for reader in readers:
                reader.join(timeout=5)
        stdout, stderr = (
            bytes(chunk).decode("utf-8", errors="replace") for chunk in chunks
        )
        diagnostic = (
            f"OMP {mode}: rc={child.returncode}; stderr={stderr}; stdout={stdout}"
        )
        assert not timed_out, f"timeout after {TIMEOUT_S}s; {diagnostic}"
        assert not overflow.is_set(), (
            f"output exceeded {OUTPUT_LIMIT} bytes; {diagnostic}"
        )
        assert all(not reader.is_alive() for reader in readers), diagnostic
        assert child.returncode == 0, diagnostic
    return json.loads(stdout)


def test_omp_public_sdk_persistence_coldload(tmp_path: Path) -> None:
    if not BUN.is_file():
        pytest.skip(f"installed Bun absent: {BUN}")
    if not (PACKAGE / "package.json").is_file():
        pytest.skip(f"installed OMP package absent: {PACKAGE}")
    env = _environment(tmp_path)
    written = _run("write", tmp_path, env)
    record = Path(written["file"])
    assert record.is_relative_to(tmp_path / "sessions")
    before = record.read_bytes()
    first_line = before.split(b"\n", 1)[0]
    assert len(first_line) + 1 == 256
    assert json.loads(first_line)["type"] == "title"
    # A new OS process, not another manager in the writer's module/cache.
    loaded = _run("read", tmp_path, env, str(record))
    assert loaded == written  # native ID, exact file, ordered entries and tool IDs
    assert record.read_bytes() == before
    assert not list(tmp_path.rglob("terminal-sessions/*"))


def test_omp_adapter_export_import_sdk_coldload(tmp_path: Path, monkeypatch) -> None:
    """SDK write -> production export/import -> independent SDK cold read.

    This proves historical persistence, not model continuation or SDK append.
    """
    from untether.session_migration.native_adapter import assert_exact_restore
    from untether.session_migration.omp_adapter import OmpPersistenceAdapter
    from untether.session_migration.settings import OmpPersistenceProfile

    monkeypatch.setenv("UNTETHER_SESSION_GATE_ROOT", str(tmp_path / "gates"))

    if not BUN.is_file():
        pytest.skip(f"installed Bun absent: {BUN}")
    if not (PACKAGE / "package.json").is_file():
        pytest.skip(f"installed OMP package absent: {PACKAGE}")
    src_root = tmp_path / "src"
    src_root.mkdir()
    env = _environment(src_root)
    written = _run("write", src_root, env)
    blobs = src_root / "agent" / "blobs" / "data"
    blobs.mkdir(parents=True, exist_ok=True)
    sid = written["id"]
    source = OmpPersistenceAdapter(
        engine="omp",
        profile=OmpPersistenceProfile(
            format="omp-v1",
            sessions_dir=str(src_root / "sessions"),
            blobs_dir=str(blobs),
            restart_argv=("unused", "{session_path}"),
        ),
    )
    assert source.lookup_for_project(sid, src_root) == sid
    archive = source.export_exact(sid)
    assert archive.records["session.jsonl"] == Path(written["file"]).read_bytes()
    verify_root = tmp_path / "verify"
    verify_root.mkdir()
    env2 = _environment(verify_root)
    env2["OMP_SESSION_DIR"] = str(verify_root / "restored")
    env2["OMP_EXPECT_CWD"] = str(src_root)
    sessions = verify_root / "restored"
    target_blobs = verify_root / "agent" / "blobs" / "data"
    sessions.mkdir(mode=0o700)
    target_blobs.mkdir(parents=True)
    probes = []

    def restart(argv: tuple[str, ...]) -> None:
        probes.append(_run("read", verify_root, env2, argv[-1]))

    target = OmpPersistenceAdapter(
        engine="omp",
        profile=OmpPersistenceProfile(
            format="omp-v1",
            sessions_dir=str(sessions),
            blobs_dir=str(target_blobs),
            restart_argv=("offline-sdk-read", "{session_path}"),
        ),
        restart_runner=restart,
    )
    proof = target.import_exact(archive)
    assert_exact_restore(archive, proof)
    assert len(probes) == 1
    assert probes[0]["id"] == sid
    assert probes[0]["entries"] == written["entries"]
    assert target.lookup_for_project(sid, src_root) == sid
    assert target.export_exact(sid).records == archive.records
    restored = sessions / Path(written["file"]).name
    assert restored.read_bytes() == Path(written["file"]).read_bytes()
    loaded = _run("read", verify_root, env2, str(restored))
    assert loaded["id"] == sid
    assert loaded["entries"] == written["entries"]
    assert restored.read_bytes() == Path(written["file"]).read_bytes()
