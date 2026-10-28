"""Installed OMP 18.1.6 SDK persistence/cold-load and adapter roundtrip evidence.

OMP v3 is not Pi v3: its title slot, sibling artifacts and global blob refs
require a separate transfer contract. This text/tool-only probe tests no asset
transfer. All processes use disposable state; RPC bash calls no model/provider API.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
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


def _rpc(
    root: Path, env: dict[str, str], record: Path, *, command: str | None = None
) -> tuple[dict, list]:
    """Installed OMP v1 JSONL, not a prompt/model or synthetic append.

    args.ts/flag-tables.ts declare these suppression/exact-session flags.
    rpc-mode.ts emits ready and dispatches bash asynchronously: correlate IDs
    and await bash before requesting messages. EOF disposes/flushes the session.
    """
    argv = [
        str(BUN),
        str(PACKAGE / "dist" / "cli.js"),
        # Explicit catalog selection bypasses credential-based default discovery;
        # no prompt is sent and no provider credentials enter this environment.
        "--model",
        "openai/gpt-4o-mini",
        "--mode",
        "rpc",
        "--session-dir",
        str(record.parent),
        "--session",
        str(record),
        "--no-extensions",
        "--no-skills",
        "--no-rules",
        "--no-title",
        "--no-lsp",
        "--no-pty",
        "--no-prewalk",
        "--no-tools",  # RPC bash is separate; omit model tool schemas from state.
    ]
    frames: queue.Queue[bytes] = queue.Queue()
    errors = bytearray()
    observed = []
    overflow = threading.Event()
    deadline = time.monotonic() + TIMEOUT_S
    with subprocess.Popen(
        argv,
        cwd=root,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as child:
        assert child.stdin and child.stdout and child.stderr

        def drain(stream, *, stdout: bool) -> None:
            total = 0
            line = bytearray()
            while data := stream.read(1):
                total += len(data)
                if total > OUTPUT_LIMIT:
                    overflow.set()
                    child.kill()
                    return
                if stdout:
                    line.extend(data)
                    if data == b"\n":
                        frames.put(bytes(line))
                        line.clear()
                else:
                    errors.extend(data)
            if line:
                frames.put(bytes(line))

        readers = [
            threading.Thread(
                target=drain, args=(stream,), kwargs={"stdout": out}, daemon=True
            )
            for stream, out in ((child.stdout, True), (child.stderr, False))
        ]
        for reader in readers:
            reader.start()

        def receive(expected: str) -> dict:
            while True:
                remaining = deadline - time.monotonic()
                assert remaining > 0, f"OMP RPC timeout waiting for {expected}"
                try:
                    frame = json.loads(frames.get(timeout=remaining))
                except queue.Empty:
                    pytest.fail(
                        f"OMP RPC missing {expected}; rc={child.poll()}; "
                        f"overflow={overflow.is_set()}; frame_types={observed}; "
                        f"stderr={errors.decode('utf-8', errors='replace')}"
                    )
                observed.append(frame.get("type"))
                assert not overflow.is_set(), "OMP RPC output overflow"
                if expected == "ready" and frame.get("type") == "ready":
                    assert frame["protocolVersion"] == 1
                    return frame
                if frame.get("type") == "response" and frame.get("id") == expected:
                    assert frame["success"], frame
                    assert frame["command"] == expected
                    return frame["data"]

        def request(kind: str, **payload) -> dict:
            child.stdin.write(
                (json.dumps({"id": kind, "type": kind, **payload}) + "\n").encode()
            )
            child.stdin.flush()
            return receive(kind)

        try:
            receive("ready")
            state = request("get_state")
            assert Path(state["sessionFile"]) == record
            if command is not None:
                result = request("bash", command=command)
                assert result["exitCode"] == 0 and not result["cancelled"], result
                assert result["output"].strip() == "omp-offline-followup", result
            messages = request("get_messages")["messages"]
            child.stdin.close()
            child.wait(timeout=max(0.01, deadline - time.monotonic()))
            assert child.returncode == 0, errors.decode("utf-8", errors="replace")
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            for reader in readers:
                reader.join(timeout=5)
        assert not overflow.is_set(), "OMP RPC output overflow"
        assert all(not reader.is_alive() for reader in readers)
    return state, messages


def _probe_argv(root: Path) -> tuple[str, ...]:
    """Production probe launcher template: the adapter substitutes ``{probe}``
    with its embedded RPC probe script and appends the restored journal,
    evidence and environment paths itself."""
    return (str(BUN), "{probe}", "rpc", str(PACKAGE), str(root))


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


@pytest.mark.parametrize("injected", [False, True], ids=["unaided", "injected-runner"])
def test_omp_adapter_export_import_sdk_coldload(
    tmp_path: Path, monkeypatch, injected: bool
) -> None:
    """SDK write -> production export/import -> independent SDK cold read.

    Then native RPC bash -> process restart -> evolved export/import return.
    This proves offline native execution, not model/tool continuation. The
    ``unaided`` variant injects no restart runner: the production adapter
    spawns the embedded OMP RPC probe itself and the probe's own evidence
    record (sessionId, native file, ordered message count) is asserted; the
    ``injected-runner`` variant keeps the SDK cold-load runner assertions.
    """
    from untether.session_migration import omp_adapter
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

    # Production-path evidence: the probe writes its own record after a fresh
    # engine answered get_state/get_messages for the restored store. Wrap the
    # adapter's real run_probe seam so the unaided proof is inspectable before
    # the adapter disposes of the probe scratch root.
    probes_run = []
    probe_evidence = []
    real_probe = omp_adapter.run_probe

    def recording_probe(argv, timeout_s):
        real_probe(argv, timeout_s)
        probes_run.append(list(argv))
        evidence = Path(argv[-2])
        if evidence.is_file():
            probe_evidence.append(json.loads(evidence.read_text(encoding="utf-8")))

    if not injected:
        monkeypatch.setattr(omp_adapter, "run_probe", recording_probe)
    target_kwargs = {} if injected else {"restart_probe_argv": _probe_argv(verify_root)}
    target = OmpPersistenceAdapter(
        engine="omp",
        profile=OmpPersistenceProfile(
            format="omp-v1",
            sessions_dir=str(sessions),
            blobs_dir=str(target_blobs),
            restart_argv=("offline-sdk-read", "{session_path}"),
            **target_kwargs,
        ),
        **({} if not injected else {"restart_runner": restart}),
    )
    proof = target.import_exact(archive)
    assert_exact_restore(archive, proof)
    assert proof.restarted is True and proof.resumed is True
    restored = sessions / Path(written["file"]).name
    if injected:
        assert len(probes) == 1
        assert probes[0]["id"] == sid
        assert probes[0]["entries"] == written["entries"]
    else:
        assert not probes
        assert len(probes_run) == 1
        argv = probes_run[0]
        assert argv[0] == str(BUN)
        assert argv[2] == "rpc" and argv[3] == str(PACKAGE)
        assert Path(argv[5]) == restored
        assert argv[6].endswith("probe-evidence.json")
        assert len(probe_evidence) == 1
        evidence = probe_evidence[0]
        assert evidence["sessionId"] == sid
        assert Path(evidence["sessionFile"]) == restored
        assert evidence["messages"] == len(written["entries"])
    assert target.lookup_for_project(sid, src_root) == sid
    assert target.export_exact(sid).records == archive.records
    assert restored.read_bytes() == Path(written["file"]).read_bytes()
    loaded = _run("read", verify_root, env2, str(restored))
    assert loaded["id"] == sid
    assert loaded["entries"] == written["entries"]
    assert restored.read_bytes() == Path(written["file"]).read_bytes()

    historical = [entry["message"] for entry in written["entries"]]
    state, messages = _rpc(src_root, env2, restored)
    assert state["sessionId"] == sid
    assert messages == historical
    command = "echo omp-offline-followup"
    state, evolved_messages = _rpc(src_root, env2, restored, command=command)
    assert state["sessionId"] == sid
    assert evolved_messages[:-1] == historical
    execution = evolved_messages[-1]
    assert execution["role"] == "bashExecution"
    assert execution["command"] == command
    assert execution["output"].strip() == "omp-offline-followup"
    assert execution["exitCode"] == 0
    assert execution["cancelled"] is False
    persisted = [json.loads(line) for line in restored.read_bytes().splitlines()]
    assert [entry for entry in persisted if entry.get("type") == "message"][
        :3
    ] == written["entries"]
    assert [
        entry["message"] for entry in persisted if entry.get("type") == "message"
    ] == evolved_messages
    state, recovered = _rpc(src_root, env2, restored)
    assert state["sessionId"] == sid
    assert recovered == evolved_messages
    evolved = target.export_exact(sid)
    assert evolved.records["session.jsonl"] == restored.read_bytes()
    assert evolved.records["session.jsonl"] != archive.records["session.jsonl"]

    returned_root = tmp_path / "returned"
    returned_root.mkdir()
    returned_env = _environment(returned_root)
    returned_sessions = returned_root / "sessions"
    returned_sessions.mkdir(mode=0o700)
    returned_blobs = returned_root / "agent" / "blobs" / "data"
    returned_blobs.mkdir(parents=True)
    returned_probes = []

    def restart_returned(argv: tuple[str, ...]) -> None:
        returned_probes.append(_rpc(src_root, returned_env, Path(argv[-1])))

    returned_run = []
    returned_evidence = []

    def recording_returned_probe(argv, timeout_s):
        real_probe(argv, timeout_s)
        returned_run.append(list(argv))
        evidence = Path(argv[-2])
        if evidence.is_file():
            returned_evidence.append(json.loads(evidence.read_text(encoding="utf-8")))

    if not injected:
        monkeypatch.setattr(omp_adapter, "run_probe", recording_returned_probe)
    returned_kwargs = (
        {} if injected else {"restart_probe_argv": _probe_argv(returned_root)}
    )
    returned = OmpPersistenceAdapter(
        engine="omp",
        profile=OmpPersistenceProfile(
            format="omp-v1",
            sessions_dir=str(returned_sessions),
            blobs_dir=str(returned_blobs),
            restart_argv=("offline-native-rpc", "{session_path}"),
            **returned_kwargs,
        ),
        **({} if not injected else {"restart_runner": restart_returned}),
    )
    assert_exact_restore(evolved, returned.import_exact(evolved))
    returned_restored = returned_sessions / Path(written["file"]).name
    if injected:
        assert len(returned_probes) == 1
        assert returned_probes[0][0]["sessionId"] == sid
        assert returned_probes[0][1] == evolved_messages
    else:
        assert not returned_probes
        assert len(returned_run) == 1
        assert returned_run[0][3] == str(PACKAGE)
        assert Path(returned_run[0][5]) == returned_restored
        assert len(returned_evidence) == 1
        assert returned_evidence[0]["sessionId"] == sid
        assert Path(returned_evidence[0]["sessionFile"]) == returned_restored
        assert returned_evidence[0]["messages"] == len(evolved_messages)
    assert returned.lookup_for_project(sid, src_root) == sid
    assert returned.export_exact(sid).records == evolved.records
