"""Receiver tests use isolated on-disk endpoints, not real SSH hosts."""
from __future__ import annotations

from uuid import uuid4

import pytest

from untether.session_migration.package import pack_context
from untether.session_migration.intent import OperationConflict
from untether.session_migration.receiver import ReceiverRefusal, StagingReceiver


@pytest.mark.parametrize("direction", ["to", "from"])
@pytest.mark.parametrize("deployment", ["native", "docker", "podman", "coolify"])
def test_receiver_import_adapter_gate(tmp_path, direction, deployment):
    receiver = StagingReceiver(
        tmp_path / f"{direction}-{deployment}",
        max_archive=4096, max_expanded=8192, min_free_bytes=1,
    )
    archive = pack_context(
        engine="fake-acp", project="demo", content=b'{"messages":[]}',
        max_bytes=4096,
    )
    operation_id = str(uuid4())
    calls = []

    def fake_independent_resume_probe(package):
        calls.append(package)
        assert package.engine == "fake-acp"
        return "fresh-native-session"

    receipt = receiver.receive(
        operation_id, archive, expected_project="demo",
        import_and_probe=fake_independent_resume_probe,
    )
    assert receipt.status == "verified"
    assert receiver.receive(
        operation_id, archive, expected_project="demo",
    ) == receipt
    assert len(calls) == 1
    assert not list((receiver.root / "staging").iterdir())


def test_no_adapter_never_publishes_receipt(tmp_path):
    root = tmp_path / "dest"
    receiver = StagingReceiver(
        root, max_archive=4096, max_expanded=8192, min_free_bytes=1,
    )
    archive = pack_context(
        engine="acp", project="demo", content=b"abc", max_bytes=4096,
    )
    with pytest.raises(ReceiverRefusal, match="ADAPTER_UNAVAILABLE"):
        receiver.receive(str(uuid4()), archive, expected_project="demo")
    assert not root.exists()


def test_failure_of_resume_probe_has_no_success_receipt(tmp_path):
    receiver = StagingReceiver(
        tmp_path / "dest", max_archive=4096,
        max_expanded=8192, min_free_bytes=1,
    )
    archive = pack_context(
        engine="acp", project="demo", content=b"abc", max_bytes=4096,
    )
    with pytest.raises(ReceiverRefusal, match="RESUME_PROBE_FAILED"):
        receiver.receive(
            str(uuid4()), archive, expected_project="demo",
            import_and_probe=lambda _package: "",
        )
    assert not list((receiver.root / "receipts").glob("*.json")) if (
        receiver.root / "receipts"
    ).exists() else True


def test_lost_receipt_retry_never_imports_twice(tmp_path):
    receiver = StagingReceiver(
        tmp_path / 'dest', max_archive=4096, max_expanded=8192, min_free_bytes=1,
    )
    archive = pack_context(engine='acp', project='demo', content=b'abc', max_bytes=4096)
    operation_id = str(uuid4())
    calls = []

    def crash_after_import(_package):
        calls.append('imported')
        raise RuntimeError('simulated power loss before receipt')

    with pytest.raises(RuntimeError, match='simulated power loss'):
        receiver.receive(
            operation_id, archive, expected_project='demo',
            import_and_probe=crash_after_import,
        )
    with pytest.raises(OperationConflict, match='NEEDS_RECONCILE'):
        receiver.receive(
            operation_id, archive, expected_project='demo',
            import_and_probe=crash_after_import,
        )
    assert calls == ['imported']
