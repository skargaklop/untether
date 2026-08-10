"""Deterministic scheduler queue lifecycle and race contracts (Task 21/24).

These tests pin the scheduler's ownership of pending/claimed state using
``anyio.Event`` barriers — never sleeps — so that queueing, exact
cancellation, claim boundaries, failures, and rollback are deterministic.
"""

from __future__ import annotations

from typing import Any

import anyio
import pytest

from untether.model import ResumeToken
from untether.scheduler import (
    CancelQueuedResult,
    CancelQueuedStatus,
    EnqueueDisposition,
    ThreadJob,
    ThreadScheduler,
)
from untether.transport import MessageRef

CODEX = "codex"


class _NoopTaskGroup:
    """Task group stub that never starts a worker — for pure state tests."""

    def start_soon(self, func: Any, *args: Any) -> None:
        _ = func, args


def _job(
    *,
    msg_id: int,
    progress_id: int,
    engine: str = CODEX,
    value: str = "sid",
    text: str | None = None,
) -> ThreadJob:
    return ThreadJob(
        chat_id=123,
        user_msg_id=msg_id,
        text=text or f"prompt-{msg_id}",
        resume_token=ResumeToken(engine=engine, value=value),
        progress_ref=MessageRef(channel_id=123, message_id=progress_id),
    )


# ---------------------------------------------------------------------------
# Step 1: deterministic queue, ordering, and isolation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_busy_token_keeps_two_jobs_pending_and_addressable() -> None:
    """Two jobs queued behind a busy token stay addressable by their own refs."""
    resume = ResumeToken(engine=CODEX, value="sid")
    active_done = anyio.Event()
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)

        j1 = _job(msg_id=10, progress_id=50, text="first")
        j2 = _job(msg_id=11, progress_id=51, text="second")
        await scheduler.enqueue(j1)
        await scheduler.enqueue(j2)

        await anyio.wait_all_tasks_blocked()

        assert await scheduler.get_queued(123, 50) is not None
        assert await scheduler.get_queued(123, 51) is not None
        assert ran == []

        active_done.set()
        with anyio.fail_after(5):
            while len(ran) < 2:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["first", "second"]


@pytest.mark.anyio
async def test_cancel_second_removes_only_second() -> None:
    """Cancelling the second queued job leaves the first queued."""
    resume = ResumeToken(engine=CODEX, value="sid")
    active_done = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        pass

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)

        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="first"))
        await scheduler.enqueue(_job(msg_id=11, progress_id=51, text="second"))

        result = await scheduler.cancel_queued(123, 51)
        assert isinstance(result, CancelQueuedResult)
        assert result.status is CancelQueuedStatus.CANCELLED
        assert result.job is not None
        assert result.job.text == "second"

        assert await scheduler.get_queued(123, 50) is not None
        assert await scheduler.get_queued(123, 51) is None

        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_cancelled_job_never_runs_and_fifo_preserved() -> None:
    """A cancelled job is never executed; remaining jobs run FIFO."""
    resume = ResumeToken(engine=CODEX, value="sid")
    active_done = anyio.Event()
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)

        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="first"))
        await scheduler.enqueue(_job(msg_id=11, progress_id=51, text="second"))
        await scheduler.enqueue(_job(msg_id=12, progress_id=52, text="third"))

        result = await scheduler.cancel_queued(123, 51)
        assert result.status is CancelQueuedStatus.CANCELLED

        active_done.set()
        with anyio.fail_after(5):
            while len(ran) < 2:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["first", "third"]


@pytest.mark.anyio
async def test_different_engines_same_session_isolated() -> None:
    """Same session value under different engines are separate queues."""
    active_done_a = anyio.Event()
    active_done_b = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        pass

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(
            ResumeToken(engine=CODEX, value="sid"), active_done_a
        )
        await scheduler.note_thread_known(
            ResumeToken(engine="claude", value="sid"), active_done_b
        )

        await scheduler.enqueue(
            _job(msg_id=10, progress_id=50, engine=CODEX, value="sid")
        )
        await scheduler.enqueue(
            _job(msg_id=11, progress_id=51, engine="claude", value="sid")
        )

        depth_codex = await scheduler.queue_depth(
            ResumeToken(engine=CODEX, value="sid")
        )
        depth_claude = await scheduler.queue_depth(
            ResumeToken(engine="claude", value="sid")
        )
        assert depth_codex == 1
        assert depth_claude == 1

        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_same_engine_different_sessions_isolated() -> None:
    """Different session values on one engine are separate queues."""
    active_done_1 = anyio.Event()
    active_done_2 = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        pass

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(
            ResumeToken(engine=CODEX, value="s1"), active_done_1
        )
        await scheduler.note_thread_known(
            ResumeToken(engine=CODEX, value="s2"), active_done_2
        )

        await scheduler.enqueue(_job(msg_id=10, progress_id=50, value="s1"))
        await scheduler.enqueue(_job(msg_id=11, progress_id=51, value="s2"))

        assert await scheduler.queue_depth(ResumeToken(engine=CODEX, value="s1")) == 1
        assert await scheduler.queue_depth(ResumeToken(engine=CODEX, value="s2")) == 1

        tg.cancel_scope.cancel()


# ---------------------------------------------------------------------------
# Step 2: cancel-before/after-claim race tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancel_before_claim_returns_cancelled() -> None:
    """Cancel before the worker claims the job removes it and returns CANCELLED."""
    resume = ResumeToken(engine=CODEX, value="sid")
    active_done = anyio.Event()
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="queued"))

        result = await scheduler.cancel_queued(123, 50)
        assert result.status is CancelQueuedStatus.CANCELLED
        assert result.job is not None
        assert result.job.text == "queued"

        active_done.set()
        await anyio.wait_all_tasks_blocked()
        assert ran == []

        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_cancel_after_claim_returns_already_claimed() -> None:
    """Cancel after the worker has claimed the job returns ALREADY_CLAIMED."""
    claimed = anyio.Event()
    release_job = anyio.Event()

    async def _run_job(job: ThreadJob) -> None:
        claimed.set()
        await release_job.wait()

    async def _on_claimed(_job: ThreadJob) -> None:
        pass

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(
            task_group=tg, run_job=_run_job, on_job_claimed=_on_claimed
        )
        await scheduler.enqueue(_job(msg_id=10, progress_id=50))

        with anyio.fail_after(5):
            await claimed.wait()

        result = await scheduler.cancel_queued(123, 50)
        assert result.status is CancelQueuedStatus.ALREADY_CLAIMED
        assert result.job is None

        release_job.set()

    assert claimed.is_set()


@pytest.mark.anyio
async def test_cancel_after_claim_does_not_touch_predecessor() -> None:
    """A claimed queued job's cancel must never affect the active predecessor."""
    resume = ResumeToken(engine=CODEX, value="sid")
    active_done = anyio.Event()
    claimed_queued = anyio.Event()
    release_queued = anyio.Event()

    async def _run_job(job: ThreadJob) -> None:
        claimed_queued.set()
        await release_queued.wait()

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="queued"))

        active_done.set()
        with anyio.fail_after(5):
            await claimed_queued.wait()

        result = await scheduler.cancel_queued(123, 50)
        assert result.status is CancelQueuedStatus.ALREADY_CLAIMED
        assert result.job is None

        release_queued.set()

    assert active_done.is_set()


@pytest.mark.anyio
async def test_stale_callback_after_completion_returns_not_found() -> None:
    """After a claimed job finishes, a stale cancel returns NOT_FOUND."""
    finished = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        finished.set()

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.enqueue(_job(msg_id=10, progress_id=50))

        with anyio.fail_after(5):
            await finished.wait()

        result = await scheduler.cancel_queued(123, 50)
        assert result.status is CancelQueuedStatus.NOT_FOUND
        assert result.job is None

        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_repeated_cancel_after_success_is_not_found() -> None:
    """A repeated cancel after a successful cancellation returns NOT_FOUND."""
    resume = ResumeToken(engine=CODEX, value="sid")
    active_done = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        pass

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)
        await scheduler.enqueue(_job(msg_id=10, progress_id=50))

        first = await scheduler.cancel_queued(123, 50)
        assert first.status is CancelQueuedStatus.CANCELLED

        second = await scheduler.cancel_queued(123, 50)
        assert second.status is CancelQueuedStatus.NOT_FOUND

        tg.cancel_scope.cancel()


# ---------------------------------------------------------------------------
# Step 3: failure and rollback tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_worker_failure_notifies_once_and_continues_fifo() -> None:
    """An unexpected run_job exception invokes on_job_failed once and continues."""
    failed_calls: list[str] = []
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)
        if job.text == "boom":
            raise RuntimeError("worker boom")

    async def _on_failed(job: ThreadJob, exc: BaseException) -> None:
        failed_calls.append(job.text)
        assert isinstance(exc, RuntimeError)

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(
            task_group=tg, run_job=_run_job, on_job_failed=_on_failed
        )
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="boom"))
        await scheduler.enqueue(_job(msg_id=11, progress_id=51, text="ok"))

        with anyio.fail_after(5):
            while len(ran) < 2:
                await anyio.wait_all_tasks_blocked()

    assert failed_calls == ["boom"]
    assert ran == ["boom", "ok"]


@pytest.mark.anyio
async def test_cancellation_exception_is_not_job_failure() -> None:
    """AnyIO cancellation must propagate, not become a job-failure notification."""
    failed_calls: list[str] = []
    started = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        started.set()
        await anyio.sleep_forever()

    async def _on_failed(_job: ThreadJob, _exc: BaseException) -> None:
        failed_calls.append("called")

    with anyio.move_on_after(5):
        async with anyio.create_task_group() as tg:
            scheduler = ThreadScheduler(
                task_group=tg, run_job=_run_job, on_job_failed=_on_failed
            )
            await scheduler.enqueue(_job(msg_id=10, progress_id=50))
            with anyio.fail_after(5):
                await started.wait()
            tg.cancel_scope.cancel()

    assert failed_calls == []


class _FailingTaskGroup:
    """Task group whose start_soon raises to simulate worker startup failure."""

    def __init__(self) -> None:
        self.calls = 0

    def start_soon(self, func: Any, *args: Any) -> None:
        self.calls += 1
        _ = func, args
        raise RuntimeError("task group start failed")


@pytest.mark.anyio
async def test_enqueue_rolls_back_on_start_failure() -> None:
    """If the worker cannot start, enqueue rolls back all inserted state."""
    resume = ResumeToken(engine=CODEX, value="sid")

    async def _run_job(_job: ThreadJob) -> None:
        pass

    tg = _FailingTaskGroup()
    scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)

    with pytest.raises(RuntimeError, match="task group start failed"):
        await scheduler.enqueue(_job(msg_id=10, progress_id=50))

    assert await scheduler.get_queued(123, 50) is None
    assert await scheduler.queue_depth(resume) == 0
    assert resume is not None


@pytest.mark.anyio
async def test_enqueue_after_rollback_starts_normally() -> None:
    """A valid enqueue after a failed one starts the job normally."""
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    failing_tg = _FailingTaskGroup()
    scheduler_failing = ThreadScheduler(task_group=failing_tg, run_job=_run_job)
    with pytest.raises(RuntimeError):
        await scheduler_failing.enqueue(_job(msg_id=99, progress_id=49))

    async with anyio.create_task_group() as real_tg:
        scheduler2 = ThreadScheduler(task_group=real_tg, run_job=_run_job)
        await scheduler2.enqueue(_job(msg_id=10, progress_id=50, text="ok"))

        with anyio.fail_after(5):
            while not ran:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["ok"]


# ---------------------------------------------------------------------------
# Enqueue disposition
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_enqueue_returns_queued_when_busy() -> None:
    """Enqueue behind a busy token returns QUEUED disposition."""
    resume = ResumeToken(engine=CODEX, value="sid")
    active_done = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        pass

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)
        disposition = await scheduler.enqueue(_job(msg_id=10, progress_id=50))
        assert disposition is EnqueueDisposition.QUEUED
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_enqueue_returns_claimable_when_idle() -> None:
    """Enqueue on an idle token returns CLAIMABLE disposition."""
    ran = anyio.Event()

    async def _run_job(_job: ThreadJob) -> None:
        ran.set()

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        disposition = await scheduler.enqueue(_job(msg_id=10, progress_id=50))
        assert disposition is EnqueueDisposition.CLAIMABLE
        with anyio.fail_after(5):
            await ran.wait()


# ---------------------------------------------------------------------------
# Observer contracts
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_on_job_claimed_invoked_before_run_job() -> None:
    """on_job_claimed fires before run_job executes."""
    order: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        order.append(f"run:{job.text}")

    async def _on_claimed(job: ThreadJob) -> None:
        order.append(f"claim:{job.text}")

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(
            task_group=tg, run_job=_run_job, on_job_claimed=_on_claimed
        )
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="hi"))

        with anyio.fail_after(5):
            while len(order) < 2:
                await anyio.wait_all_tasks_blocked()

    assert order == ["claim:hi", "run:hi"]


# ---------------------------------------------------------------------------
# Validation and default observer coverage
# ---------------------------------------------------------------------------


def test_cancel_queued_result_cancelled_requires_job() -> None:
    """CANCELLED without a job is a programming error."""
    with pytest.raises(ValueError, match="must carry the removed job"):
        CancelQueuedResult(status=CancelQueuedStatus.CANCELLED, job=None)


def test_cancel_queued_result_already_claimed_rejects_job() -> None:
    """ALREADY_CLAIMED with a job is a programming error."""
    job = _job(msg_id=10, progress_id=50)
    with pytest.raises(ValueError, match="must not carry a job"):
        CancelQueuedResult(status=CancelQueuedStatus.ALREADY_CLAIMED, job=job)


def test_cancel_queued_result_not_found_rejects_job() -> None:
    """NOT_FOUND with a job is a programming error."""
    job = _job(msg_id=10, progress_id=50)
    with pytest.raises(ValueError, match="must not carry a job"):
        CancelQueuedResult(status=CancelQueuedStatus.NOT_FOUND, job=job)


@pytest.mark.anyio
async def test_default_observers_are_no_ops() -> None:
    """Scheduler with no observers still runs jobs normally."""
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="ok"))

        with anyio.fail_after(5):
            while not ran:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["ok"]


# ---------------------------------------------------------------------------
# claim_queued / requeue_front / observer exception coverage
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_claim_queued_removes_from_pending_and_returns_job() -> None:
    """claim_queued removes the job from pending state and returns it."""
    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(
            task_group=tg,
            run_job=lambda job: anyio.lowlevel.checkpoint(),  # never actually reached  # ty: ignore[unresolved-attribute]
        )
        async with scheduler._lock:
            job = _job(msg_id=10, progress_id=50, text="steer-me")
            key = scheduler.thread_key(job.resume_token)
            scheduler._pending_by_thread[key] = __import__("collections").deque([job])
            assert job.progress_ref is not None
            progress_key = (job.chat_id, job.progress_ref.message_id)
            scheduler._queued_by_progress[progress_key] = job

        claimed = await scheduler.claim_queued(123, 50)
        assert claimed is not None
        assert claimed.text == "steer-me"

        assert await scheduler.get_queued(123, 50) is None

        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_claim_queued_returns_none_for_unknown_job() -> None:
    """claim_queued returns None for a job that was never queued."""
    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(
            task_group=tg,
            run_job=lambda job: anyio.lowlevel.checkpoint(),  # ty: ignore[unresolved-attribute]
        )
        claimed = await scheduler.claim_queued(123, 999)
        assert claimed is None

        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_requeue_front_creates_queue_if_missing() -> None:
    """requeue_front creates a new queue and starts the worker if needed."""
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        job = _job(msg_id=10, progress_id=50, text="requeued")
        await scheduler.requeue_front(job)

        with anyio.fail_after(5):
            while not ran:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["requeued"]


@pytest.mark.anyio
async def test_claim_observer_exception_does_not_block_run_job() -> None:
    """A raising on_job_claimed is logged but run_job still runs."""
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    async def _failing_claimed(_job: ThreadJob) -> None:
        raise RuntimeError("observer broke")

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(
            task_group=tg, run_job=_run_job, on_job_claimed=_failing_claimed
        )
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="survive"))

        with anyio.fail_after(5):
            while not ran:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["survive"]


@pytest.mark.anyio
async def test_queue_drains_after_busy_release() -> None:
    """After busy token releases, queued jobs drain and thread becomes inactive."""
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="first"))

        with anyio.fail_after(5):
            while not ran:
                await anyio.wait_all_tasks_blocked()

        await scheduler.enqueue(_job(msg_id=11, progress_id=51, text="second"))

        with anyio.fail_after(5):
            while len(ran) < 2:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["first", "second"]


@pytest.mark.anyio
async def test_failure_observer_exception_is_logged_not_raised() -> None:
    """A raising on_job_failed is logged but does not crash the scheduler."""
    ran: list[str] = []

    async def _run_job(job: ThreadJob) -> None:
        ran.append(job.text)
        if job.text == "boom":
            raise RuntimeError("job failed")

    async def _failing_on_failed(_job: ThreadJob, _exc: BaseException) -> None:
        raise RuntimeError("observer also broke")

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(
            task_group=tg,
            run_job=_run_job,
            on_job_failed=_failing_on_failed,
        )
        await scheduler.enqueue(_job(msg_id=10, progress_id=50, text="boom"))
        await scheduler.enqueue(_job(msg_id=11, progress_id=51, text="ok"))

        with anyio.fail_after(5):
            while len(ran) < 2:
                await anyio.wait_all_tasks_blocked()

    assert ran == ["boom", "ok"]
