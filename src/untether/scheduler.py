from __future__ import annotations

import contextlib
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, Protocol

import anyio

from .context import RunContext
from .logging import get_logger
from .model import EngineId, ResumeToken
from .transport import ChannelId, MessageId, MessageRef, ThreadId

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ThreadJob:
    chat_id: ChannelId
    user_msg_id: MessageId
    text: str
    resume_token: ResumeToken
    context: RunContext | None = None
    thread_id: ThreadId | None = None
    session_key: tuple[int, int | None] | None = None
    progress_ref: MessageRef | None = None
    plan: bool = False
    goal: str | None = None
    kind: Literal["prompt", "compact", "handoff"] = "prompt"
    compact_instructions: str | None = None
    handoff_target: EngineId | None = None


RunJob = Callable[[ThreadJob], Awaitable[None]]
JobClaimed = Callable[[ThreadJob], Awaitable[None]]
JobFailed = Callable[[ThreadJob, BaseException], Awaitable[None]]


class EnqueueDisposition(Enum):
    """Result of an enqueue operation, reflecting scheduler state at enqueue time."""

    QUEUED = "queued"
    CLAIMABLE = "claimable"


class CancelQueuedStatus(Enum):
    """Discriminated result of a queued-job cancellation query."""

    CANCELLED = "cancelled"
    ALREADY_CLAIMED = "already_claimed"
    NOT_FOUND = "not_found"


@dataclass(frozen=True, slots=True)
class CancelQueuedResult:
    """Result of cancelling a queued job by progress message identity.

    Only ``CANCELLED`` carries the removed ``job``; ``ALREADY_CLAIMED`` and
    ``NOT_FOUND`` never do, so callers can pattern-match without guessing.
    """

    status: CancelQueuedStatus
    job: ThreadJob | None = None

    def __post_init__(self) -> None:
        if self.status is CancelQueuedStatus.CANCELLED:
            if self.job is None:
                raise ValueError("CANCELLED result must carry the removed job")
        else:
            if self.job is not None:
                raise ValueError(f"{self.status.name} result must not carry a job")


class TaskGroup(Protocol):
    def start_soon(
        self, func: Callable[..., Awaitable[object]], *args: Any
    ) -> None: ...


async def _noop_claimed(_job: ThreadJob) -> None:
    return None


async def _noop_failed(_job: ThreadJob, _exc: BaseException) -> None:
    return None


class ThreadScheduler:
    def __init__(
        self,
        *,
        task_group: TaskGroup,
        run_job: RunJob,
        on_job_claimed: JobClaimed | None = None,
        on_job_failed: JobFailed | None = None,
    ) -> None:
        self._task_group = task_group
        self._run_job = run_job
        self._on_job_claimed: JobClaimed = on_job_claimed or _noop_claimed
        self._on_job_failed: JobFailed = on_job_failed or _noop_failed
        self._lock = anyio.Lock()
        self._pending_by_thread: dict[str, deque[ThreadJob]] = {}
        self._queued_by_progress: dict[tuple[ChannelId, MessageId], ThreadJob] = {}
        self._claimed_by_progress: dict[tuple[ChannelId, MessageId], ThreadJob] = {}
        self._active_threads: set[str] = set()
        self._busy_until: dict[str, anyio.Event] = {}

    @staticmethod
    def thread_key(token: ResumeToken) -> str:
        return f"{token.engine}:{token.value}"

    async def note_thread_known(self, token: ResumeToken, done: anyio.Event) -> None:
        key = self.thread_key(token)
        async with self._lock:
            current = self._busy_until.get(key)
            if current is None or current.is_set():
                self._busy_until[key] = done
        self._task_group.start_soon(self._clear_busy, key, done)

    async def enqueue(self, job: ThreadJob) -> EnqueueDisposition:
        """Insert ``job`` into the queue for its thread and start a worker if needed.

        Returns whether the job is ``QUEUED`` behind a busy predecessor or
        ``CLAIMABLE`` (immediately runnable). If worker startup fails after
        insertion, rolls back the inserted state before re-raising.
        """
        key = self.thread_key(job.resume_token)
        progress_key = (
            (job.chat_id, job.progress_ref.message_id)
            if job.progress_ref is not None
            else None
        )
        async with self._lock:
            queue = self._pending_by_thread.get(key)
            if queue is None:
                queue = deque()
                self._pending_by_thread[key] = queue
            queue.append(job)
            if progress_key is not None:
                self._queued_by_progress[progress_key] = job
            disposition = self._disposition_locked(key)
            if key in self._active_threads:
                return disposition
            self._active_threads.add(key)

        try:
            self._task_group.start_soon(self._thread_worker, key)
        except BaseException:
            # Roll back the insertion so a later enqueue can start cleanly.
            async with self._lock:
                self._rollback_insertion_locked(key, job, progress_key)
            raise
        return disposition

    async def enqueue_resume(
        self,
        chat_id: ChannelId,
        user_msg_id: MessageId,
        text: str,
        resume_token: ResumeToken,
        context: RunContext | None = None,
        thread_id: ThreadId | None = None,
        session_key: tuple[int, int | None] | None = None,
        progress_ref: MessageRef | None = None,
        plan: bool = False,
        goal: str | None = None,
    ) -> EnqueueDisposition:
        return await self.enqueue(
            ThreadJob(
                chat_id=chat_id,
                user_msg_id=user_msg_id,
                text=text,
                resume_token=resume_token,
                context=context,
                thread_id=thread_id,
                session_key=session_key,
                progress_ref=progress_ref,
                plan=plan,
                goal=goal,
            )
        )

    async def list_queued_for_thread(self, token: ResumeToken) -> list[ThreadJob]:
        key = self.thread_key(token)
        async with self._lock:
            queue = self._pending_by_thread.get(key)
            if not queue:
                return []
            return list(queue)

    async def queue_depth(self, token: ResumeToken) -> int:
        return len(await self.list_queued_for_thread(token))

    def queued_for_chat(self, chat_id: ChannelId) -> list[ThreadJob]:
        """Return queued jobs for a specific chat (sync, for cancel fallback)."""
        return [
            job
            for job in self._queued_by_progress.values()
            if job.chat_id == chat_id and job.progress_ref is not None
        ]

    async def cancel_queued(
        self, chat_id: ChannelId, progress_msg_id: MessageId
    ) -> CancelQueuedResult:
        """Cancel a queued job by its progress-message identity.

        Returns ``CANCELLED`` with the removed job if it was pending,
        ``ALREADY_CLAIMED`` if the worker has claimed it, or ``NOT_FOUND``
        if no such job exists in any schedulable state.
        """
        progress_key = (chat_id, progress_msg_id)
        async with self._lock:
            job = self._queued_by_progress.get(progress_key)
            if job is not None:
                self._remove_pending_locked(job, progress_key)
                return CancelQueuedResult(status=CancelQueuedStatus.CANCELLED, job=job)
            if progress_key in self._claimed_by_progress:
                return CancelQueuedResult(status=CancelQueuedStatus.ALREADY_CLAIMED)
            return CancelQueuedResult(status=CancelQueuedStatus.NOT_FOUND)

    async def claim_queued(
        self, chat_id: ChannelId, progress_msg_id: MessageId
    ) -> ThreadJob | None:
        """Consumer claim for Codex steering — removes the job from pending only.

        Unlike worker claim, this does NOT enter claimed-by-progress tracking;
        steer owns the job until it succeeds or requeues it.
        """
        async with self._lock:
            return self._pop_queued_locked(chat_id, progress_msg_id)

    async def requeue_front(self, job: ThreadJob) -> None:
        """Restore a steer-claimed job to the front of its thread's pending deque."""
        key = self.thread_key(job.resume_token)
        async with self._lock:
            queue = self._pending_by_thread.get(key)
            if queue is None:
                queue = deque()
                self._pending_by_thread[key] = queue
            queue.appendleft(job)
            if job.progress_ref is not None:
                progress_key = (job.chat_id, job.progress_ref.message_id)
                self._queued_by_progress[progress_key] = job
            if key in self._active_threads:
                return
            self._active_threads.add(key)
        self._task_group.start_soon(self._thread_worker, key)

    async def get_queued(
        self, chat_id: ChannelId, progress_msg_id: MessageId
    ) -> ThreadJob | None:
        progress_key = (chat_id, progress_msg_id)
        async with self._lock:
            return self._queued_by_progress.get(progress_key)

    async def is_busy(self, token: ResumeToken) -> bool:
        key = self.thread_key(token)
        async with self._lock:
            done = self._busy_until.get(key)
            return done is not None and not done.is_set()

    # ------------------------------------------------------------------
    # Locked helpers — must be called while holding ``self._lock``
    # ------------------------------------------------------------------

    def _disposition_locked(self, key: str) -> EnqueueDisposition:
        """Determine whether the just-enqueued job is QUEUED or CLAIMABLE."""
        done = self._busy_until.get(key)
        if done is not None and not done.is_set():
            return EnqueueDisposition.QUEUED
        return EnqueueDisposition.CLAIMABLE

    def _remove_pending_locked(
        self, job: ThreadJob, progress_key: tuple[ChannelId, MessageId]
    ) -> None:
        """Remove ``job`` from the pending deque and progress index."""
        thread_key = self.thread_key(job.resume_token)
        queue = self._pending_by_thread.get(thread_key)
        if queue is not None:
            with contextlib.suppress(ValueError):
                queue.remove(job)
            if not queue:
                self._pending_by_thread.pop(thread_key, None)
        self._queued_by_progress.pop(progress_key, None)

    def _rollback_insertion_locked(
        self,
        key: str,
        job: ThreadJob,
        progress_key: tuple[ChannelId, MessageId] | None,
    ) -> None:
        """Roll back an enqueue whose worker failed to start."""
        queue = self._pending_by_thread.get(key)
        if queue is not None:
            with contextlib.suppress(ValueError):
                queue.remove(job)
            if not queue:
                self._pending_by_thread.pop(key, None)
        if (
            progress_key is not None
            and self._queued_by_progress.get(progress_key) is job
        ):
            self._queued_by_progress.pop(progress_key, None)
        self._active_threads.discard(key)

    def _pop_queued_locked(
        self, chat_id: ChannelId, progress_msg_id: MessageId
    ) -> ThreadJob | None:
        """Remove and return a pending job by progress identity (for steer claim)."""
        progress_key = (chat_id, progress_msg_id)
        job = self._queued_by_progress.get(progress_key)
        if job is None:
            return None
        self._remove_pending_locked(job, progress_key)
        return job

    async def _clear_busy(self, key: str, done: anyio.Event) -> None:
        await done.wait()
        async with self._lock:
            if self._busy_until.get(key) is done:
                self._busy_until.pop(key, None)

    async def _thread_worker(self, key: str) -> None:
        try:
            while True:
                async with self._lock:
                    done = self._busy_until.get(key)
                    queue = self._pending_by_thread.get(key)
                    if not queue:
                        self._pending_by_thread.pop(key, None)
                        self._active_threads.discard(key)
                        return

                if done is not None and not done.is_set():
                    await done.wait()
                    continue

                async with self._lock:
                    queue = self._pending_by_thread.get(key)
                    if not queue:
                        continue
                    job = queue.popleft()
                    progress_key: tuple[ChannelId, MessageId] | None = None
                    if job.progress_ref is not None:
                        progress_key = (job.chat_id, job.progress_ref.message_id)
                        self._queued_by_progress.pop(progress_key, None)
                        self._claimed_by_progress[progress_key] = job
                    if not queue:
                        self._pending_by_thread.pop(key, None)

                try:
                    if progress_key is not None:
                        try:
                            await self._on_job_claimed(job)
                        except Exception:
                            logger.exception(
                                "scheduler.claim_observer_failed",
                                key=key,
                                tag=job.resume_token.engine,
                                chat_id=job.chat_id,
                                user_msg_id=job.user_msg_id,
                            )
                    await self._run_job(job)
                except BaseException as exc:
                    if isinstance(exc, anyio.get_cancelled_exc_class()):
                        raise
                    logger.exception(
                        "scheduler.job_failed",
                        key=key,
                        tag=job.resume_token.engine,
                        chat_id=job.chat_id,
                        user_msg_id=job.user_msg_id,
                        error=str(exc),
                        error_type=exc.__class__.__name__,
                    )
                    try:
                        await self._on_job_failed(job, exc)
                    except Exception:
                        logger.exception(
                            "scheduler.failure_observer_failed",
                            key=key,
                            tag=job.resume_token.engine,
                            chat_id=job.chat_id,
                            user_msg_id=job.user_msg_id,
                        )
                finally:
                    if progress_key is not None:
                        async with self._lock:
                            self._claimed_by_progress.pop(progress_key, None)
        finally:
            async with self._lock:
                self._active_threads.discard(key)
