from __future__ import annotations

import asyncio
import heapq
import inspect
import itertools
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable

from .errors import QueueClosed, QueueFull
from .retry import RetryPolicy
from .task import TaskHandle, TaskState

logger = logging.getLogger("quoit")


@dataclass(frozen=True)
class QueueStats:
    submitted: int
    pending: int
    running: int
    succeeded: int
    failed: int
    cancelled: int
    retried: int


class TaskQueue:
    def __init__(
        self,
        workers: int = 4,
        *,
        default_policy: RetryPolicy | None = None,
        max_pending: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
        on_retry: Callable[[TaskHandle, BaseException, float], Any] | None = None,
        on_success: Callable[[TaskHandle], Any] | None = None,
        on_failure: Callable[[TaskHandle, BaseException], Any] | None = None,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        if max_pending is not None and max_pending < 1:
            raise ValueError("max_pending must be at least 1")
        self.workers = workers
        self._default_policy = default_policy or RetryPolicy.none()
        self._max_pending = max_pending
        self._clock = clock
        self._rng = rng
        self._on_retry = on_retry
        self._on_success = on_success
        self._on_failure = on_failure
        self._delayed: list[tuple[float, int, TaskHandle]] = []
        self._ready: list[tuple[int, int, TaskHandle]] = []
        self._sequence = itertools.count()
        self._ids = itertools.count(1)
        self._tasks: list[asyncio.Task[None]] = []
        self._wakeup = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._started = False
        self._closing = False
        self._unfinished = 0
        self._pending = 0
        self._running = 0
        self._counts = {"submitted": 0, "succeeded": 0, "failed": 0, "cancelled": 0, "retried": 0}

    async def __aenter__(self) -> TaskQueue:
        self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.shutdown(wait=exc_type is None)

    def __len__(self) -> int:
        return self._pending

    @property
    def closed(self) -> bool:
        return self._closing

    def start(self) -> None:
        if self._started:
            raise RuntimeError("queue already started")
        if self._closing:
            raise QueueClosed("queue is shut down")
        self._started = True
        for index in range(self.workers):
            self._tasks.append(asyncio.create_task(self._worker(), name=f"quoit-worker-{index}"))

    def submit(
        self,
        func: Callable[..., Any],
        *args: Any,
        delay: float = 0.0,
        priority: int = 0,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
        name: str | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> TaskHandle:
        if self._closing:
            raise QueueClosed("queue is shut down")
        if not callable(func):
            raise TypeError("func must be callable")
        if delay < 0:
            raise ValueError("delay cannot be negative")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        if self._max_pending is not None and self._pending >= self._max_pending:
            raise QueueFull(f"{self._pending} tasks already pending")

        task_id = next(self._ids)
        handle = TaskHandle(
            task_id,
            func,
            args,
            dict(kwargs or {}),
            name or getattr(func, "__name__", f"task-{task_id}"),
            priority,
            retry or self._default_policy,
            timeout,
            owner=self,
        )
        self._counts["submitted"] += 1
        self._unfinished += 1
        self._idle.clear()
        self._enqueue(handle, delay)
        return handle

    async def run_all(self, func: Callable[..., Any], items: Iterable[Any], **options: Any) -> list[Any]:
        handles = [self.submit(func, item, **options) for item in items]
        return list(await asyncio.gather(*(handle.result() for handle in handles)))

    def cancel(self, handle: TaskHandle) -> bool:
        if handle.state not in (TaskState.PENDING, TaskState.RETRYING):
            return False
        handle._cancel()
        self._pending -= 1
        self._counts["cancelled"] += 1
        self._finish_one()
        self._wakeup.set()
        return True

    async def join(self) -> None:
        await self._idle.wait()

    async def shutdown(self, wait: bool = True) -> None:
        self._closing = True
        if wait and self._started:
            self._wakeup.set()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            return
        for handle in self._drain_pending():
            handle._cancel()
            self._counts["cancelled"] += 1
            self._finish_one()
        self._wakeup.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def stats(self) -> QueueStats:
        return QueueStats(
            submitted=self._counts["submitted"],
            pending=self._pending,
            running=self._running,
            succeeded=self._counts["succeeded"],
            failed=self._counts["failed"],
            cancelled=self._counts["cancelled"],
            retried=self._counts["retried"],
        )

    def _drain_pending(self) -> list[TaskHandle]:
        handles = [entry[2] for entry in self._delayed + self._ready if not entry[2].done()]
        self._delayed.clear()
        self._ready.clear()
        self._pending = 0
        return handles

    def _enqueue(self, handle: TaskHandle, delay: float) -> None:
        self._pending += 1
        if delay > 0:
            heapq.heappush(self._delayed, (self._clock() + delay, next(self._sequence), handle))
        else:
            heapq.heappush(self._ready, (handle.priority, next(self._sequence), handle))
        self._wakeup.set()

    def _promote_due(self) -> float | None:
        now = self._clock()
        while self._delayed:
            run_at, _, handle = self._delayed[0]
            if handle.done():
                heapq.heappop(self._delayed)
            elif run_at <= now:
                heapq.heappop(self._delayed)
                heapq.heappush(self._ready, (handle.priority, next(self._sequence), handle))
            else:
                return run_at - now
        return None

    async def _next(self) -> TaskHandle | None:
        while True:
            self._wakeup.clear()
            wait = self._promote_due()
            while self._ready:
                handle = heapq.heappop(self._ready)[2]
                if not handle.done():
                    self._pending -= 1
                    return handle
            if wait is None and not self._delayed and self._closing:
                return None
            try:
                await asyncio.wait_for(self._wakeup.wait(), wait)
            except asyncio.TimeoutError:
                pass

    async def _worker(self) -> None:
        while True:
            handle = await self._next()
            if handle is None:
                return
            await self._run(handle)

    async def _invoke(self, handle: TaskHandle) -> Any:
        if inspect.iscoroutinefunction(handle.func):
            awaitable: Awaitable[Any] = handle.func(*handle.args, **handle.kwargs)
        else:
            awaitable = asyncio.to_thread(handle.func, *handle.args, **handle.kwargs)
        value = await asyncio.wait_for(awaitable, handle.timeout)
        if inspect.isawaitable(value):
            value = await asyncio.wait_for(value, handle.timeout)
        return value

    async def _run(self, handle: TaskHandle) -> None:
        handle.state = TaskState.RUNNING
        handle.attempts += 1
        self._running += 1
        try:
            value = await self._invoke(handle)
        except asyncio.CancelledError:
            handle._cancel()
            self._counts["cancelled"] += 1
            self._finish_one()
            raise
        except Exception as error:
            handle.errors.append(error)
            self._after_failure(handle, error)
        else:
            handle._succeed(value)
            self._counts["succeeded"] += 1
            self._finish_one()
            self._hook(self._on_success, handle)
        finally:
            self._running -= 1

    def _after_failure(self, handle: TaskHandle, error: BaseException) -> None:
        if handle.policy.should_retry(error, handle.attempts):
            delay = handle.policy.delay_for(handle.attempts, self._rng)
            handle.state = TaskState.RETRYING
            self._counts["retried"] += 1
            self._hook(self._on_retry, handle, error, delay)
            self._enqueue(handle, delay)
            return
        handle._fail()
        self._counts["failed"] += 1
        self._finish_one()
        self._hook(self._on_failure, handle, error)

    def _finish_one(self) -> None:
        self._unfinished -= 1
        if self._unfinished == 0:
            self._idle.set()

    def _hook(self, hook: Callable[..., Any] | None, *args: Any) -> None:
        if hook is None:
            return
        try:
            hook(*args)
        except Exception:
            logger.exception("quoit hook raised")

