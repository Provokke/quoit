from __future__ import annotations

import asyncio
import enum
from typing import Any, Callable

from .errors import TaskCancelled, TaskFailed
from .retry import RetryPolicy


class TaskState(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def finished(self) -> bool:
        return self in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED)


class TaskHandle:
    def __init__(
        self,
        task_id: int,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        name: str,
        priority: int,
        policy: RetryPolicy,
        timeout: float | None,
        owner: Any = None,
    ) -> None:
        self.id = task_id
        self.name = name
        self.priority = priority
        self.policy = policy
        self.timeout = timeout
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.state = TaskState.PENDING
        self.attempts = 0
        self.errors: list[BaseException] = []
        self._value: Any = None
        self._owner = owner
        self._finished = asyncio.Event()

    def __repr__(self) -> str:
        return f"<TaskHandle {self.id} {self.name!r} {self.state.value} attempts={self.attempts}>"

    def done(self) -> bool:
        return self.state.finished

    def cancelled(self) -> bool:
        return self.state is TaskState.CANCELLED

    def cancel(self) -> bool:
        if self._owner is None:
            return False
        return self._owner.cancel(self)

    async def wait(self) -> None:
        await self._finished.wait()

    async def result(self) -> Any:
        await self._finished.wait()
        if self.state is TaskState.SUCCEEDED:
            return self._value
        if self.state is TaskState.CANCELLED:
            raise TaskCancelled(f"task {self.name!r} was cancelled")
        raise TaskFailed(self.name, self.attempts, self.errors[-1])

    def exception(self) -> BaseException | None:
        if not self.done():
            raise RuntimeError("task has not finished")
        if self.state is TaskState.FAILED:
            return self.errors[-1]
        return None

    def _succeed(self, value: Any) -> None:
        self._value = value
        self.state = TaskState.SUCCEEDED
        self._finished.set()

    def _fail(self) -> None:
        self.state = TaskState.FAILED
        self._finished.set()

    def _cancel(self) -> None:
        self.state = TaskState.CANCELLED
        self._finished.set()

