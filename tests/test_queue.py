import asyncio
import time

import pytest

from quoit import (
    QueueClosed,
    QueueFull,
    RetryPolicy,
    TaskCancelled,
    TaskFailed,
    TaskQueue,
    TaskState,
)

from conftest import aio


async def double(x):
    return x * 2


def fast_retry(attempts):
    return RetryPolicy.fixed(attempts, 0.005)


class Flaky:
    def __init__(self, failures, error=RuntimeError):
        self.failures = failures
        self.error = error
        self.calls = 0

    async def __call__(self):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error(f"failure {self.calls}")
        return f"ok after {self.calls}"


@aio
async def test_runs_a_coroutine_function():
    async with TaskQueue(workers=2) as q:
        handle = q.submit(double, 21)

        assert await handle.result() == 42
        assert handle.state is TaskState.SUCCEEDED
        assert handle.attempts == 1


@aio
async def test_runs_plain_functions_in_a_thread():
    import threading

    main = threading.get_ident()

    def where():
        return threading.get_ident()

    async with TaskQueue() as q:
        assert await q.submit(where).result() != main


@aio
async def test_passes_args_and_kwargs():
    async def combine(a, b, sep="-"):
        return f"{a}{sep}{b}"

    async with TaskQueue() as q:
        assert await q.submit(combine, "x", "y", kwargs={"sep": "+"}).result() == "x+y"


@aio
async def test_callables_returning_awaitables_are_awaited():
    async with TaskQueue() as q:
        assert await q.submit(lambda: double(4)).result() == 8


@aio
async def test_task_name_defaults_to_the_function_name():
    async with TaskQueue() as q:
        assert q.submit(double, 1).name == "double"
        assert q.submit(double, 1, name="custom").name == "custom"


@aio
async def test_tasks_wait_until_the_queue_starts():
    q = TaskQueue(workers=1)
    handle = q.submit(double, 5)
    await asyncio.sleep(0.02)

    assert handle.state is TaskState.PENDING
    q.start()
    assert await handle.result() == 10
    await q.shutdown()


@aio
async def test_start_twice_is_an_error():
    async with TaskQueue() as q:
        with pytest.raises(RuntimeError):
            q.start()


@aio
async def test_priority_runs_lower_numbers_first():
    order = []
    gate = asyncio.Event()

    async def hold():
        await gate.wait()

    async def record(tag):
        order.append(tag)

    async with TaskQueue(workers=1) as q:
        q.submit(hold)
        await asyncio.sleep(0.01)
        q.submit(record, "low", priority=5)
        q.submit(record, "high", priority=1)
        q.submit(record, "mid", priority=3)
        gate.set()
        await q.join()

    assert order == ["high", "mid", "low"]


@aio
async def test_equal_priorities_run_in_submission_order():
    order = []

    async def record(n):
        order.append(n)

    async with TaskQueue(workers=1) as q:
        for n in range(10):
            q.submit(record, n)
        await q.join()

    assert order == list(range(10))


@aio
async def test_worker_pool_limits_concurrency():
    running = 0
    peak = 0

    async def work():
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.03)
        running -= 1

    async with TaskQueue(workers=3) as q:
        for _ in range(9):
            q.submit(work)
        await q.join()

    assert peak == 3


@aio
async def test_single_worker_runs_one_at_a_time():
    running = 0
    peak = 0

    async def work():
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.005)
        running -= 1

    async with TaskQueue(workers=1) as q:
        for _ in range(5):
            q.submit(work)
        await q.join()

    assert peak == 1


@aio
async def test_delay_is_honoured():
    async def stamp():
        return time.monotonic()

    async with TaskQueue() as q:
        start = time.monotonic()
        ran_at = await q.submit(stamp, delay=0.08).result()

    assert ran_at - start >= 0.075


@aio
async def test_ready_tasks_do_not_wait_behind_delayed_ones():
    order = []

    async def record(tag):
        order.append(tag)

    async with TaskQueue(workers=1) as q:
        q.submit(record, "late", delay=0.06)
        q.submit(record, "now")
        await q.join()

    assert order == ["now", "late"]


@aio
async def test_delayed_tasks_run_in_due_order():
    order = []

    async def record(tag):
        order.append(tag)

    async with TaskQueue(workers=2) as q:
        q.submit(record, "c", delay=0.09)
        q.submit(record, "a", delay=0.03)
        q.submit(record, "b", delay=0.06)
        await q.join()

    assert order == ["a", "b", "c"]


@aio
async def test_a_new_earlier_task_wakes_a_sleeping_worker():
    async def stamp():
        return time.monotonic()

    async with TaskQueue(workers=1) as q:
        far = q.submit(stamp, delay=0.5)
        await asyncio.sleep(0.02)
        start = time.monotonic()
        soon = await q.submit(stamp, delay=0.03).result()
        assert soon - start < 0.3
        assert not far.done()
        await q.shutdown(wait=False)


@aio
async def test_retry_until_success():
    flaky = Flaky(2)

    async with TaskQueue() as q:
        handle = q.submit(flaky, retry=fast_retry(5))

        assert await handle.result() == "ok after 3"

    assert handle.attempts == 3
    assert len(handle.errors) == 2
    assert handle.state is TaskState.SUCCEEDED


@aio
async def test_exhausted_retries_raise_task_failed():
    flaky = Flaky(10)

    async with TaskQueue() as q:
        handle = q.submit(flaky, retry=fast_retry(3), name="flaky")
        with pytest.raises(TaskFailed) as info:
            await handle.result()

    assert info.value.attempts == 3
    assert info.value.name == "flaky"
    assert isinstance(info.value.cause, RuntimeError)
    assert flaky.calls == 3
    assert handle.state is TaskState.FAILED
    assert str(handle.exception()) == "failure 3"


@aio
async def test_default_policy_applies_when_none_is_given():
    flaky = Flaky(1)

    async with TaskQueue(default_policy=fast_retry(2)) as q:
        assert await q.submit(flaky).result() == "ok after 2"


@aio
async def test_no_retry_by_default():
    flaky = Flaky(1)

    async with TaskQueue() as q:
        handle = q.submit(flaky)
        with pytest.raises(TaskFailed):
            await handle.result()

    assert flaky.calls == 1


@aio
async def test_retry_on_filters_exception_types():
    policy = RetryPolicy(max_attempts=4, base_delay=0.001, factor=1.0, retry_on=(ConnectionError,))
    flaky = Flaky(2, error=ValueError)

    async with TaskQueue() as q:
        handle = q.submit(flaky, retry=policy)
        with pytest.raises(TaskFailed):
            await handle.result()

    assert flaky.calls == 1


@aio
async def test_give_up_on_stops_immediately():
    policy = RetryPolicy(max_attempts=4, base_delay=0.001, give_up_on=(KeyError,))
    flaky = Flaky(2, error=KeyError)

    async with TaskQueue() as q:
        with pytest.raises(TaskFailed):
            await q.submit(flaky, retry=policy).result()

    assert flaky.calls == 1


@aio
async def test_backoff_delays_are_applied_between_attempts():
    stamps = []

    async def failing():
        stamps.append(time.monotonic())
        raise RuntimeError("no")

    policy = RetryPolicy.exponential(3, 0.04, factor=2.0)

    async with TaskQueue() as q:
        with pytest.raises(TaskFailed):
            await q.submit(failing, retry=policy).result()

    assert stamps[1] - stamps[0] >= 0.035
    assert stamps[2] - stamps[1] >= 0.075


@aio
async def test_timeout_fails_the_attempt():
    async def slow():
        await asyncio.sleep(5)

    async with TaskQueue() as q:
        handle = q.submit(slow, timeout=0.02)
        with pytest.raises(TaskFailed) as info:
            await handle.result()

    assert isinstance(info.value.cause, asyncio.TimeoutError)


@aio
async def test_timeouts_can_be_retried():
    calls = 0

    async def sometimes_slow():
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(5)
        return "done"

    async with TaskQueue() as q:
        handle = q.submit(sometimes_slow, timeout=0.03, retry=fast_retry(2))

        assert await handle.result() == "done"

    assert handle.attempts == 2


@aio
async def test_cancel_a_pending_task():
    ran = []
    gate = asyncio.Event()

    async def hold():
        await gate.wait()

    async def target():
        ran.append(1)

    async with TaskQueue(workers=1) as q:
        q.submit(hold)
        await asyncio.sleep(0.01)
        handle = q.submit(target)

        assert handle.cancel() is True
        assert handle.cancelled()
        with pytest.raises(TaskCancelled):
            await handle.result()
        gate.set()
        await q.join()

    assert ran == []
    assert q.stats().cancelled == 1


@aio
async def test_cancel_a_delayed_task_frees_the_queue_to_finish():
    async with TaskQueue() as q:
        handle = q.submit(double, 1, delay=30)

        assert handle.cancel()
        await asyncio.wait_for(q.join(), 1)


@aio
async def test_cancel_a_task_waiting_for_a_retry():
    flaky = Flaky(100)
    policy = RetryPolicy.fixed(5, 30)

    async with TaskQueue() as q:
        handle = q.submit(flaky, retry=policy)
        while handle.state is not TaskState.RETRYING:
            await asyncio.sleep(0.005)

        assert handle.cancel()
        with pytest.raises(TaskCancelled):
            await handle.result()


@aio
async def test_a_running_task_cannot_be_cancelled():
    started = asyncio.Event()

    async def work():
        started.set()
        await asyncio.sleep(0.03)
        return "finished"

    async with TaskQueue() as q:
        handle = q.submit(work)
        await started.wait()

        assert handle.cancel() is False
        assert await handle.result() == "finished"


@aio
async def test_finished_tasks_cannot_be_cancelled():
    async with TaskQueue() as q:
        handle = q.submit(double, 1)
        await handle.result()

        assert handle.cancel() is False


@aio
async def test_join_waits_for_everything():
    done = []

    async def work(n):
        await asyncio.sleep(0.01)
        done.append(n)

    async with TaskQueue(workers=2) as q:
        for n in range(6):
            q.submit(work, n)
        await q.join()

        assert sorted(done) == list(range(6))


@aio
async def test_join_on_an_empty_queue_returns_at_once():
    async with TaskQueue() as q:
        await asyncio.wait_for(q.join(), 0.5)


@aio
async def test_join_includes_retries():
    flaky = Flaky(2)

    async with TaskQueue() as q:
        handle = q.submit(flaky, retry=fast_retry(5))
        await q.join()

        assert handle.state is TaskState.SUCCEEDED


@aio
async def test_run_all_returns_results_in_order():
    async def slow_square(x):
        await asyncio.sleep(0.03 - x * 0.005)
        return x * x

    async with TaskQueue(workers=4) as q:
        assert await q.run_all(slow_square, range(5)) == [0, 1, 4, 9, 16]


@aio
async def test_run_all_raises_the_first_failure():
    async def maybe(x):
        if x == 2:
            raise ValueError("two")
        return x

    async with TaskQueue(workers=2) as q:
        with pytest.raises(TaskFailed):
            await q.run_all(maybe, range(4))


@aio
async def test_max_pending_raises_queue_full():
    q = TaskQueue(workers=1, max_pending=2)
    q.submit(double, 1)
    q.submit(double, 2)

    with pytest.raises(QueueFull):
        q.submit(double, 3)
    assert len(q) == 2
    await q.shutdown(wait=False)


@aio
async def test_submit_after_shutdown_is_refused():
    q = TaskQueue()
    q.start()
    await q.shutdown()

    assert q.closed
    with pytest.raises(QueueClosed):
        q.submit(double, 1)


@aio
async def test_graceful_shutdown_runs_delayed_work():
    q = TaskQueue(workers=2)
    q.start()
    handles = [q.submit(double, n, delay=0.03 * n) for n in range(4)]
    await q.shutdown(wait=True)

    assert [h.state for h in handles] == [TaskState.SUCCEEDED] * 4
    assert [await h.result() for h in handles] == [0, 2, 4, 6]


@aio
async def test_graceful_shutdown_lets_retries_finish():
    flaky = Flaky(2)
    q = TaskQueue(workers=1)
    q.start()
    handle = q.submit(flaky, retry=fast_retry(5))
    await q.shutdown(wait=True)

    assert handle.state is TaskState.SUCCEEDED


@aio
async def test_hard_shutdown_cancels_pending_and_running_work():
    started = asyncio.Event()

    async def forever():
        started.set()
        await asyncio.sleep(60)

    q = TaskQueue(workers=1)
    q.start()
    running = q.submit(forever)
    waiting = q.submit(double, 1)
    delayed = q.submit(double, 2, delay=30)
    await started.wait()
    await q.shutdown(wait=False)

    assert {running.state, waiting.state, delayed.state} == {TaskState.CANCELLED}
    assert q.stats().cancelled == 3


@aio
async def test_shutdown_of_an_unstarted_queue_cancels_pending_tasks():
    q = TaskQueue()
    handle = q.submit(double, 1)
    await q.shutdown()

    assert handle.cancelled()


@aio
async def test_context_manager_drains_on_normal_exit():
    async with TaskQueue(workers=1) as q:
        handles = [q.submit(double, n) for n in range(5)]

    assert all(h.state is TaskState.SUCCEEDED for h in handles)


@aio
async def test_context_manager_cancels_after_an_error():
    gate = asyncio.Event()

    async def hold():
        await gate.wait()

    with pytest.raises(ZeroDivisionError):
        async with TaskQueue(workers=1) as q:
            handle = q.submit(hold)
            await asyncio.sleep(0.01)
            1 / 0

    assert handle.cancelled()


@aio
async def test_stats_track_outcomes():
    flaky = Flaky(1)

    async def always_fails():
        raise ValueError

    async with TaskQueue(workers=2) as q:
        q.submit(double, 1)
        q.submit(flaky, retry=fast_retry(3))
        bad = q.submit(always_fails)
        await q.join()
        with pytest.raises(TaskFailed):
            await bad.result()
        stats = q.stats()

    assert stats.submitted == 3
    assert stats.succeeded == 2
    assert stats.failed == 1
    assert stats.retried == 1
    assert stats.pending == 0
    assert stats.running == 0


@aio
async def test_hooks_see_retries_successes_and_failures():
    events = []
    flaky = Flaky(1)

    async def doomed():
        raise KeyError("k")

    q = TaskQueue(
        on_retry=lambda h, e, d: events.append(("retry", h.name, type(e).__name__, d)),
        on_success=lambda h: events.append(("success", h.name)),
        on_failure=lambda h, e: events.append(("failure", h.name, type(e).__name__)),
    )
    async with q:
        q.submit(flaky, retry=fast_retry(2), name="a")
        q.submit(doomed, name="b")
        await q.join()

    assert ("retry", "a", "RuntimeError", 0.005) in events
    assert ("success", "a") in events
    assert ("failure", "b", "KeyError") in events


@aio
async def test_a_raising_hook_does_not_break_the_queue(caplog):
    def bad_hook(handle):
        raise RuntimeError("hook bug")

    async with TaskQueue(on_success=bad_hook) as q:
        handle = q.submit(double, 2)

        assert await handle.result() == 4
    assert "hook raised" in caplog.text


@aio
async def test_exception_accessor_requires_a_finished_task():
    q = TaskQueue()
    handle = q.submit(double, 1)

    with pytest.raises(RuntimeError):
        handle.exception()
    await q.shutdown()
    assert handle.exception() is None


@aio
async def test_jitter_uses_the_supplied_rng():
    import random

    delays = []
    policy = RetryPolicy(max_attempts=3, base_delay=0.01, factor=1.0, jitter=0.5)
    flaky = Flaky(2)

    async with TaskQueue(rng=random.Random(11), on_retry=lambda h, e, d: delays.append(d)) as q:
        await q.submit(flaky, retry=policy).result()

    expected = random.Random(11)
    assert delays == [policy.delay_for(1, expected), policy.delay_for(2, expected)]


@pytest.mark.parametrize(
    "call",
    [
        lambda q: q.submit(double, 1, delay=-1),
        lambda q: q.submit(double, 1, timeout=0),
        lambda q: q.submit("not callable"),
    ],
)
@aio
async def test_bad_submissions_are_rejected(call):
    q = TaskQueue()

    with pytest.raises((ValueError, TypeError)):
        call(q)
    await q.shutdown()


def test_bad_constructor_arguments():
    with pytest.raises(ValueError):
        TaskQueue(workers=0)
    with pytest.raises(ValueError):
        TaskQueue(max_pending=0)


@aio
async def test_shutdown_can_be_called_twice():
    q = TaskQueue(workers=2)
    q.start()
    await q.shutdown(wait=False)
    await q.shutdown(wait=True)
    await q.shutdown(wait=False)

    assert q.closed
