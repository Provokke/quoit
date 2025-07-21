<div align="center">

# quoit

An asyncio task queue for Python with a worker pool, delayed tasks, priorities, retries with backoff and per-attempt timeouts. Standard library only.

![License](https://img.shields.io/badge/LICENSE-MIT-blue?style=for-the-badge)  ![Language](https://img.shields.io/badge/PYTHON-3776AB?style=for-the-badge&logo=python&logoColor=white)  ![Tests](https://img.shields.io/badge/TESTS-73-brightgreen?style=for-the-badge)

Refer to [LICENSE](LICENSE) for the legal bits.

</div>

## What's in it

- **`TaskQueue`**: a fixed pool of worker tasks pulling from one scheduler.
  - `submit(func, *args, delay=, priority=, retry=, timeout=, name=, kwargs=)` is a plain method (not a coroutine) and returns a `TaskHandle` straight away.
  - Coroutine functions run on the event loop. Plain functions run in a thread through `asyncio.to_thread`. Callables that return an awaitable are awaited.
  - Lower `priority` numbers run first, and equal priorities run in submission order.
  - `delay` holds a task back. Ready tasks never wait behind delayed ones, and a worker sleeping on a far-off task wakes up when something earlier arrives.
  - `max_pending` makes `submit` raise `QueueFull` instead of growing without limit.
  - `join()`, `run_all(func, items)`, `cancel(handle)`, `stats()`, and `async with` support.
  - `shutdown(wait=True)` drains everything, including delayed tasks and pending retries. `shutdown(wait=False)` cancels pending and running work.
  - Hooks: `on_retry`, `on_success`, `on_failure`. A hook that raises is logged and ignored.
- **`RetryPolicy`**: a frozen dataclass with `max_attempts`, `base_delay`, `factor`, `max_delay`, `jitter`, `retry_on` and `give_up_on`. Helpers: `fixed`, `exponential`, `none`. Jitter takes an injectable `random.Random`, so retries can be made repeatable.
- **`TaskHandle`**: `await handle.result()` gives the value or raises `TaskFailed` (with `.attempts` and `.cause`) or `TaskCancelled`. It also has `state`, `attempts`, `errors`, `exception()`, `done()` and `cancel()`.

```python
import asyncio
from quoit import RetryPolicy, TaskQueue

async def fetch(n):
    ...

async def main():
    policy = RetryPolicy.exponential(attempts=4, base_delay=0.05)
    async with TaskQueue(workers=3, default_policy=policy) as queue:
        urgent = queue.submit(fetch, 1, priority=0, timeout=2.0)
        later = queue.submit(fetch, 2, delay=1.5)
        print(await urgent.result())
        print(await queue.run_all(fetch, range(10)))

asyncio.run(main())
```

A timeout cancels coroutine tasks. A plain function running in a thread can't be interrupted, so the attempt is abandoned and counted as failed but the thread keeps running until it returns.

## 🏗️ Architecture

```
 submit() ─▶ TaskHandle ─┬─ delay > 0 ─▶ delayed heap  (run_at, seq)
                          └─ otherwise ─▶ ready heap    (priority, seq)
                                               ▲
   worker 1 ─┐                                 │ due tasks are promoted
   worker 2 ─┼─▶ _next(): wait on wake-up event or the next due time
   worker N ─┘      │
                    ▼
                _run(handle): invoke (with timeout) ─┬─ ok ──▶ SUCCEEDED
                                                      ├─ error, policy says retry ─▶ RETRYING ─▶ delayed heap
                                                      └─ error, out of attempts ───▶ FAILED
 src/quoit/  queue.py  retry.py  task.py  errors.py
```

- Two heaps keep scheduling simple: one ordered by due time for delayed work, one ordered by priority for work that can run now.
- Workers share a single `asyncio.Event`. They clear it, look at the heaps and only then wait, with a timeout equal to the next due time, so a wake-up can't be missed.
- Cancelled tasks are left in the heaps and skipped when they surface, which keeps `cancel` cheap.
- A retry goes back through the delayed heap, so it doesn't hold a worker while it waits.
- Handles use an `asyncio.Event` internally, which means you can create a queue and submit work before the event loop is running a worker.

## 🚀 Quick start

```
pip install -e ".[test]"
pytest
```

There's a runnable example. It fetches six items with a flaky function and prints each retry:

```
PYTHONPATH=src python examples/demo.py
```

```
task 1: 1 after 2 attempt(s)
task 2: 4 after 2 attempt(s)
retry fetch#4 in 0.10s (item 4 dropped)
retry fetch#4 in 0.20s (item 4 dropped)
task 4: gave up (item 4 dropped)
task 6: 36 after 1 attempt(s)
QueueStats(submitted=6, pending=0, running=0, succeeded=5, failed=1, cancelled=0, retried=7)
```

(Trimmed. The delays double on each retry for task 4: 0.05, 0.10, 0.20 seconds.)

## 🧪 Testing

73 tests, all passing on Python 3.12 with pytest. The async tests use `asyncio.run` through a small decorator with a 15 second guard, so there's no pytest plugin needed. I ran the suite repeatedly to check the timing-based tests aren't flaky.

- Retry policy (21): validation, fixed and exponential delays, caps, jitter bounds and repeatability, `retry_on` and `give_up_on`, immutability.
- Queue (52): sync and async functions, arguments, priority and FIFO order, the concurrency limit, delays and due ordering, waking a sleeping worker, retries and exhaustion, backoff timing, timeouts, cancelling pending, delayed and retrying tasks, `join`, `run_all`, `QueueFull`, graceful and hard shutdown, the context manager, stats, hooks, jitter with a supplied `rng`, and rejected arguments.

