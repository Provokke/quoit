import asyncio
import random

from quoit import RetryPolicy, TaskFailed, TaskQueue


async def fetch(n):
    await asyncio.sleep(0.05)
    if random.random() < 0.4:
        raise ConnectionError(f"item {n} dropped")
    return n * n


async def main():
    random.seed(4)
    policy = RetryPolicy.exponential(attempts=4, base_delay=0.05)
    queue = TaskQueue(
        workers=3,
        default_policy=policy,
        on_retry=lambda handle, error, delay: print(f"retry {handle.name}#{handle.id} in {delay:.2f}s ({error})"),
    )
    async with queue:
        handles = [queue.submit(fetch, n, name="fetch") for n in range(1, 7)]
        for handle in handles:
            try:
                print(f"task {handle.id}: {await handle.result()} after {handle.attempts} attempt(s)")
            except TaskFailed as failure:
                print(f"task {handle.id}: gave up ({failure.cause})")
    print(queue.stats())


asyncio.run(main())
