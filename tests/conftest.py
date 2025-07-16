import asyncio
import functools

import pytest


def aio(test):
    @functools.wraps(test)
    def wrapper(*args, **kwargs):
        async def guarded():
            return await asyncio.wait_for(test(*args, **kwargs), timeout=15)

        return asyncio.run(guarded())

    return wrapper


@pytest.fixture
def aio_sleep():
    return asyncio.sleep

