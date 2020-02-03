import asyncio
import random

from hypothesis import (
    given,
    strategies as st,
)
import pytest

from p2p.resource_lock import ResourceLock


async def _yield_to_loop_some():
    for _ in range(random.randint(0, 5)):
        await asyncio.sleep(0)


class Resource:
    is_available = True

    async def consume(self):
        assert self.is_available
        self.is_available = False
        try:
            await _yield_to_loop_some()
        finally:
            self.is_available = True


@given(
    num_consumers=st.integers(min_value=0, max_value=100),
)
@pytest.mark.asyncio
async def test_resource_lock(num_consumers):
    """
    The lock is tested by fuzzing it against multiple consumers that all try to
    acquire the same resource.  Each consumer randomly yields to the event loop
    a few times between each context switch to better guarantee that different
    race conditions are covered.
    """
    resource_lock = ResourceLock()
    resource = Resource()

    async def consume_resource():
        await _yield_to_loop_some()

        async with resource_lock.lock(resource):
            await _yield_to_loop_some()
            await resource.consume()
            await _yield_to_loop_some()

    await asyncio.gather(*(
        consume_resource()
        for i in range(num_consumers)
    ))

    assert resource not in resource_lock._locks
    assert resource not in resource_lock._reference_counts
