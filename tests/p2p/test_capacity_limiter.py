import asyncio

import pytest

from eth_utils import ValidationError

from p2p.capacity_limiter import CapacityLimiter


@pytest.mark.asyncio
async def test_capacity_limiter_tracks_token_counts():
    limiter = CapacityLimiter(4)

    assert limiter.total_tokens == 4
    assert limiter.available_tokens == 4
    assert limiter.borrowed_tokens == 0

    await limiter.acquire()

    assert limiter.total_tokens == 4
    assert limiter.available_tokens == 3
    assert limiter.borrowed_tokens == 1

    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    assert limiter.total_tokens == 4
    assert limiter.available_tokens == 0
    assert limiter.borrowed_tokens == 4

    await limiter.release()

    assert limiter.total_tokens == 4
    assert limiter.available_tokens == 1
    assert limiter.borrowed_tokens == 3

    await limiter.release()
    await limiter.release()
    await limiter.release()

    assert limiter.total_tokens == 4
    assert limiter.available_tokens == 4
    assert limiter.borrowed_tokens == 0


@pytest.mark.asyncio
async def test_capacity_limiter_error_to_release_extra():
    limiter = CapacityLimiter(4)

    assert limiter.borrowed_tokens == 0

    with pytest.raises(ValidationError):
        await limiter.release()


@pytest.mark.asyncio
async def test_capacity_limiter_release_allows_borrow():
    limiter = CapacityLimiter(1)

    await limiter.acquire()

    assert limiter.available_tokens == 0

    borrow_a = asyncio.Event()
    borrow_b = asyncio.Event()

    async def _borrow(ev):
        await limiter.acquire()
        ev.set()

    asyncio.ensure_future(_borrow(borrow_a))
    asyncio.ensure_future(_borrow(borrow_b))

    assert limiter.available_tokens == 0

    await limiter.release()
    await asyncio.wait_for(borrow_a.wait(), timeout=1)

    assert limiter.available_tokens == 0

    await limiter.release()
    await asyncio.wait_for(borrow_b.wait(), timeout=1)

    assert limiter.available_tokens == 0


@pytest.mark.asyncio
async def test_capacity_limiter_wait_for_capacity():
    limiter = CapacityLimiter(1)

    await limiter.acquire()

    ready = asyncio.Event()
    done = asyncio.Event()

    async def _wait_capacity():
        ready.set()
        await limiter.wait_for_capacity()
        done.set()

    asyncio.ensure_future(_wait_capacity())

    await ready.wait()
    assert not done.is_set()

    asyncio.ensure_future(limiter.release())

    await asyncio.wait_for(done.wait(), timeout=1)
