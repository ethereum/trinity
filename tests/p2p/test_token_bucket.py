import asyncio
import pytest
import time

from p2p.token_bucket import (
    TokenBucket,
    NotEnoughTokens,
)


async def measure_zero(iterations):
    bucket = TokenBucket(1, iterations)
    start_at = time.perf_counter()
    for _ in range(iterations):
        await bucket.take()
    end_at = time.perf_counter()
    return end_at - start_at


async def slowest_of_zero_measures(iterations, attempts=5):
    timings = [await measure_zero(iterations) for _ in range(attempts)]
    return max(timings)


async def assert_close_to_zero(actual, empty_iterations):
    EPSILON = 0.0005

    if actual < EPSILON:
        # Since this should take roughly no time, as long as it's short enough,
        # it doesn't matter if the value is "close" to the zero measure. This
        # helps avoid false failures and CI turning red for no reason.
        pass
    else:
        # since the capacity should have been fully refilled, second loop time
        # should take near zero time
        expected = await slowest_of_zero_measures(empty_iterations)

        # drift is allowed to be up to 1000% since we're working with very small
        # numbers, and the performance in CI varies widely.
        assert actual < expected * 10


def assert_fuzzy_equal(actual, expected, allowed_drift):
    assert abs(1 - (actual / expected)) < allowed_drift


@pytest.mark.asyncio
async def test_token_bucket_initial_tokens():
    CAPACITY = 10
    bucket = TokenBucket(1000, CAPACITY)

    start_at = time.perf_counter()
    for _ in range(CAPACITY):
        await bucket.take()

    end_at = time.perf_counter()
    delta = end_at - start_at

    await assert_close_to_zero(delta, CAPACITY)


@pytest.mark.asyncio
async def test_token_bucket_hits_limit():
    CAPACITY = 50
    TOKENS_PER_SECOND = 1000
    bucket = TokenBucket(TOKENS_PER_SECOND, CAPACITY)

    bucket.take_nowait(CAPACITY)
    start_at = time.perf_counter()
    # first CAPACITY tokens should be roughly instant
    # next CAPACITY tokens should each take 1/TOKENS_PER_SECOND second each to generate.
    while True:
        if bucket.can_take(CAPACITY):
            break
        else:
            await asyncio.sleep(0)

    end_at = time.perf_counter()

    # we use a zero-measure of CAPACITY loops to account for the loop overhead.
    zero = await measure_zero(CAPACITY)
    expected_delta = CAPACITY / TOKENS_PER_SECOND + zero
    delta = end_at - start_at

    # allow up to 10% difference in expected time
    assert_fuzzy_equal(delta, expected_delta, allowed_drift=0.1)


@pytest.mark.asyncio
async def test_token_bucket_refills_itself():
    CAPACITY = 50
    TOKENS_PER_SECOND = 1000
    bucket = TokenBucket(TOKENS_PER_SECOND, CAPACITY)

    # consume all of the tokens
    for _ in range(CAPACITY):
        await bucket.take()

    # enough time for the bucket to fully refill
    start_at = time.perf_counter()
    time_to_refill = CAPACITY / TOKENS_PER_SECOND
    while time.perf_counter() - start_at < time_to_refill:
        await asyncio.sleep(time_to_refill)

    # This should take roughly zero time
    start_at = time.perf_counter()

    for _ in range(CAPACITY):
        await bucket.take()

    end_at = time.perf_counter()

    delta = end_at - start_at

    await assert_close_to_zero(delta, CAPACITY)


@pytest.mark.asyncio
async def test_token_bucket_can_take():
    bucket = TokenBucket(1, 10)

    assert bucket.can_take() is True  # can take 1
    assert bucket.can_take(bucket.get_num_tokens()) is True  # can take full capacity

    await bucket.take(10)  # empty the bucket

    assert bucket.can_take() is False


@pytest.mark.asyncio
async def test_token_bucket_get_num_tokens():
    bucket = TokenBucket(1, 10)

    # starts at full capacity
    assert bucket.get_num_tokens() == 10

    await bucket.take(5)
    assert 5 <= bucket.get_num_tokens() <= 5.1

    await bucket.take(bucket.get_num_tokens())

    assert 0 <= bucket.get_num_tokens() <= 0.1


def test_token_bucket_take_nowait():
    bucket = TokenBucket(1, 10)

    assert bucket.can_take(10)
    bucket.take_nowait(10)
    assert not bucket.can_take(1)

    with pytest.raises(NotEnoughTokens):
        bucket.take_nowait(1)
