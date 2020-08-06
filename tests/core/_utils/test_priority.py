import asyncio

import pytest

from trinity._utils.priority import SilenceObserver


@pytest.mark.asyncio
async def test_silence_observer_waits_until_silence():
    observer = SilenceObserver(minimum_silence_duration=0.03)

    async def noise_maker():
        async with observer.make_noise():
            await asyncio.sleep(0.02)

    asyncio.ensure_future(noise_maker())
    asyncio.ensure_future(noise_maker())
    asyncio.ensure_future(noise_maker())

    # Allow the noise makers to start
    await asyncio.sleep(0)

    # Noise makers might have stopped, but minimum_silence_duration hasn't ended
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(observer.until_silence(), timeout=0.03)

    # Already waited 0.03, so only need another 0.02, but leave a little extra for CI
    await asyncio.wait_for(
        observer.until_silence(),
        timeout=0.05,
    )
