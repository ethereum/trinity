import asyncio
from contextlib import asynccontextmanager
import time
from typing import AsyncIterator


class SilenceObserver:
    """
    This class is helpful for actions that happen in the background, with no urgency.
    Additionally, we might have urgent tasks that are competing for some shared resource.

    SilenceObserver is used like so:
    - initialize with a parameter defining how long to wait since some urgent
          task finished before starting the non-urgent task. This would wait
          for half a second:
          ``observer = SilenceObserver(minimum_silence_duration=0.5)``
    - urgent tasks are called within an ``async with observer.make_noise():`` context
    - non-urgent tasks block on ``await observer.until_silence()`` before acting

    The until_silence() method won't release until no urgent task has made
    noise for 0.5s in the example above.
    """
    def __init__(self, minimum_silence_duration: float) -> None:
        self._minimum_silence_duration = minimum_silence_duration
        self._noise_makers = 0
        self._last_noise_at = 0.0
        # Trigger an event when there are no noise makers left
        self._inactive = asyncio.Event()
        # We start with no noise makers, so trigger immediately
        self._inactive.set()

    @asynccontextmanager
    async def make_noise(self) -> AsyncIterator[None]:
        """Wrap urgent tasks in this context"""
        self._noise_makers += 1
        self._inactive.clear()
        try:
            yield
        finally:
            self._last_noise_at = time.monotonic()
            self._noise_makers -= 1

            # Notify anyone waiting if the noise makers have dropped to 0
            if self._noise_makers == 0:
                self._inactive.set()
            elif self._noise_makers < 0:
                raise ValueError(f"Invalid state: {self._noise_makers} noise makers")
            else:
                # No action to take when there are still noise makers left
                pass

    def _check_silence(self) -> bool:
        if self._noise_makers > 0:
            # There are current noise makers, do not trigger silence
            return False
        else:
            time_since_noise = time.monotonic() - self._last_noise_at
            return time_since_noise > self._minimum_silence_duration

    async def until_silence(self) -> None:
        """
        Non-urgent tasks block on this coro. It waits until all the noise
        makers have gone quiet.

        To avoid churn, wait until there has been no noise for
        minimum_silence_duration.
        """
        while not self._check_silence():
            await self._inactive.wait()
            await asyncio.sleep(self._minimum_silence_duration)
