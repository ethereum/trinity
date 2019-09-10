import asyncio

from eth_utils import ValidationError

from p2p.abc import CapacityLimiterAPI


class CapacityLimiter(CapacityLimiterAPI):
    def __init__(self, num_tokens: int) -> None:
        self.total_tokens = num_tokens
        self.borrowed_tokens = 0
        self._condition = asyncio.Condition()

    @property
    def available_tokens(self) -> int:
        return self.total_tokens - self.borrowed_tokens

    async def wait_for_capacity(self) -> int:
        async with self._condition:
            while True:
                if self.available_tokens > 0:
                    return
                else:
                    await self._condition.wait()

    async def acquire(self) -> None:
        async with self._condition:
            while True:
                if self.available_tokens > 0:
                    self.borrowed_tokens += 1
                    return
                else:
                    await self._condition.wait()

    async def release(self) -> None:
        async with self._condition:
            if self.borrowed_tokens <= 0:
                raise ValidationError(
                    "Attempt to release a token when there are no borrowed tokens"
                )
            self.borrowed_tokens -= 1
            self._condition.notify()
