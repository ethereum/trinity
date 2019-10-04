from dataclasses import dataclass, field
import logging
import time
from typing import Callable, Tuple

from cached_property import cached_property
import trio

from eth2.beacon.typing import Epoch, Slot
from validator_client.context import Context

logger = logging.getLogger("validator_client.clock")
logger.setLevel(logging.DEBUG)

TICKS_PER_SLOT = 2


@dataclass(eq=True, frozen=True)
class Tick:
    t: float = field(repr=False)
    slot: int
    epoch: int
    count: int  # non-negative counter for the tick w/in a slot

    @cached_property
    def first_in_slot(self) -> bool:
        return self.tick == 0


def _get_unix_time():
    return time.time()


TimeProvider = Callable[[], float]


class Clock:
    def __init__(
        self, context: Context, time_provider: TimeProvider = _get_unix_time
    ) -> None:
        self._context = context
        self._time_provider = time_provider
        self._tick_interval = self._compute_tick_interval()

    def _compute_tick_interval(self) -> int:
        """
        The validator client needs to take action with sub-slot granularity.
        Each period on this finer resolution is called a "tick".
        """
        return self._context.seconds_per_slot // TICKS_PER_SLOT

    def _compute_slot_and_alignment(self, t: int) -> Tuple[int, bool]:
        """
        Compute the slot corresponding to a unix time ``t``.
        Indicate if ``t`` corresponds to the first tick in the relevant slot
        with a boolean return value.
        """
        time_since_genesis = t - self._context.genesis_time
        slot, sub_slot = divmod(time_since_genesis, self._context.seconds_per_slot)
        return slot, sub_slot == 0

    def _compute_epoch(self, slot: Slot) -> Epoch:
        return Epoch(slot // self._context.slots_per_epoch)

    def _compute_tick(self) -> Tick:
        t = self._time_provider()
        slot, aligned = self._compute_slot_and_alignment(int(t))
        epoch = self._compute_epoch(slot)
        count = int(not aligned)
        return Tick(t, slot, epoch, count)

    async def _wait_for_genesis(self) -> None:
        t = self._time_provider()
        genesis_time = self._context.genesis_time
        if t >= genesis_time:
            return

        logger.info(
            "genesis time has not elapsed; blocking client until %s", genesis_time
        )
        duration = genesis_time - t
        await trio.sleep(duration)

    async def _wait_until_next_tick(self, tick: Tick) -> None:
        next_tick_at = tick.t + self._tick_interval
        t = self._time_provider()
        duration = next_tick_at - t
        if duration >= 0:
            await trio.sleep(duration)

    async def stream_ticks(self) -> None:
        await self._wait_for_genesis()

        while True:
            tick = self._compute_tick()
            yield tick
            await self._wait_until_next_tick(tick)
