import logging
import time
from typing import AsyncIterable, AsyncIterator, Callable, Tuple

import trio

from eth2.beacon.typing import Epoch, Slot
from eth2.validator_client.config import Config
from eth2.validator_client.tick import Tick

TICKS_PER_SLOT = 2
DEFAULT_EPOCH_LOOKAHEAD = 1


def _get_unix_time() -> float:
    return time.time()


TimeProvider = Callable[[], float]


def _compute_tick_multiplier(interval: float) -> int:
    """
    The tick multiplier is the order of magnitude by which the ``interval`` has to
    be multiplied such that the interval is a number >= 1.

    The tick multiplier simplifies the math to determine the sub slot count
    when the ``Clock``'s tick interval is fractional. The idea is to scale the
    sub slot value such that we can just look at the integer part of this value.
    """
    multiplier = 0
    while interval < 1:
        multiplier += 1
        interval *= 10
    return 10 ** multiplier


class Clock(AsyncIterable[Tick]):
    logger = logging.getLogger("eth2.validator_client.clock")

    def __init__(
        self,
        seconds_per_slot: int,
        genesis_time: int,
        slots_per_epoch: Slot,
        seconds_per_epoch: int,
        time_provider: TimeProvider = _get_unix_time,
        ticks_per_slot: int = TICKS_PER_SLOT,
    ) -> None:
        self._time_provider = time_provider
        self.logger.setLevel(logging.DEBUG)

        self._ticks_per_slot = ticks_per_slot
        # The validator client needs to take action with sub-slot granularity.
        # Each period on this finer resolution is called a "tick".
        self._tick_interval = seconds_per_slot / self._ticks_per_slot
        self._tick_multiplier = _compute_tick_multiplier(self._tick_interval)
        self._seconds_per_slot = seconds_per_slot
        self._genesis_time = genesis_time
        self._slots_per_epoch = slots_per_epoch
        self._seconds_per_epoch = seconds_per_epoch

    @classmethod
    def from_config(
        cls, config: Config, time_provider: TimeProvider = _get_unix_time
    ) -> "Clock":
        return cls(
            config.seconds_per_slot,
            config.genesis_time,
            config.slots_per_epoch,
            config.seconds_per_epoch,
            time_provider=time_provider,
        )

    def _compute_slot_and_alignment(self, t: float) -> Tuple[Slot, bool]:
        """
        Compute the slot corresponding to a time ``t``.
        Indicate if ``t`` corresponds to the first tick in the relevant slot
        with a boolean return value.
        """
        time_since_genesis = t - self._genesis_time
        slot, sub_slot = divmod(time_since_genesis, self._seconds_per_slot)
        return (Slot(int(slot)), int(sub_slot * self._tick_multiplier) == 0)

    def _compute_epoch(self, slot: Slot) -> Epoch:
        return Epoch(slot // self._slots_per_epoch)

    def _compute_current_tick(self) -> Tick:
        t = self._time_provider()
        slot, aligned = self._compute_slot_and_alignment(t)
        epoch = self._compute_epoch(slot)
        count = int(not aligned)
        return Tick(t, slot, epoch, count)

    async def _wait_until(self, target_t: float) -> None:
        """
        Block execution until ``target_t`` (a unix timestamp)
        """
        t = self._time_provider()
        duration = max(target_t - t, 0)
        await trio.sleep(duration)

    async def _wait_for_genesis_with_lookahead(
        self, with_epoch_lookahead: int = DEFAULT_EPOCH_LOOKAHEAD
    ) -> None:
        """
        Sleep the clock until the time is ``with_epoch_lookahead`` epoch(s)
        of slots ahead of the genesis time.

        This is the earliest time the client can poll for duties and expect to receive anything
        meaningful from a honest beacon node.
        """
        genesis_time = self._genesis_time
        start_time = genesis_time - (with_epoch_lookahead * self._seconds_per_epoch)
        self.logger.info(
            "...blocking until unix time %s which is %d epochs before the genesis time %d",
            start_time,
            with_epoch_lookahead,
            genesis_time,
        )
        await self._wait_until(start_time)

    def _compute_time_of_next_tick(self, tick: Tick) -> float:
        """
        The first ``Tick`` in a slot is aligned to the nearest second in unix time.
        We can use the ``tick``'s count to determine when the time of the next
        tick should be, even if there was jitter in producing ``tick``.
        """
        next_tick_count = tick.count + 1
        count_rolls_over = next_tick_count == self._ticks_per_slot
        if count_rolls_over:
            next_slot = tick.slot + 1
        else:
            next_slot = tick.slot
        time_of_next_slot = self._genesis_time + self._seconds_per_slot * next_slot
        return time_of_next_slot + self._tick_interval * (
            next_tick_count % self._ticks_per_slot
        )

    async def _wait_until_next_tick(self, tick: Tick) -> None:
        next_time = self._compute_time_of_next_tick(tick)
        await self._wait_until(next_time)

    async def __aiter__(self) -> AsyncIterator[Tick]:
        await self._wait_for_genesis_with_lookahead()

        while True:
            tick = self._compute_current_tick()
            self.logger.debug("%s", tick)
            if tick.is_at_genesis(self._genesis_time):
                self.logger.warning("Network genesis time is now!")
            yield tick
            await self._wait_until_next_tick(tick)
