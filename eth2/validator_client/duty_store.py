from collections import defaultdict
from typing import Collection, Dict, Tuple

import trio

from eth2.beacon.typing import Slot
from eth2.clock import Tick
from eth2.validator_client.duty import Duty

# A particular subdivision of a slot:
# Tick(t, slot, epoch, count) => (slot, count)
TickCount = Tuple[Slot, int]


def to_tick_count(tick: Tick) -> TickCount:
    return (tick.slot, tick.count)


def _contains_duty_at_tick(duties: Tuple[Duty, ...], duty: Duty) -> bool:
    """
    ``duties`` are a collection of duties at one particular tick
    """
    return duty.validator_public_key in map(
        lambda duty: duty.validator_public_key, duties
    )


class DutyStore:
    def __init__(self) -> None:
        self._store: Dict[TickCount, Tuple[Duty, ...]] = defaultdict(tuple)
        self._store_lock = trio.Lock()

    async def duties_at_tick(self, tick: Tick) -> Collection[Duty]:
        target = to_tick_count(tick)
        async with self._store_lock:
            return self._store[target]

    async def add_duties(self, *duties: Duty) -> None:
        async with self._store_lock:
            for duty in duties:
                target = to_tick_count(duty.tick_for_execution)
                existing_duties = self._store[target]
                if _contains_duty_at_tick(existing_duties, duty):
                    continue
                else:
                    existing_duties += (duty,)
                    self._store[target] = existing_duties
