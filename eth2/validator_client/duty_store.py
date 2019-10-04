from collections import defaultdict
from typing import Collection, Dict, Tuple

import trio

from eth2.beacon.typing import Slot
from eth2.validator_client.duty import Duty
from eth2.validator_client.tick import Tick


class DutyStore:
    def __init__(self) -> None:
        self._store: Dict[Slot, Dict[int, Tuple[Duty, ...]]] = {}
        self._store_lock = trio.Lock()

    async def duties_at_tick(self, tick: Tick) -> Collection[Duty]:
        async with self._store_lock:
            if tick.slot in self._store:
                duties_by_tick = self._store[tick.slot]
                return duties_by_tick.get(tick.count, ())
            else:
                return ()

    async def add_duties(self, *duties: Duty) -> None:
        async with self._store_lock:
            for duty in duties:
                tick = duty.tick_for_execution
                if tick.slot not in self._store:
                    self._store[tick.slot] = defaultdict(tuple)

                self._store[tick.slot][tick.count] += (duty,)
