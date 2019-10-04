from collections import defaultdict
from typing import Collection

import trio

from validator_client.clock import Tick
from validator_client.context import Context
from validator_client.duty import Duty


class DutyStore:
    """
    A ``DutyStore`` maintains a record of the current duties discovered by
    the ``DutyScheduler`` which are intended to be acted upon by the ``DutyExecutor``.
    """

    def __init__(self, context: Context) -> None:
        self._context = context
        self._store = defaultdict(tuple)
        self._store_lock = trio.Lock()

    async def duties_at_tick(self, tick: Tick) -> Collection[Duty]:
        async with self._store_lock:
            return self._store[tick]

    async def add(self, duty: Duty, tick: Tick) -> None:
        async with self._store_lock:
            self._store[tick] += (duty,)

    async def add_duties_at_tick(self, duties: Collection[Duty], tick: Tick) -> None:
        async with self._store_lock:
            for duty in duties:
                self._store[tick] += (duty,)
