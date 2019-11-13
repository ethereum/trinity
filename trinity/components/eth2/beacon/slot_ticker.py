import asyncio
import time

from cancel_token import (
    CancelToken,
)
from dataclasses import (
    dataclass,
)
from lahja import (
    BaseEvent,
    BroadcastConfig,
)

from lahja import EndpointAPI

from eth2.beacon.typing import (
    Second,
    Slot,
)
from p2p.service import (
    BaseService,
)
from trinity._utils.shellart import (
    bold_white,
)
from trinity.components.eth2.misc.tick_type import TickType


# Check twice per second: SECONDS_PER_SLOT * 2
DEFAULT_CHECK_FREQUENCY = 24


@dataclass
class SlotTickEvent(BaseEvent):

    slot: Slot
    elapsed_time: Second
    tick_type: TickType


class SlotTicker(BaseService):
    genesis_slot: Slot
    genesis_time: int
    seconds_per_slot: Second
    latest_slot: Slot
    event_bus: EndpointAPI

    def __init__(
            self,
            genesis_slot: Slot,
            genesis_time: int,
            seconds_per_slot: Second,
            event_bus: EndpointAPI,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self.genesis_slot = genesis_slot
        self.genesis_time = genesis_time
        # FIXME: seconds_per_slot is assumed to be constant here.
        # Should it changed in the future fork, fix it as #491 described.
        self.seconds_per_slot = seconds_per_slot
        self.latest_slot = genesis_slot
        self.event_bus = event_bus

    async def _run(self) -> None:
        self.run_daemon_task(self._keep_ticking())
        await self.cancellation()

    async def _keep_ticking(self) -> None:
        """
        Ticker should tick twice in one slot:
        one for a new slot, one for the second half of an already ticked slot,
        e.g., if `seconds_per_slot` is `6`, for slot `49` it should tick once
        for the first 3 seconds and once for the last 3 seconds.
        """
        # Use `sent_tick_types_at_slot` set to record
        # the tick types that haven been sent at current slot.
        sent_tick_types_at_slot = set()
        while self.is_operational:
            elapsed_time = Second(int(time.time()) - self.genesis_time)
            if elapsed_time >= self.seconds_per_slot:
                elapsed_slots = elapsed_time // self.seconds_per_slot
                slot = Slot(elapsed_slots + self.genesis_slot)

                elapsed_time_in_slot = elapsed_time % self.seconds_per_slot
                if elapsed_time_in_slot >= (self.seconds_per_slot * 2 / 3):
                    tick_type = TickType.SLOT_TWO_THIRD
                elif elapsed_time_in_slot >= (self.seconds_per_slot / 3):
                    tick_type = TickType.SLOT_ONE_THIRD
                else:
                    tick_type = TickType.SLOT_START

                # Case 1: new slot
                if slot > self.latest_slot:
                    self.latest_slot = slot
                    await self._broadcast_slot_tick_event(slot, elapsed_time, tick_type)
                    # Clear set
                    sent_tick_types_at_slot = set()
                    sent_tick_types_at_slot.add(TickType.SLOT_START)
                # Case 2: 1/3 of the given slot
                elif (
                    tick_type.is_one_third
                    and TickType.SLOT_ONE_THIRD not in sent_tick_types_at_slot
                ):
                    # TODO: Add aggregator logic
                    pass
                # Case 3: 2/3 of the given slot
                elif (
                    tick_type.is_two_third
                    and TickType.SLOT_TWO_THIRD not in sent_tick_types_at_slot
                ):
                    await self._broadcast_slot_tick_event(slot, elapsed_time, tick_type)
                    sent_tick_types_at_slot.add(TickType.SLOT_TWO_THIRD)

            await asyncio.sleep(self.seconds_per_slot // DEFAULT_CHECK_FREQUENCY)

    async def _broadcast_slot_tick_event(self, slot, elapsed_time, tick_type):
        self.logger.debug(
            bold_white("[%s] tick for slot %s, elapsed %ds)"), tick_type, slot, elapsed_time
        )
        await self.event_bus.broadcast(
            SlotTickEvent(
                slot=slot,
                elapsed_time=elapsed_time,
                tick_type=tick_type,
            ),
            BroadcastConfig(internal=True),
        )
