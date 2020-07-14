from dataclasses import dataclass

from eth2.beacon.typing import Epoch, Slot


@dataclass(eq=True, frozen=True)
class Tick:
    t: float
    slot: Slot
    epoch: Epoch
    count: int  # non-negative counter for the tick w/in a slot

    def __repr__(self) -> str:
        return f"Tick({self.epoch},{self.slot},{self.count})"

    @classmethod
    def computing_t_from(
        cls,
        slot: Slot,
        epoch: Epoch,
        count: int,
        genesis_time: int,
        seconds_per_slot: int,
        ticks_per_slot: int,
    ) -> "Tick":
        t = genesis_time + seconds_per_slot * slot + seconds_per_slot / ticks_per_slot
        return cls(t, slot, epoch, count)

    def is_at_genesis(self, genesis_time: int) -> bool:
        return int(self.t) == genesis_time and self.count == 0

    def slot_in_epoch(self, slots_per_epoch: int) -> Slot:
        return Slot(self.slot % slots_per_epoch)

    def is_first_in_slot(self) -> bool:
        return self.count == 0
