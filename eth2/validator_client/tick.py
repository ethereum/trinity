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

    def is_at_genesis(self, genesis_time: int) -> bool:
        return int(self.t) == genesis_time and self.count == 0
