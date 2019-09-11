from typing import (
    NamedTuple,
)

from eth_typing import (
    BlockNumber,
)
from eth2.beacon.typing import (
    Slot,
)


class HeadInfo(NamedTuple):
    slot: Slot

    def update_head(self, new_head_slot) -> 'HeadInfo':
        return HeadInfo(new_head_slot)
