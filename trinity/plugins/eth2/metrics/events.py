from dataclasses import (
    dataclass,
)
from typing import (
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from eth2.beacon.typing import Slot
from trinity.plugins.eth2.metrics.types import (
    HeadInfo,
)

@dataclass
class HeadSlotResponse(BaseEvent):
    slot: Slot


class HeadSlotRequest(BaseRequestResponseEvent[HeadSlotResponse]):
    @staticmethod
    def expected_response_type() -> Type[HeadSlotResponse]:
        return HeadSlotResponse
