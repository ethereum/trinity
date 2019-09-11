import logging
from typing import Any, Dict

from eth_utils import (
    encode_hex,
)
from lahja import EndpointAPI

from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.typing import (
    Slot,
)

from trinity.constants import TO_BEACON_NETWORKING_BROADCAST_CONFIG
from trinity.db.beacon.chain import BaseAsyncBeaconChainDB
from trinity.rpc.modules import BaseRPCModule
from trinity.plugins.eth2.metrics.events import HeadSlotRequest


class Beacon(BaseRPCModule):
    logger = logging.getLogger("rpc.module.beacon")

    def __init__(self, chain: BaseAsyncBeaconChainDB, event_bus: EndpointAPI) -> None:
        self.chain = chain
        self.event_bus = event_bus


    async def currentSlot(self) -> str:
        return hex(666)

    #
    # Validator APIs
    #
    async def head(self) -> Dict[Any, Any]:
        block = await self.chain.coro_get_canonical_head(BeaconBlock)
        return dict(
            slot=block.slot,
            block_root=encode_hex(block.signing_root),
            state_root=encode_hex(block.state_root),
        )

    #
    # Metrics
    #
    async def headSlot(self) -> str:
        self.logger.debug('Building HeadSlotRequest')
        res = await self.event_bus.request(HeadSlotRequest(), TO_BEACON_NETWORKING_BROADCAST_CONFIG)
        return str(res.slot)
