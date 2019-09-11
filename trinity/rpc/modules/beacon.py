import logging
from typing import Any, Dict

from eth_utils import (
    encode_hex,
)
from lahja import EndpointAPI

from eth2.beacon.types.blocks import BeaconBlock

from trinity.constants import TO_BEACON_NETWORKING_BROADCAST_CONFIG
from trinity.db.beacon.chain import BaseAsyncBeaconChainDB
from trinity.rpc.modules import BaseRPCModule
from trinity.plugins.eth2.metrics.events import (
    HeadSlotRequest,
    HeadRootRequest,
    PreviousJustifiedEpochRequest,
    PreviousJustifizedRootRequest,
    CurrentJustifiedEpochRequest,
    CurrentJustifiedRootRequest,
    FinalizedEpochRequest,
    FinalizedRootRequest,
)


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
        res = await self.event_bus.request(HeadSlotRequest(),
        TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return str(res.slot)

    async def headRoot(self) -> str:
        res = await self.event_bus.request(
            HeadRootRequest(),
            TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return encode_hex(res.root)

    async def previousJustifiedEpoch(self) -> str:
        res = await self.event_bus.request(
            PreviousJustifiedEpochRequest(),
            TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return str(res.epoch)

    async def previousJustifiedRoot(self) -> str:
        res = await self.event_bus.request(
            PreviousJustifizedRootRequest(),
            TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return encode_hex(res.root)

    async def currentJustifiedEpoch(self) -> str:
        res = await self.event_bus.request(
            CurrentJustifiedEpochRequest(),
            TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return str(res.epoch)

    async def currentJustifiedRoot(self) -> str:
        res = await self.event_bus.request(
            CurrentJustifiedRootRequest(),
            TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return encode_hex(res.root)

    async def finalizedEpoch(self) -> str:
        res = await self.event_bus.request(
            FinalizedEpochRequest(),
            TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return str(res.epoch)

    async def finalizedRoot(self) -> str:
        res = await self.event_bus.request(
            FinalizedRootRequest(),
            TO_BEACON_NETWORKING_BROADCAST_CONFIG,
        )
        return encode_hex(res.root)
