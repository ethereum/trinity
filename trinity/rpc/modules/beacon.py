from typing import Any, Dict

from eth_utils import (
    decode_hex,
    encode_hex,
)
from ssz.tools import (
    to_formatted_dict,
)

from eth2.beacon.types.blocks import SignedBeaconBlock
from eth2.beacon.typing import Root, Slot
from trinity.rpc.format import (
    format_params,
    to_int_if_hex,
)
from trinity.rpc.modules import BeaconChainRPCModule


class Beacon(BeaconChainRPCModule):

    async def currentSlot(self) -> str:
        return hex(666)

    async def head(self) -> Dict[Any, Any]:
        block = await self.chain.coro_get_canonical_head(SignedBeaconBlock)
        return dict(
            slot=block.slot,
            block_root=encode_hex(block.message.hash_tree_root),
            state_root=encode_hex(block.message.state_root),
        )

    #
    # Debug
    #
    async def getFinalizedHead(self) -> Dict[Any, Any]:
        """
        Return finalized head block.
        """
        block = await self.chain.coro_get_finalized_head(SignedBeaconBlock)
        return to_formatted_dict(block, sedes=SignedBeaconBlock)

    @format_params(to_int_if_hex)
    async def getCanonicalBlockBySlot(self, slot: Slot) -> Dict[Any, Any]:
        """
        Return the canonical block of the given slot.
        """
        block = await self.chain.coro_get_canonical_block_by_slot(slot, SignedBeaconBlock)
        return to_formatted_dict(block, sedes=SignedBeaconBlock)

    @format_params(decode_hex)
    async def getBlockByRoot(self, root: Root) -> Dict[Any, Any]:
        """
        Return the block of given root.
        """
        block = await self.chain.coro_get_block_by_root(root, SignedBeaconBlock)
        return to_formatted_dict(block, sedes=SignedBeaconBlock)

    async def getGenesisBlockRoot(self) -> str:
        """
        Return genesis ``Root`` in hex string.
        """
        block_root = await self.chain.coro_get_genesis_block_root()
        return encode_hex(block_root)
