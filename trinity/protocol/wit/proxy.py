from typing import Tuple

from eth_typing import (
    Hash32,
)

from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import SessionAPI
from trinity._utils.logging import get_logger
from trinity._utils.errors import pass_or_raise
from trinity.protocol.wit.events import (
    GetBlockWitnessHashesRequest,
    SendBlockWitnessHashesEvent,
)
from trinity.protocol.wit.commands import (
    BlockWitnessHashes,
    BlockWitnessHashesPayload,
)


class ProxyWitAPI:
    """
    An ``WitnessAPI`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """

    def __init__(self,
                 session: SessionAPI,
                 event_bus: EndpointAPI,
                 broadcast_config: BroadcastConfig):
        self.logger = get_logger('trinity.protocol.wit.proxy.ProxyWitAPI')
        self.session = session
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config

    async def get_block_witness_hashes(
            self, block_hash: Hash32, timeout: float = None) -> Tuple[Hash32, ...]:
        response = await self._event_bus.request(
            GetBlockWitnessHashesRequest(self.session, block_hash, timeout), self._broadcast_config)
        pass_or_raise(response)
        return tuple(response.node_hashes)

    def send_block_witness_hashes(
            self, request_id: int, block_hash: Hash32, node_hashes: Tuple[Hash32, ...]) -> None:
        command = BlockWitnessHashes(BlockWitnessHashesPayload(request_id, node_hashes))
        self._event_bus.broadcast_nowait(
            SendBlockWitnessHashesEvent(self.session, command, block_hash),
            self._broadcast_config,
        )
