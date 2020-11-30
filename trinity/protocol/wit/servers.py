from typing import Any

from eth.db.backends.base import BaseAtomicDB

from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from p2p.abc import CommandAPI, SessionAPI

from trinity.exceptions import WitnessHashesUnavailable
from trinity.protocol.common.servers import (
    BaseIsolatedRequestServer,
)
from .commands import GetBlockWitnessHashes
from .db import AsyncWitnessDB
from .events import GetBlockWitnessHashesEvent
from trinity.protocol.eth.peer import ETHProxyPeer


class WitRequestServer(BaseIsolatedRequestServer):

    def __init__(self,
                 event_bus: EndpointAPI,
                 broadcast_config: BroadcastConfig,
                 base_db: BaseAtomicDB) -> None:
        super().__init__(event_bus, broadcast_config, (GetBlockWitnessHashesEvent,))
        self.db = AsyncWitnessDB(base_db)

    async def _handle_msg(self, session: SessionAPI, cmd: CommandAPI[Any]) -> None:
        peer = ETHProxyPeer.from_session(session, self.event_bus, self.broadcast_config)
        if isinstance(cmd, GetBlockWitnessHashes):
            await self.handle_get_block_witnesses(peer, cmd)
        else:
            self.logger.warning("Unexpected command type: %s", cmd)

    async def handle_get_block_witnesses(
            self, peer: ETHProxyPeer, cmd: GetBlockWitnessHashes) -> None:
        self.logger.debug2(
            "Peer %s requested block witnesses for block %s", peer, cmd.payload.block_hash)
        try:
            witness_hashes = await self.db.coro_get_witness_hashes(cmd.payload.block_hash)
        except WitnessHashesUnavailable:
            witness_hashes = tuple()
        self.logger.debug2("Replying to %s with %d witnesses", peer, len(witness_hashes))
        peer.wit_api.send_block_witness_hashes(
            cmd.payload.request_id, cmd.payload.block_hash, witness_hashes)
