import asyncio
from concurrent.futures import CancelledError
from typing import (
    Any,
    FrozenSet,
    Set,
    Tuple,
    Type,
    cast,
)

from async_service import Service

from lahja import EndpointAPI

from eth.abc import (
    BlockAPI,
)
from eth_typing import Hash32

from p2p.abc import CommandAPI
from p2p.disconnect import DisconnectReason
from p2p.exceptions import PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from trinity._utils.logging import get_logger
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.fh.proto import FirehoseProtocol
from trinity.sync.common.events import StatelessBlockImportDone


class WitnessBroadcaster(Service, PeerSubscriber):
    """
    Broadcast block witness node hashes to all Firehose-compatible peers.
    """

    # We are only interested in peers entering or leaving the pool
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    _firehose_peers: Set[ETHPeer]

    def __init__(self, peer_pool: ETHPeerPool, event_bus: EndpointAPI) -> None:
        self.logger = get_logger('trinity.sync.beam.WitnessBroadcaster')
        self._peer_pool = peer_pool
        self._event_bus = event_bus

        self._firehose_peers = set()

    def register_peer(self, peer: BasePeer) -> None:
        super().register_peer(peer)
        # This service is only interested in peers that implement firehose
        if peer.connection.has_protocol(FirehoseProtocol):
            fh_peer = cast(ETHPeer, peer)
            self.logger.debug("Added *confirmed* Firehose for broadcast: %s", fh_peer)
            if fh_peer in self._firehose_peers:
                self.logger.warning("%s was already in the set of Firehose peers", fh_peer)
            else:
                self._firehose_peers.add(fh_peer)

    def deregister_peer(self, peer: BasePeer) -> None:
        super().deregister_peer(peer)
        if peer in self._firehose_peers:
            self.logger.warning("Removing Firehose peer: %s", peer)
            self._firehose_peers.remove(cast(ETHPeer, peer))

    async def _broadcast_new_witnesses(self) -> None:
        async for event in self._event_bus.stream(StatelessBlockImportDone):
            if not event.witness_hashes:
                self.logger.warning("Witness metadata for %s is empty", event.block)
            else:
                await self._broadcast_witness_metadata(event.block, event.witness_hashes)

    async def _broadcast_witness_metadata(
            self, block: BlockAPI, witness_hashes: Tuple[Hash32, ...]) -> None:

        # make a copy, so the set doesn't modify during loop
        eligible_peers = tuple(self._firehose_peers)

        if len(eligible_peers) == 0:
            self.logger.debug(
                "Skipping witness broadcast for %s, because not connected to any peers",
                block,
            )

        for peer in eligible_peers:
            self.logger.info("Sending %d hashes of witness to: %s", len(witness_hashes), peer)
            try:
                peer.fh_api.send_new_block_witness_hashes(block.hash, witness_hashes)
            except asyncio.TimeoutError:
                self.logger.debug("Timed out broadcasting witness to %s", peer)
                continue
            except CancelledError:
                self.logger.debug("Peer %s witness broadcast cancelled", peer)
                raise
            except PeerConnectionLost:
                self.logger.warning(
                    "Peer %s went away, dropping the witness broadcast and peer",
                    peer,
                    exc_info=True,
                )
                await peer.disconnect(DisconnectReason.TIMEOUT)
                self.logger.warning("Peer %s successfully disconnected", peer)
                continue
            except Exception:
                self.logger.exception("Unknown error when broadcasting witness")
                raise

    async def run(self) -> None:
        """
        Request all nodes in the queue, running indefinitely
        """
        self.logger.info("Starting witness metadata broadcaster")
        with self.subscribe(self._peer_pool):
            await self._broadcast_new_witnesses()
