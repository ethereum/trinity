import logging
from typing import AsyncIterable, Callable, Sequence, Set

import libp2p.crypto.ed25519 as ed25519
from libp2p.crypto.keys import KeyPair
from libp2p.host.basic_host import BasicHost
from libp2p.network.swarm import Swarm
from libp2p.peer.id import ID as PeerID
from libp2p.peer.peerinfo import PeerInfo
from libp2p.peer.peerstore import PeerStore
import libp2p.security.secio.transport as secio
import libp2p.security.noise.transport as noise
from libp2p.stream_muxer.mplex.mplex import MPLEX_PROTOCOL_ID, Mplex
from libp2p.transport.tcp.tcp import TCP
from libp2p.transport.upgrader import TransportUpgrader
from libp2p.typing import TProtocol
from multiaddr import Multiaddr

from eth2.beacon.types.blocks import SignedBeaconBlock
from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.typing import Epoch, Root, Slot
from eth2.configs import Eth2Config
from trinity.nodes.beacon.gossiper import ForkDigestProvider, Gossiper
from trinity.nodes.beacon.metadata import MetaData
from trinity.nodes.beacon.metadata import SeqNumber as MetaDataSeqNumber
from trinity.nodes.beacon.request_responder import (
    BlockProviderByRoot,
    BlockProviderBySlot,
    BlocksByRangeRequest,
    GoodbyeReason,
    MetadataProvider,
    PeerUpdater,
    ReqRespProtocol,
    RequestResponder,
    StatusProvider,
)
from trinity.nodes.beacon.status import Status


class Host(BasicHost):
    logger = logging.getLogger("trinity.nodes.beacon.host.Host")

    def __init__(
        self,
        key_pair: KeyPair,
        peer_id: PeerID,
        peer_updater: PeerUpdater,
        status_provider: StatusProvider,
        finalized_root_provider: Callable[[Epoch], Root],
        block_provider_by_slot: BlockProviderBySlot,
        block_provider_by_root: BlockProviderByRoot,
        metadata_provider: MetadataProvider,
        fork_digest_provider: ForkDigestProvider,
        eth2_config: Eth2Config,
    ) -> None:
        peer_store = PeerStore()
        peer_store.add_key_pair(peer_id, key_pair)

        muxer_transports_by_protocol = {MPLEX_PROTOCOL_ID: Mplex}
        noise_key = ed25519.create_new_key_pair()
        security_transports_by_protocol = {
            TProtocol(secio.ID): secio.Transport(
                key_pair
            ),
            TProtocol(noise.PROTOCOL_ID): noise.Transport(
                key_pair, noise_key.private_key
            )
        }
        upgrader = TransportUpgrader(
            security_transports_by_protocol, muxer_transports_by_protocol
        )

        transport = TCP()

        swarm = Swarm(peer_id, peer_store, upgrader, transport)
        BasicHost.__init__(self, swarm)

        self._peer_updater = peer_updater
        self._status_provider = status_provider
        self._finalized_root_provider_by_epoch = finalized_root_provider
        self._block_provider_by_slot = block_provider_by_slot
        self._block_provider_by_root = block_provider_by_root
        self._metadata_provider = metadata_provider

        self._request_responder = RequestResponder(
            self._peer_updater,
            self._status_provider,
            self._block_provider_by_slot,
            self._block_provider_by_root,
            self._metadata_provider,
        )
        self._install_req_resp_protocols()

        self._gossiper = Gossiper(fork_digest_provider, self)

        self._peers: Set[PeerID] = set()

    def _install_req_resp_protocols(self) -> None:
        for protocol_id, handler in self._request_responder.get_protocols():
            self.set_stream_handler(protocol_id, handler)

    def _has_compatible_finalized_head(
        self, local_checkpoint: Checkpoint, remote_checkpoint: Checkpoint
    ) -> bool:
        if remote_checkpoint.epoch > local_checkpoint.epoch:
            # peer is ahead, we can't tell...
            # assume they are good and try to resolve over sync...
            return True

        if remote_checkpoint.epoch == local_checkpoint.epoch:
            return remote_checkpoint.root == local_checkpoint.root

        # peer is behind... ensure they have a checkpoint in our chain.
        finalized_root = self._finalized_root_provider_by_epoch(remote_checkpoint.epoch)
        if finalized_root:
            return finalized_root == remote_checkpoint.root
        else:
            # NOTE: unexpected to his this arm given the above code...
            return False

    def _peer_is_compatible(self, local_status: Status, remote_status: Status) -> bool:
        has_same_fork = local_status.fork_digest == remote_status.fork_digest
        has_compatible_finalized_head = self._has_compatible_finalized_head(
            local_status.finalized_checkpoint, remote_status.finalized_checkpoint
        )
        return has_same_fork and has_compatible_finalized_head

    async def add_peer_from_maddr(self, maddr: Multiaddr) -> None:
        """
        Connect to the eth2 peer at ``maddr`` and incorporate them into
        our view of the network.
        """
        peer_id_encoded = maddr.value_for_protocol("p2p")
        peer_id = PeerID.from_base58(peer_id_encoded)
        try:
            await self.connect(PeerInfo(peer_id=peer_id, addrs=[maddr]))
            local_status = self._status_provider()
            remote_status = await self.exchange_status(peer_id, local_status)
            if self._peer_is_compatible(local_status, remote_status):
                self._peers.add(peer_id)
                await self._peer_updater(peer_id, remote_status)
            else:
                await self.disconnect(peer_id)
        except Exception as e:
            self.logger.exception(e)

    async def drop_peer(self, peer_id: PeerID) -> None:
        await self.disconnect(peer_id)

    async def broadcast_block(self, block: SignedBeaconBlock) -> None:
        await self._gossiper.broadcast_block(block)

    async def stream_block_gossip(self) -> AsyncIterable[SignedBeaconBlock]:
        async for block in self._gossiper.stream_block_gossip():
            yield block

    async def subscribe_gossip_channels(self) -> None:
        await self._gossiper.subscribe_gossip_channels()

    async def unsubscribe_gossip_channels(self) -> None:
        await self._gossiper.unsubscribe_gossip_channels()

    async def exchange_status(self, peer_id: PeerID, local_status: Status) -> Status:
        """
        NOTE: ``local_status`` is explicitly passed to avoid race conditions for callers
        where the status changes while we are handshaking a peer.
        """
        stream = await self.new_stream(peer_id, (ReqRespProtocol.status.id(),))
        return await self._request_responder.send_status(stream, local_status)

    async def send_goodbye_to(self, peer_id: PeerID, reason: GoodbyeReason) -> None:
        stream = await self.new_stream(peer_id, (ReqRespProtocol.goodbye.id(),))
        await self._request_responder.send_goodbye(stream, reason)
        await self.drop_peer(peer_id)

    async def get_blocks_by_range(
        self, peer_id: PeerID, start_slot: Slot, count: int, step: int = 1
    ) -> AsyncIterable[SignedBeaconBlock]:
        request = BlocksByRangeRequest.create(
            start_slot=start_slot, count=count, step=step
        )
        if request.count_expected_blocks() < 1:
            return

        stream = await self.new_stream(
            peer_id, (ReqRespProtocol.beacon_blocks_by_range.id(),)
        )
        async for block in self._request_responder.get_blocks_by_range(stream, request):
            yield block

    async def get_blocks_by_root(
        self, peer_id: PeerID, *roots: Sequence[Root]
    ) -> AsyncIterable[SignedBeaconBlock]:
        if not roots:
            return

        stream = await self.new_stream(
            peer_id, (ReqRespProtocol.beacon_blocks_by_root.id(),)
        )
        async for block in self._request_responder.get_blocks_by_root(stream, *roots):
            yield block

    async def send_ping(self, peer_id: PeerID) -> MetaDataSeqNumber:
        stream = await self.new_stream(peer_id, (ReqRespProtocol.ping.id(),))
        return await self._request_responder.send_ping(stream)

    async def get_metadata(self, peer_id: PeerID) -> MetaData:
        stream = await self.new_stream(peer_id, (ReqRespProtocol.metadata.id(),))
        return await self._request_responder.send_metadata(stream)
