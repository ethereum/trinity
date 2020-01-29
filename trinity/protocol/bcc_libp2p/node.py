import asyncio
from dataclasses import dataclass
import logging
import operator
import random
from typing import (
    AsyncIterator,
    Dict,
    Optional,
    Sequence,
    Set,
    Tuple,
    Iterator,
)

from lahja import (
    EndpointAPI,
)

from cancel_token import (
    CancelToken,
)

from eth_utils import encode_hex
from eth_utils.toolz import first

from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from eth2.beacon.types.aggregate_and_proof import (
    AggregateAndProof,
)
from eth2.beacon.types.attestations import (
    Attestation,
)
from eth2.beacon.types.blocks import (
    BaseSignedBeaconBlock,
    SignedBeaconBlock,
)
from eth2.beacon.typing import (
    Epoch,
    Slot,
    Version,
    Root,
    SubnetId,
)

from libp2p import (
    initialize_default_swarm,
)
from libp2p.typing import TProtocol

from libp2p.crypto.keys import (
    KeyPair,
)
from libp2p.host.basic_host import (
    BasicHost,
)
from libp2p.host.exceptions import (
    StreamFailure,
)
from libp2p.network.network_interface import (
    INetwork,
)
from libp2p.security.secio.transport import ID as SecIOID
from libp2p.security.secio.transport import Transport as SecIOTransport
from libp2p.network.stream.net_stream_interface import (
    INetStream,
)
from libp2p.network.exceptions import SwarmException
from libp2p.peer.id import (
    ID,
)
from libp2p.peer.peerinfo import (
    PeerInfo,
)
from libp2p.peer.peerstore import (
    PeerStore,
)
from libp2p.pubsub.pubsub import (
    Pubsub,
)
from libp2p.pubsub.gossipsub import (
    GossipSub,
)
from libp2p.security.base_transport import BaseSecureTransport
from libp2p.stream_muxer.abc import IMuxedConn
from libp2p.stream_muxer.mplex.mplex import MPLEX_PROTOCOL_ID, Mplex

from multiaddr import (
    Multiaddr,
    protocols,
)
from multiaddr.exceptions import (
    BinaryParseError,
    ProtocolLookupError,
)

import ssz

from p2p.service import (
    BaseService,
)
from trinity.sync.beacon.events import SyncRequest

from .configs import (
    GOSSIPSUB_PROTOCOL_ID,
    GoodbyeReasonCode,
    GossipsubParams,
    PUBSUB_TOPIC_BEACON_AGGREGATE_AND_PROOF,
    PUBSUB_TOPIC_BEACON_BLOCK,
    PUBSUB_TOPIC_BEACON_ATTESTATION,
    PUBSUB_TOPIC_COMMITTEE_BEACON_ATTESTATION,
    REQ_RESP_BEACON_BLOCKS_BY_RANGE,
    REQ_RESP_GOODBYE,
    REQ_RESP_STATUS,
    REQ_RESP_BEACON_BLOCKS_BY_ROOT,
    ResponseCode,
)
from .exceptions import (
    DialPeerError,
    HandshakeFailure,
    IrrelevantNetwork,
    InvalidRequest,
    MessageIOFailure,
    NodeStartError,
    PeerRespondedAnError,
    ReadMessageFailure,
    RequestFailure,
    UnhandshakedPeer,
    WriteMessageFailure,
)
from .messages import (
    Goodbye,
    Status,
    BeaconBlocksByRangeRequest,
    BeaconBlocksByRootRequest,
)
from .topic_validators import (
    get_beacon_aggregate_and_proof_validator,
    get_beacon_attestation_validator,
    get_beacon_block_validator,
    get_committee_index_beacon_attestation_validator,
)
from .utils import (
    make_rpc_v1_ssz_protocol_id,
    make_tcp_ip_maddr,
    Interaction,
    peer_is_ahead,
    validate_peer_status,
    get_my_status,
    get_requested_beacon_blocks,
    get_beacon_blocks_by_root,
)
from async_generator import asynccontextmanager

from trinity.components.eth2.metrics.events import (
    Libp2pPeersRequest,
    Libp2pPeersResponse,
)
from trinity.http.api.events import (
    Libp2pPeerIDRequest,
    Libp2pPeerIDResponse,
)

logger = logging.getLogger('trinity.protocol.bcc_libp2p')


REQ_RESP_STATUS_SSZ = make_rpc_v1_ssz_protocol_id(REQ_RESP_STATUS)
REQ_RESP_GOODBYE_SSZ = make_rpc_v1_ssz_protocol_id(REQ_RESP_GOODBYE)
REQ_RESP_BEACON_BLOCKS_BY_RANGE_SSZ = make_rpc_v1_ssz_protocol_id(
    REQ_RESP_BEACON_BLOCKS_BY_RANGE
)
REQ_RESP_BEACON_BLOCKS_BY_ROOT_SSZ = make_rpc_v1_ssz_protocol_id(
    REQ_RESP_BEACON_BLOCKS_BY_ROOT
)
NEXT_UPDATE_INTERVAL = 10


@dataclass
class Peer:

    node: "Node"
    _id: ID
    head_fork_version: Version  # noqa: E701
    finalized_root: Root
    finalized_epoch: Epoch
    head_root: Root
    head_slot: Slot

    @classmethod
    def from_status(
        cls, node: "Node", peer_id: ID, status: Status
    ) -> "Peer":
        return cls(
            node=node,
            _id=peer_id,
            head_fork_version=status.head_fork_version,
            finalized_root=status.finalized_root,
            finalized_epoch=status.finalized_epoch,
            head_root=status.head_root,
            head_slot=status.head_slot,
        )

    async def request_beacon_blocks_by_range(
        self, start_slot: Slot, count: int, step: int = 1
    ) -> Tuple[BaseSignedBeaconBlock, ...]:
        return await self.node.request_beacon_blocks_by_range(
            self._id,
            head_block_root=self.head_root,
            start_slot=start_slot,
            count=count,
            step=step,
        )

    async def request_beacon_blocks_by_root(
        self, block_roots: Sequence[Root]
    ) -> Tuple[BaseSignedBeaconBlock, ...]:
        return await self.node.request_beacon_blocks_by_root(self._id, block_roots)

    def __repr__(self) -> str:
        return (
            f"Peer {self._id} "
            f"head_fork_version={encode_hex(self.head_fork_version)} "
            f"finalized_root={encode_hex(self.finalized_root)} "
            f"finalized_epoch={self.finalized_epoch} "
            f"head_root={encode_hex(self.head_root)} "
            f"head_slot={self.head_slot}"
        )


class PeerPool:
    peers: Dict[ID, Peer]

    def __init__(self) -> None:
        self.peers = {}

    def add(self, peer: Peer) -> None:
        self.peers[peer._id] = peer

    def remove(self, peer_id: ID) -> None:
        del self.peers[peer_id]

    def __contains__(self, peer_id: ID) -> bool:
        return peer_id in self.peers.keys()

    def __len__(self) -> int:
        return len(self.peers)

    def get_best(self, field: str) -> Peer:
        sorted_peers = sorted(
            self.peers.values(), key=operator.attrgetter(field), reverse=True
        )
        return first(sorted_peers)

    def get_best_head_slot_peer(self) -> Peer:
        return self.get_best("head_slot")

    @property
    def peer_ids(self) -> Iterator[ID]:
        return iter(self.peers.keys())


DIAL_RETRY_COUNT = 10


class Node(BaseService):

    _is_started: bool = False

    key_pair: KeyPair
    listen_ip: str
    listen_port: int
    host: BasicHost
    pubsub: Pubsub
    bootstrap_nodes: Tuple[Multiaddr, ...]
    preferred_nodes: Tuple[Multiaddr, ...]
    chain: BaseBeaconChain
    subnets: Set[SubnetId]
    _event_bus: EndpointAPI

    handshaked_peers: PeerPool = None

    def __init__(
            self,
            key_pair: KeyPair,
            listen_ip: str,
            listen_port: int,
            chain: BaseBeaconChain,
            event_bus: EndpointAPI,
            security_protocol_ops: Dict[TProtocol, BaseSecureTransport] = None,
            muxer_protocol_ops: Dict[TProtocol, IMuxedConn] = None,
            gossipsub_params: Optional[GossipsubParams] = None,
            cancel_token: CancelToken = None,
            bootstrap_nodes: Tuple[Multiaddr, ...] = (),
            preferred_nodes: Tuple[Multiaddr, ...] = (),
            subnets: Optional[Set[SubnetId]] = None) -> None:
        super().__init__(cancel_token)
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.key_pair = key_pair
        self.bootstrap_nodes = bootstrap_nodes
        self.preferred_nodes = preferred_nodes
        self.subnets = subnets if subnets is not None else set()
        # TODO: Add key and peer_id to the peerstore
        if security_protocol_ops is None:
            security_protocol_ops = {
                SecIOID: SecIOTransport(key_pair)
            }
        if muxer_protocol_ops is None:
            muxer_protocol_ops = {MPLEX_PROTOCOL_ID: Mplex}
        network: INetwork = initialize_default_swarm(
            key_pair=key_pair,
            transport_opt=[self.listen_maddr],
            muxer_opt=muxer_protocol_ops,
            sec_opt=security_protocol_ops,
            peerstore_opt=None,  # let the function initialize it
        )
        self.host = BasicHost(network=network)

        if gossipsub_params is None:
            gossipsub_params = GossipsubParams()
        gossipsub_router = GossipSub(
            protocols=[GOSSIPSUB_PROTOCOL_ID],
            degree=gossipsub_params.DEGREE,
            degree_low=gossipsub_params.DEGREE_LOW,
            degree_high=gossipsub_params.DEGREE_HIGH,
            time_to_live=gossipsub_params.FANOUT_TTL,
            gossip_window=gossipsub_params.GOSSIP_WINDOW,
            gossip_history=gossipsub_params.GOSSIP_HISTORY,
            heartbeat_interval=gossipsub_params.HEARTBEAT_INTERVAL,
        )
        self.pubsub = Pubsub(
            host=self.host,
            router=gossipsub_router,
            my_id=self.peer_id,
        )

        self.chain = chain
        self._event_bus = event_bus

        self.handshaked_peers = PeerPool()

        self.run_task(self.start())

    @property
    def is_started(self) -> bool:
        return self._is_started

    async def _run(self) -> None:
        self.logger.info("libp2p node %s is up", self.listen_maddr)
        self.run_daemon_task(self.update_status())

        # Metrics and HTTP APIs
        self.run_daemon_task(self.handle_libp2p_peers_requests())
        self.run_daemon_task(self.handle_libp2p_peer_id_requests())

        await self.cancellation()

    async def start(self) -> None:
        # host
        self._register_rpc_handlers()
        # TODO: Register notifees
        is_listening = await self.host.get_network().listen(self.listen_maddr)
        if not is_listening:
            self.logger.error("Fail to listen on maddr: %s", self.listen_maddr)
            raise NodeStartError(f"Fail to listen on maddr: {self.listen_maddr}")
        self.logger.warning("Node listening: %s", self.listen_maddr_with_peer_id)
        await self.connect_preferred_nodes()
        # TODO: Connect bootstrap nodes?

        # Pubsub
        # Global channel
        await self.pubsub.subscribe(PUBSUB_TOPIC_BEACON_BLOCK)
        await self.pubsub.subscribe(PUBSUB_TOPIC_BEACON_ATTESTATION)
        await self.pubsub.subscribe(PUBSUB_TOPIC_BEACON_AGGREGATE_AND_PROOF)
        # Attestation subnets
        for subnet_id in self.subnets:
            topic = PUBSUB_TOPIC_COMMITTEE_BEACON_ATTESTATION.substitute(subnet_id=str(subnet_id))
            await self.pubsub.subscribe(topic)

        self._setup_topic_validators()

        self._is_started = True

    def _setup_topic_validators(self) -> None:
        # Global channel
        self.pubsub.set_topic_validator(
            PUBSUB_TOPIC_BEACON_BLOCK,
            get_beacon_block_validator(self.chain),
            False,
        )
        self.pubsub.set_topic_validator(
            PUBSUB_TOPIC_BEACON_ATTESTATION,
            get_beacon_attestation_validator(self.chain),
            False,
        )
        # Attestation subnets
        for subnet_id in self.subnets:
            self.pubsub.set_topic_validator(
                PUBSUB_TOPIC_COMMITTEE_BEACON_ATTESTATION.substitute(subnet_id=str(subnet_id)),
                get_committee_index_beacon_attestation_validator(self.chain, subnet_id),
                False,
            )

        self.pubsub.set_topic_validator(
            PUBSUB_TOPIC_BEACON_AGGREGATE_AND_PROOF,
            get_beacon_aggregate_and_proof_validator(self.chain),
            False,
        )

    async def dial_peer_maddr(self, maddr: Multiaddr, peer_id: ID) -> None:
        """
        Dial the peer with given multi-address
        """
        self.logger.debug("Dialing peer_id: %s, maddr: %s", peer_id, maddr)
        try:
            await self.host.connect(
                PeerInfo(
                    peer_id=peer_id,
                    addrs=[maddr],
                )
            )
        except SwarmException as e:
            self.logger.debug("Fail to dial peer_id: %s, maddr: %s, error: %s", peer_id, maddr, e)
            raise DialPeerError from e

        try:
            # TODO: set a time limit on completing handshake
            await self.request_status(peer_id)
        except HandshakeFailure as e:
            self.logger.debug("Fail to handshake with peer_id: %s, error: %s", peer_id, e)
            raise DialPeerError from e
        self.logger.debug("Successfully connect to peer_id %s maddr %s", peer_id, maddr)

    async def dial_peer_maddr_with_retries(self, maddr: Multiaddr) -> None:
        """
        Dial the peer with given multi-address repeatedly for `DIAL_RETRY_COUNT` times
        """
        try:
            p2p_id = maddr.value_for_protocol(protocols.P_P2P)
        except (BinaryParseError, ProtocolLookupError) as error:
            self.logger.debug("Invalid maddr: %s, error: %s", maddr, error)
            raise DialPeerError from error
        peer_id = ID.from_base58(p2p_id)

        for i in range(DIAL_RETRY_COUNT):
            try:
                # exponential backoff...
                await asyncio.sleep(2**i + random.random())
                await self.dial_peer_maddr(maddr, peer_id)
                return
            except DialPeerError:
                self.logger.debug(
                    "Could not dial peer: %s, maddr: %s retrying attempt %d of %d...",
                    peer_id,
                    maddr,
                    i,
                    DIAL_RETRY_COUNT,
                )
                continue
        raise DialPeerError

    async def connect_preferred_nodes(self) -> None:
        results = await asyncio.gather(
            *(self.dial_peer_maddr_with_retries(node_maddr)
              for node_maddr in self.preferred_nodes),
            return_exceptions=True,
        )
        for result, node_maddr in zip(results, self.preferred_nodes):
            if isinstance(result, Exception):
                logger.warning("Could not connect to preferred node at %s", node_maddr)

    async def disconnect_peer(self, peer_id: ID) -> None:
        if peer_id in self.handshaked_peers:
            self.logger.debug("Disconnect from %s", peer_id)
            self.handshaked_peers.remove(peer_id)
            await self.host.disconnect(peer_id)
        else:
            self.logger.debug("Already disconnected from %s", peer_id)

    async def broadcast_beacon_block(self, block: BaseSignedBeaconBlock) -> None:
        await self._broadcast_data(PUBSUB_TOPIC_BEACON_BLOCK, ssz.encode(block))

    async def broadcast_attestation(self, attestation: Attestation) -> None:
        await self._broadcast_data(PUBSUB_TOPIC_BEACON_ATTESTATION, ssz.encode(attestation))

    async def broadcast_attestation_to_subnet(
        self, attestation: Attestation, subnet_id: SubnetId
    ) -> None:
        await self._broadcast_data(
            PUBSUB_TOPIC_COMMITTEE_BEACON_ATTESTATION.substitute(subnet_id=str(subnet_id)),
            ssz.encode(attestation)
        )

    async def broadcast_beacon_aggregate_and_proof(
        self, aggregate_and_proof: AggregateAndProof
    ) -> None:
        await self._broadcast_data(
            PUBSUB_TOPIC_BEACON_AGGREGATE_AND_PROOF,
            ssz.encode(aggregate_and_proof),
        )

    async def _broadcast_data(self, topic: str, data: bytes) -> None:
        await self.pubsub.publish(topic, data)

    @property
    def peer_id(self) -> ID:
        return self.host.get_id()

    @property
    def listen_maddr(self) -> Multiaddr:
        return make_tcp_ip_maddr(self.listen_ip, self.listen_port)

    @property
    def listen_maddr_with_peer_id(self) -> Multiaddr:
        return self.listen_maddr.encapsulate(Multiaddr(f"/p2p/{self.peer_id.to_base58()}"))

    @property
    def peer_store(self) -> PeerStore:
        return self.host.get_network().peerstore

    async def close(self) -> None:
        # FIXME: Add `tear_down` to `Swarm` in the upstream
        network = self.host.get_network()
        for listener in network.listeners.values():
            listener.server.close()
            await listener.server.wait_closed()
        # TODO: Add `close` in `Pubsub`

    def _register_rpc_handlers(self) -> None:
        self.host.set_stream_handler(REQ_RESP_STATUS_SSZ, self._handle_status)
        self.host.set_stream_handler(REQ_RESP_GOODBYE_SSZ, self._handle_goodbye)
        self.host.set_stream_handler(
            REQ_RESP_BEACON_BLOCKS_BY_RANGE_SSZ,
            self._handle_beacon_blocks_by_range,
        )
        self.host.set_stream_handler(
            REQ_RESP_BEACON_BLOCKS_BY_ROOT_SSZ,
            self._handle_beacon_blocks_by_root,
        )

    #
    # RPC Handlers
    #

    async def new_stream(self, peer_id: ID, protocol: TProtocol) -> INetStream:
        return await self.host.new_stream(peer_id, [protocol])

    @asynccontextmanager
    async def new_handshake_interaction(self, stream: INetStream) -> AsyncIterator[Interaction]:
        try:
            async with Interaction(stream) as interaction:
                peer_id = interaction.peer_id
                yield interaction
        except MessageIOFailure as error:
            await self.disconnect_peer(peer_id)
            raise HandshakeFailure() from error
        except PeerRespondedAnError as error:
            await stream.reset()
            await self.disconnect_peer(peer_id)
            raise HandshakeFailure() from error
        except IrrelevantNetwork as error:
            await stream.reset()
            asyncio.ensure_future(
                self.say_goodbye(peer_id, GoodbyeReasonCode.IRRELEVANT_NETWORK)
            )
            raise HandshakeFailure from error

    @asynccontextmanager
    async def post_handshake_handler_interaction(
        self,
        stream: INetStream
    ) -> AsyncIterator[Interaction]:
        try:
            async with Interaction(stream) as interaction:
                yield interaction
        except WriteMessageFailure as error:
            self.logger.debug("WriteMessageFailure %s", error)
            return
        except ReadMessageFailure as error:
            self.logger.debug("ReadMessageFailure %s", error)
            return
        except UnhandshakedPeer:
            await stream.reset()
            return

    @asynccontextmanager
    async def my_request_interaction(self, stream: INetStream) -> AsyncIterator[Interaction]:
        try:
            async with Interaction(stream) as interaction:
                yield interaction
        except (MessageIOFailure, UnhandshakedPeer, PeerRespondedAnError) as error:
            raise RequestFailure(str(error)) from error

    # TODO: Handle the reputation of peers. Deduct their scores and even disconnect when they
    #   behave.

    # TODO: Register notifee to the `Network` to
    #   - Record peers' joining time.
    #   - Disconnect peers when they fail to join in a certain amount of time.

    def _add_peer_from_status(self, peer_id: ID, status: Status) -> None:
        peer = Peer.from_status(self, peer_id, status)
        self.handshaked_peers.add(peer)
        self.logger.debug(
            "Handshake from %s is finished. Added to the `handshake_peers`",
            peer_id,
        )

    async def _handle_status(self, stream: INetStream) -> None:
        # TODO: Find out when we should respond the `ResponseCode`
        #   other than `ResponseCode.SUCCESS`.

        async with self.new_handshake_interaction(stream) as interaction:
            peer_id = interaction.peer_id
            peer_status = await interaction.read_request(Status)
            self.logger.info("Received Status from %s  %s", str(peer_id), peer_status)
            await validate_peer_status(self.chain, peer_status)

            my_status = get_my_status(self.chain)
            await interaction.write_response(my_status)

            self._add_peer_from_status(peer_id, peer_status)

            if peer_is_ahead(self.chain, peer_status):
                logger.debug(
                    "Peer's chain is ahead of us, start syncing with the peer(%s)",
                    str(peer_id),
                )
                await self._event_bus.broadcast(SyncRequest())

    async def request_status(self, peer_id: ID) -> None:
        self.logger.info("Initiate handshake with %s", str(peer_id))

        try:
            stream = await self.new_stream(peer_id, REQ_RESP_STATUS_SSZ)
        except StreamFailure as error:
            self.logger.debug("Fail to open stream to %s", str(peer_id))
            raise HandshakeFailure from error
        async with self.new_handshake_interaction(stream) as interaction:
            my_status = get_my_status(self.chain)
            await interaction.write_request(my_status)
            peer_status = await interaction.read_response(Status)

            await validate_peer_status(self.chain, peer_status)

            self._add_peer_from_status(peer_id, peer_status)

            if peer_is_ahead(self.chain, peer_status):
                logger.debug(
                    "Peer's chain is ahead of us, start syncing with the peer(%s)",
                    str(peer_id),
                )
                await self._event_bus.broadcast(SyncRequest())

    async def _handle_goodbye(self, stream: INetStream) -> None:
        async with Interaction(stream) as interaction:
            peer_id = interaction.peer_id
            try:
                await interaction.read_request(Goodbye)
            except ReadMessageFailure:
                pass
            await self.disconnect_peer(peer_id)

    async def say_goodbye(self, peer_id: ID, reason: GoodbyeReasonCode) -> None:
        try:
            stream = await self.new_stream(peer_id, REQ_RESP_GOODBYE_SSZ)
        except StreamFailure:
            self.logger.debug("Fail to open stream to %s", str(peer_id))
        else:
            async with Interaction(stream) as interaction:
                goodbye = Goodbye.create(reason)
                try:
                    await interaction.write_request(goodbye)
                except WriteMessageFailure:
                    pass
        finally:
            await self.disconnect_peer(peer_id)

    def _check_peer_handshaked(self, peer_id: ID) -> None:
        if peer_id not in self.handshaked_peers:
            raise UnhandshakedPeer(peer_id)

    async def _handle_beacon_blocks_by_range(self, stream: INetStream) -> None:
        # TODO: Should it be a successful response if peer is requesting
        # blocks on a fork we don't have data for?

        async with self.post_handshake_handler_interaction(stream) as interaction:
            peer_id = interaction.peer_id
            self._check_peer_handshaked(peer_id)

            request = await interaction.read_request(BeaconBlocksByRangeRequest)
            try:
                blocks = get_requested_beacon_blocks(self.chain, request)
            except InvalidRequest as error:
                error_message = str(error)[:128]
                await interaction.write_error_response(error_message, ResponseCode.INVALID_REQUEST)
            else:
                await interaction.write_chunk_response(blocks)

    async def request_beacon_blocks_by_range(
        self,
        peer_id: ID,
        head_block_root: Root,
        start_slot: Slot,
        count: int,
        step: int,
    ) -> Tuple[BaseSignedBeaconBlock, ...]:
        try:
            stream = await self.new_stream(peer_id, REQ_RESP_BEACON_BLOCKS_BY_RANGE_SSZ)
        except StreamFailure as error:
            self.logger.debug("Fail to open stream to %s", str(peer_id))
            raise RequestFailure(str(error)) from error
        async with self.my_request_interaction(stream) as interaction:
            self._check_peer_handshaked(peer_id)
            request = BeaconBlocksByRangeRequest.create(
                head_block_root=head_block_root,
                start_slot=start_slot,
                count=count,
                step=step,
            )
            await interaction.write_request(request)
            blocks = tuple([
                block async for block in
                interaction.read_chunk_response(SignedBeaconBlock, count)
            ])

            return blocks

    async def _handle_beacon_blocks_by_root(self, stream: INetStream) -> None:
        async with self.post_handshake_handler_interaction(stream) as interaction:
            peer_id = interaction.peer_id
            self._check_peer_handshaked(peer_id)
            request = await interaction.read_request(BeaconBlocksByRootRequest)
            blocks = get_beacon_blocks_by_root(self.chain, request)

            await interaction.write_chunk_response(blocks)

    async def request_beacon_blocks_by_root(
            self,
            peer_id: ID,
            block_roots: Sequence[Root]) -> Tuple[BaseSignedBeaconBlock, ...]:
        try:
            stream = await self.new_stream(peer_id, REQ_RESP_BEACON_BLOCKS_BY_ROOT_SSZ)
        except StreamFailure as error:
            self.logger.debug("Fail to open stream to %s", str(peer_id))
            raise RequestFailure(str(error)) from error
        async with self.my_request_interaction(stream) as interaction:
            self._check_peer_handshaked(peer_id)
            request = BeaconBlocksByRootRequest.create(block_roots=block_roots)
            await interaction.write_request(request)
            blocks = tuple([
                block async for block in
                interaction.read_chunk_response(SignedBeaconBlock, len(block_roots))
            ])

            return blocks

    async def update_status(self) -> None:
        while True:
            for peer_id in self.handshaked_peers.peer_ids:
                asyncio.ensure_future(self.request_status(peer_id))
            await asyncio.sleep(NEXT_UPDATE_INTERVAL)

    #
    # Metrics and APIs
    #
    async def handle_libp2p_peers_requests(self) -> None:
        async for req in self.wait_iter(self._event_bus.stream(Libp2pPeersRequest)):
            peers = tuple(self.handshaked_peers.peer_ids)
            await self._event_bus.broadcast(
                Libp2pPeersResponse(peers),
                req.broadcast_config(),
            )

    async def handle_libp2p_peer_id_requests(self) -> None:
        async for req in self.wait_iter(self._event_bus.stream(Libp2pPeerIDRequest)):
            await self._event_bus.broadcast(
                Libp2pPeerIDResponse(self.peer_id),
                req.broadcast_config(),
            )
