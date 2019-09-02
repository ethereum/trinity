import asyncio
from typing import (
    Dict,
    Iterable,
    Optional,
    Set,
    Sequence,
    Tuple,
    cast,
)

from cancel_token import (
    CancelToken,
)

from eth_typing import (
    Hash32,
)

from eth_utils import ValidationError, to_tuple

from eth.constants import ZERO_HASH32
from eth.exceptions import (
    BlockNotFound,
)

from eth2.beacon.helpers import (
    compute_start_slot_of_epoch,
)
from eth2.beacon.chains.base import (
    BaseBeaconChain,
)
from eth2.beacon.types.attestations import (
    Attestation,
)
from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
)
from eth2.beacon.typing import (
    Epoch,
    Slot,
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
from libp2p.network.network_interface import (
    INetwork,
)
from libp2p.network.stream.net_stream_interface import (
    INetStream,
)
from libp2p.peer.id import (
    ID,
)
from libp2p.peer.peerdata import (
    PeerData,
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
from libp2p.security.insecure.transport import PLAINTEXT_PROTOCOL_ID, InsecureTransport
from libp2p.stream_muxer.abc import IMuxedConn
from libp2p.stream_muxer.mplex.mplex import MPLEX_PROTOCOL_ID, Mplex

from multiaddr import (
    Multiaddr,
    protocols,
)

import ssz

from p2p.service import (
    BaseService,
)

from .configs import (
    GOSSIPSUB_PROTOCOL_ID,
    GossipsubParams,
    PUBSUB_TOPIC_BEACON_BLOCK,
    PUBSUB_TOPIC_BEACON_ATTESTATION,
    REQ_RESP_BEACON_BLOCKS,
    REQ_RESP_GOODBYE,
    REQ_RESP_HELLO,
    REQ_RESP_RECENT_BEACON_BLOCKS,
    ResponseCode,
)
from .exceptions import (
    HandshakeFailure,
    ReadMessageFailure,
    RequestFailure,
    WriteMessageFailure,
)
from .messages import (
    HelloRequest,
    BeaconBlocksRequest,
    BeaconBlocksResponse,
    RecentBeaconBlocksRequest,
    RecentBeaconBlocksResponse,
)
from .utils import (
    make_rpc_v1_ssz_protocol_id,
    make_tcp_ip_maddr,
    read_req,
    read_resp,
    write_req,
    write_resp,
)


REQ_RESP_HELLO_SSZ = make_rpc_v1_ssz_protocol_id(REQ_RESP_HELLO)
REQ_RESP_GOODBYE_SSZ = make_rpc_v1_ssz_protocol_id(REQ_RESP_GOODBYE)
REQ_RESP_BEACON_BLOCKS_SSZ = make_rpc_v1_ssz_protocol_id(REQ_RESP_BEACON_BLOCKS)
REQ_RESP_RECENT_BEACON_BLOCKS_SSZ = make_rpc_v1_ssz_protocol_id(REQ_RESP_RECENT_BEACON_BLOCKS)


class Node(BaseService):

    key_pair: KeyPair
    listen_ip: str
    listen_port: int
    host: BasicHost
    pubsub: Pubsub
    bootstrap_nodes: Optional[Tuple[Multiaddr, ...]]
    preferred_nodes: Optional[Tuple[Multiaddr, ...]]
    chain: BaseBeaconChain

    handshaked_peers: Set[ID]

    def __init__(
            self,
            key_pair: KeyPair,
            listen_ip: str,
            listen_port: int,
            chain: BaseBeaconChain,
            security_protocol_ops: Dict[TProtocol, BaseSecureTransport] = None,
            muxer_protocol_ops: Dict[TProtocol, IMuxedConn] = None,
            gossipsub_params: Optional[GossipsubParams] = None,
            cancel_token: CancelToken = None,
            bootstrap_nodes: Tuple[Multiaddr, ...] = None,
            preferred_nodes: Tuple[Multiaddr, ...] = None) -> None:
        super().__init__(cancel_token)
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.key_pair = key_pair
        self.bootstrap_nodes = bootstrap_nodes
        self.preferred_nodes = preferred_nodes
        # TODO: Add key and peer_id to the peerstore
        if security_protocol_ops is None:
            security_protocol_ops = {
                PLAINTEXT_PROTOCOL_ID: InsecureTransport(key_pair)
            }
        if muxer_protocol_ops is None:
            muxer_protocol_ops = {MPLEX_PROTOCOL_ID: Mplex}
        network: INetwork = initialize_default_swarm(
            key_pair=key_pair,
            transport_opt=[self.listen_maddr],
            muxer_opt=muxer_protocol_ops,
            sec_opt=security_protocol_ops,
            peerstore_opt=None,  # let the function initialize it
            disc_opt=None,  # no routing required here
        )
        self.host = BasicHost(network=network, router=None)

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

        self.handshaked_peers = set()

    async def _run(self) -> None:
        self.run_daemon_task(self.start())
        self.logger.info("libp2p node %s is up", self.listen_maddr)
        await self.cancellation()

    async def start(self) -> None:
        # host
        self._register_rpc_handlers()
        # TODO: Register notifees
        await self.host.get_network().listen(self.listen_maddr)
        await self.connect_preferred_nodes()
        # TODO: Connect bootstrap nodes?

        # pubsub
        await self.pubsub.subscribe(PUBSUB_TOPIC_BEACON_BLOCK)
        await self.pubsub.subscribe(PUBSUB_TOPIC_BEACON_ATTESTATION)
        # TODO: Register topic validators

    async def dial_peer(self, ip: str, port: int, peer_id: ID) -> None:
        """
        Dial the peer ``peer_id`` through the IPv4 protocol
        """
        peer_data = PeerData()
        peer_data.addrs = [make_tcp_ip_maddr(ip, port)]
        await self.host.connect(
            PeerInfo(
                peer_id=peer_id,
                peer_data=peer_data,
            )
        )

    async def dial_peer_maddr(self, maddr: Multiaddr) -> None:
        """
        Parse `maddr`, get the ip:port and PeerID, and call `dial_peer` with the parameters.
        """
        ip = maddr.value_for_protocol(protocols.P_IP4)
        port = maddr.value_for_protocol(protocols.P_TCP)
        peer_id = ID.from_base58(maddr.value_for_protocol(protocols.P_P2P))
        await self.dial_peer(ip=ip, port=port, peer_id=peer_id)

    async def connect_preferred_nodes(self) -> None:
        if self.preferred_nodes is None:
            return
        await asyncio.wait([
            self.dial_peer_maddr(node_maddr)
            for node_maddr in self.preferred_nodes
        ])

    async def broadcast_beacon_block(self, block: BaseBeaconBlock) -> None:
        await self._broadcast_data(PUBSUB_TOPIC_BEACON_BLOCK, ssz.encode(block))

    async def broadcast_attestations(self, attestations: Sequence[Attestation]) -> None:
        await self._broadcast_data(PUBSUB_TOPIC_BEACON_ATTESTATION, ssz.encode(attestations))

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
        return self.listen_maddr.encapsulate(Multiaddr(f"/p2p/{self.peer_id}"))

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
        self.host.set_stream_handler(REQ_RESP_HELLO_SSZ, self._handle_hello)
        self.host.set_stream_handler(REQ_RESP_BEACON_BLOCKS_SSZ, self._handle_beacon_blocks)
        self.host.set_stream_handler(
            REQ_RESP_RECENT_BEACON_BLOCKS_SSZ,
            self._handle_recent_beacon_blocks,
        )

    #
    # RPC Handlers
    #

    # TODO: Add a wrapper or decorator to handle the exceptions in handlers,
    #   to close the streams safely. Probably starting from: if the function
    #   returns successfully, then close the stream. Otherwise, reset the stream.

    # TODO: Handle the reputation of peers. Deduct their scores and even disconnect when they
    #   behave.

    # TODO: Register notifee to the `Network` to
    #   - Record peers' joining time.
    #   - Disconnect peers when they fail to join in a certain amount of time.

    async def _validate_hello_req(self, hello_other_side: HelloRequest) -> None:
        state_machine = self.chain.get_state_machine()
        state = self.chain.get_head_state()
        config = state_machine.config
        if hello_other_side.fork_version != state.fork.current_version:
            raise ValidationError(
                "`fork_version` mismatches: "
                f"hello_other_side.fork_version={hello_other_side.fork_version}, "
                f"state.fork.current_version={state.fork.current_version}"
            )

        # Can not validate the checkpoint with `finalized_epoch` higher than ours
        if hello_other_side.finalized_epoch > state.finalized_checkpoint.epoch:
            return

        # Get the finalized root at `hello_other_side.finalized_epoch`
        # Edge case where nothing is finalized yet
        if (
            hello_other_side.finalized_epoch == 0 and
            hello_other_side.finalized_root == ZERO_HASH32
        ):
            return

        finalized_epoch_start_slot = compute_start_slot_of_epoch(
            hello_other_side.finalized_epoch,
            config.SLOTS_PER_EPOCH,
        )
        finalized_root = self.chain.get_canonical_block_root(
            finalized_epoch_start_slot)

        if hello_other_side.finalized_root != finalized_root:
            raise ValidationError(
                "`finalized_root` mismatches: "
                f"hello_other_side.finalized_root={hello_other_side.finalized_root}, "
                f"hello_other_side.finalized_epoch={hello_other_side.finalized_epoch}, "
                f"our `finalized_root` at the same `finalized_epoch`={finalized_root}"
            )

    async def _request_beacon_blocks(self) -> None:
        """
        TODO:
        Once the handshake completes, the client with the lower `finalized_epoch` or
        `head_slot` (if the clients have equal `finalized_epochs`) SHOULD request beacon blocks
        from its counterparty via the BeaconBlocks request.
        """

    def _make_hello_packet(self) -> HelloRequest:
        state = self.chain.get_head_state()
        finalized_checkpoint = state.finalized_checkpoint
        return HelloRequest(
            fork_version=state.fork.current_version,
            finalized_root=finalized_checkpoint.root,
            finalized_epoch=finalized_checkpoint.epoch,
            head_root=state.hash_tree_root,
            head_slot=state.slot,
        )

    def _compare_chain_tip_and_finalized_epoch(self,
                                               peer_finalized_epoch: Epoch,
                                               peer_head_slot: Slot) -> None:
        checkpoint = self.chain.get_head_state().finalized_checkpoint
        head_block = self.chain.get_canonical_head()
        peer_has_higher_finalized_epoch = peer_finalized_epoch > checkpoint.epoch
        peer_has_equal_finalized_epoch = peer_finalized_epoch == checkpoint.epoch
        peer_has_higher_head_slot = peer_head_slot > head_block.slot
        if (
            peer_has_higher_finalized_epoch or
            (peer_has_equal_finalized_epoch and peer_has_higher_head_slot)
        ):
            # TODO: kickoff syncing process with this peer
            self.logger.debug("Peer's chain is ahead of us, start syncing with the peer.")
            pass

    async def _handle_hello(self, stream: INetStream) -> None:
        # TODO: Find out when we should respond the `ResponseCode`
        #   other than `ResponseCode.SUCCESS`.

        # TODO: Handle `stream.close` and `stream.reset`

        peer_id = stream.mplex_conn.peer_id

        self.logger.debug("Waiting for hello from the other side")
        try:
            hello_other_side = await read_req(stream, HelloRequest)
        except ReadMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            # TODO: send `Goodbye` req then disconnect
            return
        self.logger.debug("Received the hello message %s", hello_other_side)

        try:
            await self._validate_hello_req(hello_other_side)
        except ValidationError as error:
            self.logger.info(
                "Handshake failed: hello message %s is invalid: %s",
                hello_other_side,
                str(error)
            )
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            # TODO: send `Goodbye` req then disconnect
            return

        hello_mine = self._make_hello_packet()

        self.logger.debug("Sending our hello message %s", hello_mine)
        try:
            await write_resp(stream, hello_mine, ResponseCode.SUCCESS)
        except WriteMessageFailure as error:
            self.logger.info(
                "Handshake failed: failed to write message %s",
                hello_mine,
            )
            # await stream.reset()
            # TODO: Disconnect
            return

        if peer_id not in self.handshaked_peers:
            self.handshaked_peers.add(peer_id)
            self.logger.debug(
                "Handshake from %s is finished. Added to the `handshake_peers`",
                peer_id,
            )

        # Check if we are behind the peer
        self._compare_chain_tip_and_finalized_epoch(
            hello_other_side.finalized_epoch,
            hello_other_side.head_slot,
        )

        await stream.close()

    async def say_hello(self, peer_id: ID) -> None:
        # TODO: Handle `stream.close` and `stream.reset`

        hello_mine = self._make_hello_packet()

        self.logger.debug(
            "Opening new stream to peer=%s with protocols=%s",
            peer_id,
            [REQ_RESP_HELLO_SSZ],
        )
        stream = await self.host.new_stream(peer_id, [REQ_RESP_HELLO_SSZ])
        self.logger.debug("Sending our hello message %s", hello_mine)
        try:
            await write_req(stream, hello_mine)
        except WriteMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            # TODO: Disconnect
            error_msg = f"fail to write request={hello_mine}"
            self.logger.info("Handshake failed: %s", error_msg)
            raise HandshakeFailure(error_msg) from error

        self.logger.debug("Waiting for hello from the other side")
        try:
            resp_code, hello_other_side = await read_resp(stream, HelloRequest)
        except ReadMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            # TODO: Disconnect
            self.logger.info("Handshake failed: fail to read the response")
            raise HandshakeFailure("fail to read the response") from error

        self.logger.debug(
            "Received the hello message %s, resp_code=%s",
            hello_other_side,
            resp_code,
        )

        # TODO: Handle the case when `resp_code` is not success.
        if resp_code != ResponseCode.SUCCESS:
            # TODO: Do something according to the `ResponseCode`
            # TODO: Disconnect
            error_msg = (
                "resp_code != ResponseCode.SUCCESS, "
                f"resp_code={resp_code}, error_msg={hello_other_side}"
            )
            self.logger.info("Handshake failed: %s", error_msg)
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            # TODO: Disconnect
            raise HandshakeFailure(error_msg)

        hello_other_side = cast(HelloRequest, hello_other_side)
        try:
            await self._validate_hello_req(hello_other_side)
        except ValidationError as error:
            error_msg = f"hello message {hello_other_side} is invalid: {str(error)}"
            self.logger.info(
                "Handshake failed: %s. Disconnecting %s",
                error_msg,
                peer_id,
            )
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            # TODO: Disconnect
            raise HandshakeFailure(error_msg) from error

        if peer_id not in self.handshaked_peers:
            self.handshaked_peers.add(peer_id)
            self.logger.debug(
                "Handshake to peer=%s is finished. Added to the `handshake_peers`",
                peer_id,
            )

        # Check if we are behind the peer
        self._compare_chain_tip_and_finalized_epoch(
            hello_other_side.finalized_epoch,
            hello_other_side.head_slot,
        )

        await stream.close()

    @to_tuple
    def _get_blocks_from_canonical_chain_by_slot(
        self,
        slot_of_requested_blocks: Sequence[Slot],
    ) -> Iterable[BaseBeaconBlock]:
        # If peer's head block is on our canonical chain,
        # start getting the requested blocks by slots.
        for slot in slot_of_requested_blocks:
            try:
                block = self.chain.get_canonical_block_by_slot(slot)
            except BlockNotFound:
                pass
            else:
                yield block

    @to_tuple
    def _get_blocks_from_fork_chain_by_root(
        self,
        start_slot: Slot,
        peer_head_block: BaseBeaconBlock,
        slot_of_requested_blocks: Sequence[Slot],
    ) -> Iterable[BaseBeaconBlock]:
        # Peer's head block is on a fork chain,
        # start getting the requested blocks by
        # traversing the history from the head.

        # `slot_of_requested_blocks` starts with earliest slot
        # and end with most recent slot, so we start traversing
        # from the most recent slot.
        cur_index = len(slot_of_requested_blocks) - 1
        block = peer_head_block
        if block.slot == slot_of_requested_blocks[cur_index]:
            yield block
            cur_index -= 1
        while block.slot > start_slot and cur_index >= 0:
            try:
                block = self.chain.get_block_by_root(block.parent_root)
            except (BlockNotFound, ValidationError):
                # This should not happen as we only persist block if its
                # ancestors are also in the database.
                break
            else:
                while block.slot < slot_of_requested_blocks[cur_index]:
                    if cur_index > 0:
                        cur_index -= 1
                    else:
                        break
                if block.slot == slot_of_requested_blocks[cur_index]:
                    yield block

    def _validate_start_slot(self, start_slot: Slot) -> None:
        state_machine = self.chain.get_state_machine()
        finalized_epoch_start_slot = compute_start_slot_of_epoch(
            epoch=state_machine.state.finalized_checkpoint.epoch,
            slots_per_epoch=state_machine.config.SLOTS_PER_EPOCH,
        )
        if start_slot < finalized_epoch_start_slot:
            raise ValidationError(
                f"`start_slot`({start_slot}) lower than our"
                f" latest finalized slot({finalized_epoch_start_slot})"
            )

    def _get_requested_beacon_blocks(
        self,
        beacon_blocks_request: BeaconBlocksRequest,
        peer_head_block: BaseBeaconBlock,
    ) -> Tuple[BaseBeaconBlock, ...]:
        slot_of_requested_blocks = tuple(
            beacon_blocks_request.start_slot + i * beacon_blocks_request.step
            for i in range(beacon_blocks_request.count)
        )
        slot_of_requested_blocks = tuple(filter(
            lambda slot: slot <= peer_head_block.slot,
            slot_of_requested_blocks,
        ))

        if len(slot_of_requested_blocks) == 0:
            return tuple()

        # We have the peer's head block in our database,
        # next check if the head block is on our canonical chain.
        try:
            canonical_block_at_slot = self.chain.get_canonical_block_by_slot(
                peer_head_block.slot
            )
            block_match = canonical_block_at_slot == peer_head_block
        except BlockNotFound:
            # Peer's head block is not on our canonical chain
            block_match = False
        finally:
            if block_match:
                # Peer's head block is on our canonical chain
                return self._get_blocks_from_canonical_chain_by_slot(slot_of_requested_blocks)
            else:
                # Peer's head block is not on our canonical chain
                # Validate `start_slot` is greater than our latest finalized slot
                self._validate_start_slot(beacon_blocks_request.start_slot)
                return self._get_blocks_from_fork_chain_by_root(
                    beacon_blocks_request.start_slot,
                    peer_head_block,
                    slot_of_requested_blocks,
                )

    async def _handle_beacon_blocks(self, stream: INetStream) -> None:
        # TODO: Handle `stream.close` and `stream.reset`
        peer_id = stream.mplex_conn.peer_id
        if peer_id not in self.handshaked_peers:
            self.logger.info(
                "Processing beacon blocks request failed: not handshaked with peer=%s yet",
                peer_id,
            )
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            return

        self.logger.debug("Waiting for beacon blocks request from the other side")
        try:
            beacon_blocks_request = await read_req(stream, BeaconBlocksRequest)
        except ReadMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            return
        self.logger.debug("Received the beacon blocks request message %s", beacon_blocks_request)

        try:
            peer_head_block = self.chain.get_block_by_root(beacon_blocks_request.head_block_root)
        except (BlockNotFound, ValidationError):
            # We don't have the chain data peer is requesting
            requested_beacon_blocks: Tuple[BaseBeaconBlock, ...] = tuple()
        else:
            # Check if slot of specified head block is greater than specified start slot
            if peer_head_block.slot < beacon_blocks_request.start_slot:
                reason = (
                    f"Invalid request: head block slot({peer_head_block.slot})"
                    f" lower than `start_slot`({beacon_blocks_request.start_slot})"
                )
                try:
                    await write_resp(stream, reason, ResponseCode.INVALID_REQUEST)
                except WriteMessageFailure as error:
                    self.logger.info(
                        "Processing beacon blocks request failed: failed to write message %s",
                        reason,
                    )
                # await stream.reset()
                return
            else:
                try:
                    requested_beacon_blocks = self._get_requested_beacon_blocks(
                        beacon_blocks_request,
                        peer_head_block,
                    )
                except ValidationError as val_error:
                    reason = "Invalid request: " + str(val_error)
                    try:
                        await write_resp(stream, reason, ResponseCode.INVALID_REQUEST)
                    except WriteMessageFailure as error:
                        self.logger.info(
                            "Processing beacon blocks request failed: failed to write message %s",
                            reason,
                        )
                    # await stream.reset()
                    return

        # TODO: Should it be a successful response if peer is requesting
        # blocks on a fork we don't have data for?
        beacon_blocks_response = BeaconBlocksResponse(blocks=requested_beacon_blocks)
        self.logger.debug("Sending beacon blocks response %s", )
        try:
            await write_resp(stream, beacon_blocks_response, ResponseCode.SUCCESS)
        except WriteMessageFailure as error:
            self.logger.info(
                "Processing beacon blocks request failed: failed to write message %s",
                beacon_blocks_response,
            )
            # await stream.reset()
            return

        self.logger.debug(
            "Processing beacon blocks request from %s is finished",
            peer_id,
        )

    async def request_beacon_blocks(self,
                                    peer_id: ID,
                                    head_block_root: Hash32,
                                    start_slot: Slot,
                                    count: int,
                                    step: int) -> Tuple[BaseBeaconBlock, ...]:
        # TODO: Handle `stream.close` and `stream.reset`
        if peer_id not in self.handshaked_peers:
            error_msg = f"not handshaked with peer={peer_id} yet"
            self.logger.info("Request beacon block failed: %s", error_msg)
            raise RequestFailure(error_msg)

        beacon_blocks_request = BeaconBlocksRequest(
            head_block_root=head_block_root,
            start_slot=start_slot,
            count=count,
            step=step,
        )

        self.logger.debug(
            "Opening new stream to peer=%s with protocols=%s",
            peer_id,
            [REQ_RESP_BEACON_BLOCKS_SSZ],
        )
        stream = await self.host.new_stream(peer_id, [REQ_RESP_BEACON_BLOCKS_SSZ])
        self.logger.debug("Sending beacon blocks request %s", beacon_blocks_request)
        try:
            await write_req(stream, beacon_blocks_request)
        except WriteMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            error_msg = f"fail to write request={beacon_blocks_request}"
            self.logger.info("Request beacon blocks failed: %s", error_msg)
            raise RequestFailure(error_msg) from error

        self.logger.debug("Waiting for beacon blocks response")
        try:
            resp_code, beacon_blocks_response = await read_resp(stream, BeaconBlocksResponse)
        except ReadMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            self.logger.info("Request beacon blocks failed: fail to read the response")
            raise RequestFailure("fail to read the response") from error

        self.logger.debug(
            "Received beacon blocks response %s, resp_code=%s",
            beacon_blocks_response,
            resp_code,
        )

        if resp_code != ResponseCode.SUCCESS:
            error_msg = (
                "resp_code != ResponseCode.SUCCESS, "
                f"resp_code={resp_code}, error_msg={beacon_blocks_response}"
            )
            self.logger.info("Request beacon blocks failed: %s", error_msg)
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            raise RequestFailure(error_msg)

        await stream.close()

        beacon_blocks_response = cast(BeaconBlocksResponse, beacon_blocks_response)
        return beacon_blocks_response.blocks

    async def _handle_recent_beacon_blocks(self, stream: INetStream) -> None:
        # TODO: Handle `stream.close` and `stream.reset`
        peer_id = stream.mplex_conn.peer_id
        if peer_id not in self.handshaked_peers:
            self.logger.info(
                "Processing recent beacon blocks request failed: not handshaked with peer=%s yet",
                peer_id,
            )
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            return

        self.logger.debug("Waiting for recent beacon blocks request from the other side")
        try:
            recent_beacon_blocks_request = await read_req(stream, RecentBeaconBlocksRequest)
        except ReadMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            return
        self.logger.debug(
            "Received the recent beacon blocks request message %s",
            recent_beacon_blocks_request,
        )

        recent_beacon_blocks = []
        for block_root in recent_beacon_blocks_request.block_roots:
            try:
                block = self.chain.get_block_by_root(block_root)
            except (BlockNotFound, ValidationError):
                pass
            else:
                recent_beacon_blocks.append(block)

        recent_beacon_blocks_response = RecentBeaconBlocksResponse(blocks=recent_beacon_blocks)
        self.logger.debug("Sending recent beacon blocks response %s", recent_beacon_blocks_response)
        try:
            await write_resp(stream, recent_beacon_blocks_response, ResponseCode.SUCCESS)
        except WriteMessageFailure as error:
            self.logger.info(
                "Processing recent beacon blocks request failed: failed to write message %s",
                recent_beacon_blocks_response,
            )
            # await stream.reset()
            return

        self.logger.debug(
            "Processing recent beacon blocks request from %s is finished",
            peer_id,
        )

    async def request_recent_beacon_blocks(
            self,
            peer_id: ID,
            block_roots: Sequence[Hash32]) -> Tuple[BaseBeaconBlock, ...]:
        # TODO: Handle `stream.close` and `stream.reset`
        if peer_id not in self.handshaked_peers:
            error_msg = f"not handshaked with peer={peer_id} yet"
            self.logger.info("Request recent beacon block failed: %s", error_msg)
            raise RequestFailure(error_msg)

        recent_beacon_blocks_request = RecentBeaconBlocksRequest(block_roots=block_roots)

        self.logger.debug(
            "Opening new stream to peer=%s with protocols=%s",
            peer_id,
            [REQ_RESP_RECENT_BEACON_BLOCKS_SSZ],
        )
        stream = await self.host.new_stream(peer_id, [REQ_RESP_RECENT_BEACON_BLOCKS_SSZ])
        self.logger.debug("Sending recent beacon blocks request %s", recent_beacon_blocks_request)
        try:
            await write_req(stream, recent_beacon_blocks_request)
        except WriteMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            error_msg = f"fail to write request={recent_beacon_blocks_request}"
            self.logger.info("Request recent beacon blocks failed: %s", error_msg)
            raise RequestFailure(error_msg) from error

        self.logger.debug("Waiting for recent beacon blocks response")
        try:
            resp_code, recent_beacon_blocks_response = await read_resp(
                stream,
                RecentBeaconBlocksResponse,
            )
        except ReadMessageFailure as error:
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            self.logger.info("Request recent beacon blocks failed: fail to read the response")
            raise RequestFailure("fail to read the response") from error

        self.logger.debug(
            "Received recent beacon blocks response %s, resp_code=%s",
            recent_beacon_blocks_response,
            resp_code,
        )

        if resp_code != ResponseCode.SUCCESS:
            error_msg = (
                "resp_code != ResponseCode.SUCCESS, "
                f"resp_code={resp_code}, error_msg={recent_beacon_blocks_response}"
            )
            self.logger.info("Request recent beacon blocks failed: %s", error_msg)
            # FIXME: Use `Stream.reset()` when `NetStream` has this API.
            # await stream.reset()
            raise RequestFailure(error_msg)

        await stream.close()

        recent_beacon_blocks_response = cast(
            RecentBeaconBlocksResponse,
            recent_beacon_blocks_response,
        )
        return recent_beacon_blocks_response.blocks
