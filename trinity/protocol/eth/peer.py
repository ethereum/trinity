from abc import (
    abstractmethod,
)
from typing import (
    Any,
    cast,
    Dict,
    List,
)
import typing_extensions

from eth_utils import encode_hex
from eth_typing import (
    BlockNumber,
    Hash32,
)
from lahja import (
    BroadcastConfig,
    Endpoint,
)
from p2p.exceptions import (
    HandshakeFailure,
    NoConnectedPeers,
    WrongNetworkFailure,
    WrongGenesisFailure,
)
from p2p.peer import IdentifiablePeer
from p2p.p2p_proto import DisconnectReason
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)

from trinity.endpoint import (
    TrinityEventBusEndpoint,
)
from trinity.protocol.common.peer import (
    BaseChainPeer,
    BaseChainDTOPeer,
    BaseChainPeerFactory,
    BaseChainPeerPool,
)
from trinity.protocol.common.peer_pool_event_bus import (
    BasePeerPoolEventBusRequestHandler,
)

from .commands import (
    NewBlock,
    Status,
)
from .constants import MAX_HEADERS_FETCH
from .events import (
    SendBlockBodiesEvent,
    SendBlockHeadersEvent,
    SendNodeDataEvent,
    SendReceiptsEvent,
    GetConnectedPeersRequest,
    GetConnectedPeersResponse,
    GetBlockHeadersRequest,
    GetBlockHeadersResponse,
    GetBlockBodiesRequest,
    GetBlockBodiesResponse,
    GetHighestTDPeerRequest,
    GetHighestTDPeerResponse,
    GetNodeDataRequest,
    GetNodeDataResponse,
    GetReceiptsRequest,
    GetReceiptsResponse,
)
from .proto import (
    ETHProtocol,
    ETHProtocolLike,
    ProxyETHProtocol,
)
from .handlers import (
    ETHExchangeHandler,
    ETHExchangeHandlerLike,
    ProxyETHExchangeHandler,
)


class ETHPeerLike(typing_extensions.Protocol):

    @property
    @abstractmethod
    def sub_proto(self) -> ETHProtocolLike:
        pass

    @property
    @abstractmethod
    def requests(self) -> ETHExchangeHandlerLike:
        pass

    @property
    @abstractmethod
    def is_operational(self) -> bool:
        pass


class ETHPeer(BaseChainPeer):
    max_headers_fetch = MAX_HEADERS_FETCH

    _supported_sub_protocols = [ETHProtocol]
    sub_proto: ETHProtocol = None

    _requests: ETHExchangeHandler = None

    def get_extra_stats(self) -> List[str]:
        stats_pairs = self.requests.get_stats().items()
        return ['%s: %s' % (cmd_name, stats) for cmd_name, stats in stats_pairs]

    @property
    def requests(self) -> ETHExchangeHandler:
        if self._requests is None:
            self._requests = ETHExchangeHandler(self)
        return self._requests

    def handle_sub_proto_msg(self, cmd: Command, msg: _DecodedMsgType) -> None:
        if isinstance(cmd, NewBlock):
            msg = cast(Dict[str, Any], msg)
            header, _, _ = msg['block']
            actual_head = header.parent_hash
            actual_td = msg['total_difficulty'] - header.difficulty
            if actual_td > self.head_td:
                self.head_hash = actual_head
                self.head_td = actual_td

        super().handle_sub_proto_msg(cmd, msg)

    async def send_sub_proto_handshake(self) -> None:
        self.sub_proto.send_handshake(await self._local_chain_info)

    async def process_sub_proto_handshake(
            self, cmd: Command, msg: _DecodedMsgType) -> None:
        if not isinstance(cmd, Status):
            await self.disconnect(DisconnectReason.subprotocol_error)
            raise HandshakeFailure(f"Expected a ETH Status msg, got {cmd}, disconnecting")
        msg = cast(Dict[str, Any], msg)
        if msg['network_id'] != self.network_id:
            await self.disconnect(DisconnectReason.useless_peer)
            raise WrongNetworkFailure(
                f"{self} network ({msg['network_id']}) does not match ours "
                f"({self.network_id}), disconnecting"
            )
        genesis = await self.genesis
        if msg['genesis_hash'] != genesis.hash:
            await self.disconnect(DisconnectReason.useless_peer)
            raise WrongGenesisFailure(
                f"{self} genesis ({encode_hex(msg['genesis_hash'])}) does not "
                f"match ours ({genesis.hex_hash}), disconnecting"
            )
        self.head_td = msg['td']
        self.head_hash = msg['best_hash']


class ETHProxyPeer:
    """
    A ``ETHPeer`` that can be used from any process as a drop-in replacement for the actual
    peer that sits in the peer pool. Any action performed on the ``ETHProxyPeer`` is delegated
    to the actual peer in the pool.
    """

    def __init__(self,
                 dto_peer: BaseChainDTOPeer,
                 sub_proto: ProxyETHProtocol,
                 requests: ProxyETHExchangeHandler):

        self.dto_peer = dto_peer
        self.sub_proto = sub_proto
        self.requests = requests

    # TODO: Wondering if we should only allow one-time read and throw if code
    # tries to read again them again. I think the proper fix may be to just group
    # all of these behind one async API that fetches this info. It just convenient
    # to try to mimic the API for now.
    @property
    def head_td(self) -> int:
        return self.dto_peer.head_td

    @property
    def head_hash(self) -> Hash32:
        return self.dto_peer.head_hash

    @property
    def header_number(self) -> BlockNumber:
        return self.dto_peer.head_number

    @property
    def max_headers_fetch(self) -> int:
        return self.dto_peer.max_headers_fetch

    @property
    def is_operational(self) -> bool:
        # We implement this API because parts of our code base works with actual and proxy peers
        # for the time being and expect this API to exist.
        # We return `True` here because it would be a waste to do this extra round trip, plus it
        # would only be a race condition at best.
        # When working with a proxy peer one *must* do the `is_operational` check in the request
        # handler that runs in the peer pool process.
        return True

    @classmethod
    def from_dto_peer(cls,
                      dto_peer: IdentifiablePeer,
                      event_bus: Endpoint,
                      broadcast_config: BroadcastConfig) -> 'ETHProxyPeer':
        return cls(
            dto_peer,
            ProxyETHProtocol(dto_peer, event_bus, broadcast_config),
            ProxyETHExchangeHandler(dto_peer, event_bus, broadcast_config),
        )


class ETHPeerFactory(BaseChainPeerFactory):
    peer_class = ETHPeer


class ETHPeerPoolEventBusRequestHandler(BasePeerPoolEventBusRequestHandler[ETHPeer]):
    """
    A request handler to handle ETH specific requests to the peer pool.
    """

    async def _run(self) -> None:
        self.logger.info("Running ETHPeerPoolEventBusRequestHandler")
        self.run_daemon_task(self.handle_send_blockheader_events())
        self.run_daemon_task(self.handle_send_block_bodies_events())
        self.run_daemon_task(self.handle_send_nodes_events())
        self.run_daemon_task(self.handle_send_receipts_events())
        self.run_daemon_task(self.handle_get_block_headers_requests())
        self.run_daemon_task(self.handle_get_block_bodies_requests())
        self.run_daemon_task(self.handle_get_node_data_requests())
        self.run_daemon_task(self.handle_get_receipts_requests())
        self.run_daemon_task(self.handle_get_highest_td_peer_requests())
        self.run_daemon_task(self.handle_connected_peers_requests())
        await super()._run()

    async def handle_send_blockheader_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendBlockHeadersEvent)):
            peer = self.maybe_return_peer(ev.dto_peer)
            if peer is None:
                continue
            peer.sub_proto.send_block_headers(ev.headers)

    async def handle_send_block_bodies_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendBlockBodiesEvent)):
            peer = self.maybe_return_peer(ev.dto_peer)
            if peer is None:
                continue
            peer.sub_proto.send_block_bodies(ev.blocks)

    async def handle_send_nodes_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendNodeDataEvent)):
            peer = self.maybe_return_peer(ev.dto_peer)
            if peer is None:
                continue
            peer.sub_proto.send_node_data(ev.nodes)

    async def handle_send_receipts_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendReceiptsEvent)):
            peer = self.maybe_return_peer(ev.dto_peer)
            if peer is None:
                continue
            peer.sub_proto.send_receipts(ev.receipts)

    async def handle_get_block_headers_requests(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(GetBlockHeadersRequest)):
            peer = self.maybe_return_peer(ev.dto_peer)

            if peer is None:
                continue

            try:
                headers = await peer.requests.get_block_headers(
                    ev.block_number_or_hash,
                    ev.max_headers,
                    ev.skip,
                    ev.reverse,
                    ev.timeout,
                )
            except TimeoutError:
                self.logger.debug("Timed out waiting on %s from %s", GetBlockHeadersRequest, peer)
            else:
                self._event_bus.broadcast(GetBlockHeadersResponse(headers), ev.broadcast_config())

    async def handle_get_block_bodies_requests(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(GetBlockBodiesRequest)):
            peer = self.maybe_return_peer(ev.dto_peer)

            if peer is None:
                continue

            try:
                bundles = await peer.requests.get_block_bodies(
                    ev.headers,
                    ev.timeout,
                )
            except TimeoutError:
                self.logger.debug("Timed out waiting on %s from %s", GetBlockBodiesRequest, peer)
            else:
                self._event_bus.broadcast(GetBlockBodiesResponse(bundles), ev.broadcast_config())

    async def handle_get_node_data_requests(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(GetNodeDataRequest)):
            peer = self.maybe_return_peer(ev.dto_peer)

            if peer is None:
                continue

            try:
                bundles = await peer.requests.get_node_data(ev.node_hashes, ev.timeout)
            except TimeoutError:
                self.logger.debug("Timed out waiting on %s from %s", GetBlockBodiesRequest, peer)
            else:
                self._event_bus.broadcast(GetNodeDataResponse(bundles), ev.broadcast_config())

    async def handle_get_receipts_requests(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(GetReceiptsRequest)):
            peer = self.maybe_return_peer(ev.dto_peer)

            if peer is None:
                continue

            try:
                bundles = await peer.requests.get_receipts(ev.headers, ev.timeout)
            except TimeoutError:
                self.logger.debug("Timed out waiting on %s from %s", GetReceiptsRequest, peer)
            else:
                self._event_bus.broadcast(GetReceiptsResponse(bundles), ev.broadcast_config())

    async def handle_get_highest_td_peer_requests(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(GetHighestTDPeerRequest)):

            try:
                highest_td_peer = self._peer_pool.highest_td_peer.to_dto()
            except NoConnectedPeers:
                # no peers are available right now
                highest_td_peer = None

            self._event_bus.broadcast(
                GetHighestTDPeerResponse(highest_td_peer),
                ev.broadcast_config()
            )

    async def handle_connected_peers_requests(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(GetConnectedPeersRequest)):

            peers = self._peer_pool.get_peers(ev.min_td)
            dto_peers = tuple(peer.to_dto() for peer in peers)
            self._event_bus.broadcast(
                GetConnectedPeersResponse(dto_peers),
                ev.broadcast_config()
            )


class ETHPeerPool(BaseChainPeerPool):
    peer_factory_class = ETHPeerFactory


# TODO: Make a base class that handles peer count etc
class ETHProxyPeerPool:

    def __init__(self, event_bus: TrinityEventBusEndpoint, broadcast_config: BroadcastConfig):
        self.event_bus = event_bus
        self.broadcast_config = broadcast_config

    # TODO: Broadcast config, timeout
    async def get_hightest_td_peer(self) -> ETHProxyPeer:
        response = await self.event_bus.request(GetHighestTDPeerRequest(2))
        return ETHProxyPeer.from_dto_peer(response.dto_peer, self.event_bus, self.broadcast_config)

    async def get_connected_peers(self, min_td: int = 0) -> ETHProxyPeer:
        response = await self.event_bus.request(GetConnectedPeersRequest())
        proxy_peers = tuple(ETHProxyPeer.from_dto_peer(peer, self.event_bus, self.broadcast_config) for peer in response.dto_peers)
        return proxy_peers
