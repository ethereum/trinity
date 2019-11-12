import asyncio
from typing import (
    Tuple,
)

from cached_property import cached_property

from lahja import EndpointAPI

from eth_typing import BlockNumber

from eth.constants import GENESIS_BLOCK_NUMBER
from eth.rlp.headers import BlockHeader
from lahja import (
    BroadcastConfig,
)

from p2p.abc import BehaviorAPI, CommandAPI, HandshakerAPI, SessionAPI
from p2p.exceptions import PeerConnectionLost
from p2p.protocol import (
    Payload,
)

from trinity._utils.decorators import (
    async_suppress_exceptions,
)
from trinity.protocol.common.peer import (
    BaseChainPeer,
    BaseChainPeerFactory,
    BaseChainPeerPool,
)
from trinity.protocol.common.peer_pool_event_bus import (
    BaseProxyPeer,
    BaseProxyPeerPool,
    PeerPoolEventServer,
)
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)

from .api import ETHAPI
from .commands import (
    GetBlockHeaders,
    GetBlockBodies,
    GetReceipts,
    GetNodeData,
    NewBlock,
    NewBlockHashes,
    Transactions,
)
from .constants import MAX_HEADERS_FETCH
from .events import (
    GetBlockHeadersEvent,
    GetBlockHeadersRequest,
    GetBlockBodiesEvent,
    GetBlockBodiesRequest,
    GetReceiptsEvent,
    GetNodeDataEvent,
    GetNodeDataRequest,
    GetReceiptsRequest,
    NewBlockEvent,
    NewBlockHashesEvent,
    SendBlockBodiesEvent,
    SendBlockHeadersEvent,
    SendNodeDataEvent,
    SendReceiptsEvent,
    TransactionsEvent,
)
from .proto import ETHProtocol, ProxyETHProtocol, ETHHandshakeParams
from .proxy import ProxyETHAPI
from .handshaker import ETHHandshaker


class ETHPeer(BaseChainPeer):
    max_headers_fetch = MAX_HEADERS_FETCH

    supported_sub_protocols = (ETHProtocol,)
    sub_proto: ETHProtocol = None

    def get_behaviors(self) -> Tuple[BehaviorAPI, ...]:
        return super().get_behaviors() + (ETHAPI().as_behavior(),)

    @cached_property
    def eth_api(self) -> ETHAPI:
        return self.connection.get_logic(ETHAPI.name, ETHAPI)


class ETHProxyPeer(BaseProxyPeer):
    """
    A ``ETHPeer`` that can be used from any process instead of the actual peer pool peer.
    Any action performed on the ``BCCProxyPeer`` is delegated to the actual peer in the pool.
    This does not yet mimic all APIs of the real peer.
    """

    def __init__(self,
                 session: SessionAPI,
                 event_bus: EndpointAPI,
                 sub_proto: ProxyETHProtocol,
                 eth_api: ProxyETHAPI):

        super().__init__(session, event_bus)

        self.sub_proto = sub_proto
        self.eth_api = eth_api

    @classmethod
    def from_session(cls,
                     session: SessionAPI,
                     event_bus: EndpointAPI,
                     broadcast_config: BroadcastConfig) -> 'ETHProxyPeer':
        return cls(
            session,
            event_bus,
            ProxyETHProtocol(session, event_bus, broadcast_config),
            ProxyETHAPI(session, event_bus, broadcast_config)
        )


class ETHPeerFactory(BaseChainPeerFactory):
    peer_class = ETHPeer

    async def get_handshakers(self) -> Tuple[HandshakerAPI, ...]:
        headerdb = self.context.headerdb

        head = await headerdb.coro_get_canonical_head()
        total_difficulty = await headerdb.coro_get_score(head.hash)
        genesis_hash = await headerdb.coro_get_canonical_block_hash(
            BlockNumber(GENESIS_BLOCK_NUMBER),
        )

        handshake_params = ETHHandshakeParams(
            head_hash=head.hash,
            total_difficulty=total_difficulty,
            genesis_hash=genesis_hash,
            network_id=self.context.network_id,
            version=ETHProtocol.version,
        )
        return (
            ETHHandshaker(handshake_params),
        )


async_fire_and_forget = async_suppress_exceptions(PeerConnectionLost, asyncio.TimeoutError)  # type: ignore  # noqa: E501


class ETHPeerPoolEventServer(PeerPoolEventServer[ETHPeer]):
    """
    ETH protocol specific ``PeerPoolEventServer``. See ``PeerPoolEventServer`` for more info.
    """

    subscription_msg_types = frozenset({
        GetBlockHeaders,
        GetBlockBodies,
        GetReceipts,
        GetNodeData,
        Transactions,
        NewBlockHashes,
        NewBlock,
    })

    async def run(self) -> None:

        self.run_daemon_event(SendBlockHeadersEvent, self.handle_block_headers_event)
        self.run_daemon_event(SendBlockBodiesEvent, self.handle_block_bodies_event)
        self.run_daemon_event(SendNodeDataEvent, self.handle_node_data_event)
        self.run_daemon_event(SendReceiptsEvent, self.handle_receipts_event)

        self.run_daemon_request(GetBlockHeadersRequest, self.handle_get_block_headers_request)
        self.run_daemon_request(GetReceiptsRequest, self.handle_get_receipts_request)
        self.run_daemon_request(GetBlockBodiesRequest, self.handle_get_block_bodies_request)
        self.run_daemon_request(GetNodeDataRequest, self.handle_get_node_data_request)

        await super().run()

    @async_fire_and_forget
    async def handle_block_headers_event(self, event: SendBlockHeadersEvent) -> None:
        await self.try_with_session(
            event.session,
            lambda peer: peer.sub_proto.send_block_headers(event.headers)
        )

    @async_fire_and_forget
    async def handle_block_bodies_event(self, event: SendBlockBodiesEvent) -> None:
        await self.try_with_session(
            event.session,
            lambda peer: peer.sub_proto.send_block_bodies(event.blocks)
        )

    @async_fire_and_forget
    async def handle_node_data_event(self, event: SendNodeDataEvent) -> None:
        await self.try_with_session(
            event.session,
            lambda peer: peer.sub_proto.send_node_data(event.nodes)
        )

    @async_fire_and_forget
    async def handle_receipts_event(self, event: SendReceiptsEvent) -> None:
        await self.try_with_session(
            event.session,
            lambda peer: peer.sub_proto.send_receipts(event.receipts)
        )

    async def handle_get_block_headers_request(
            self,
            event: GetBlockHeadersRequest) -> Tuple[BlockHeader, ...]:
        peer = self.get_peer(event.session)
        return await peer.eth_api.get_block_headers(
            event.block_number_or_hash,
            event.max_headers,
            skip=event.skip,
            reverse=event.reverse,
            timeout=event.timeout
        )

    async def handle_get_receipts_request(self,
                                          event: GetReceiptsRequest) -> ReceiptsBundles:

        return await self.with_node_and_timeout(
            event.session,
            event.timeout,
            lambda peer: peer.eth_api.get_receipts(event.headers)
        )

    async def handle_get_block_bodies_request(self,
                                              event: GetBlockBodiesRequest) -> BlockBodyBundles:
        return await self.with_node_and_timeout(
            event.session,
            event.timeout,
            lambda peer: peer.eth_api.get_block_bodies(event.headers)
        )

    async def handle_get_node_data_request(self,
                                           event: GetNodeDataRequest) -> NodeDataBundles:
        return await self.with_node_and_timeout(
            event.session,
            event.timeout,
            lambda peer: peer.eth_api.get_node_data(event.node_hashes)
        )

    async def handle_native_peer_message(self,
                                         session: SessionAPI,
                                         cmd: CommandAPI,
                                         msg: Payload) -> None:

        if isinstance(cmd, GetBlockHeaders):
            await self.event_bus.broadcast(GetBlockHeadersEvent(session, cmd, msg))
        elif isinstance(cmd, GetBlockBodies):
            await self.event_bus.broadcast(GetBlockBodiesEvent(session, cmd, msg))
        elif isinstance(cmd, GetReceipts):
            await self.event_bus.broadcast(GetReceiptsEvent(session, cmd, msg))
        elif isinstance(cmd, GetNodeData):
            await self.event_bus.broadcast(GetNodeDataEvent(session, cmd, msg))
        elif isinstance(cmd, NewBlock):
            await self.event_bus.broadcast(NewBlockEvent(session, cmd, msg))
        elif isinstance(cmd, NewBlockHashes):
            await self.event_bus.broadcast(NewBlockHashesEvent(session, cmd, msg))
        elif isinstance(cmd, Transactions):
            await self.event_bus.broadcast(TransactionsEvent(session, cmd, msg))
        else:
            raise Exception(f"Command {cmd} is not broadcasted")


class ETHPeerPool(BaseChainPeerPool):
    peer_factory_class = ETHPeerFactory


class ETHProxyPeerPool(BaseProxyPeerPool[ETHProxyPeer]):

    def convert_session_to_proxy_peer(self,
                                      session: SessionAPI,
                                      event_bus: EndpointAPI,
                                      broadcast_config: BroadcastConfig) -> ETHProxyPeer:
        return ETHProxyPeer.from_session(
            session,
            self.event_bus,
            self.broadcast_config
        )
