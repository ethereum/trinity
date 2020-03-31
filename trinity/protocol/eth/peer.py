from typing import (
    Any,
    Tuple,
)

from cached_property import cached_property

from lahja import EndpointAPI

from eth_typing import BlockNumber

from eth.abc import BlockHeaderAPI
from eth.constants import GENESIS_BLOCK_NUMBER
from lahja import (
    BroadcastConfig,
)

from p2p.abc import BehaviorAPI, CommandAPI, HandshakerAPI, SessionAPI

from trinity.protocol.common.peer import (
    BaseChainPeer,
    BaseChainPeerFactory,
    BaseChainPeerPool,
)
from trinity.protocol.common.peer_pool_event_bus import (
    BaseProxyPeer,
    BaseProxyPeerPool,
    PeerPoolEventServer,
    FIRE_AND_FORGET_BROADCASTING,
)
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    ReceiptsBundles,
    NodeDataBundles,
)
from . import forkid

from .api import ETHV63API, ETHAPI, AnyETHAPI, ETHV64API
from .commands import (
    GetBlockHeaders,
    GetBlockBodies,
    GetReceipts,
    GetNodeData,
    NewBlock,
    NewBlockHashes,
    Transactions,
    NewPooledTransactionHashes,
    GetPooledTransactions,
)
from .constants import MAX_HEADERS_FETCH
from .events import (
    SendBlockHeadersEvent,
    SendBlockBodiesEvent,
    SendNodeDataEvent,
    SendReceiptsEvent,
    GetBlockHeadersRequest,
    GetReceiptsRequest,
    GetBlockBodiesRequest,
    GetNodeDataRequest,
    GetBlockHeadersEvent,
    GetBlockBodiesEvent,
    GetReceiptsEvent,
    GetNodeDataEvent,
    NewBlockEvent,
    NewBlockHashesEvent,
    TransactionsEvent,
    NewPooledTransactionHashesEvent,
    GetPooledTransactionsEvent,
    GetPooledTransactionsRequest,
    SendPooledTransactionsEvent,
    SendTransactionsEvent,
)
from .payloads import StatusV63Payload, StatusPayload
from .proto import ETHProtocolV63, ETHProtocol, ETHProtocolV64
from .proxy import ProxyETHAPI
from .handshaker import ETHV63Handshaker, ETHHandshaker


class ETHPeer(BaseChainPeer):
    max_headers_fetch = MAX_HEADERS_FETCH

    supported_sub_protocols = (ETHProtocolV63, ETHProtocol)
    sub_proto: ETHProtocol = None

    def get_behaviors(self) -> Tuple[BehaviorAPI, ...]:
        return super().get_behaviors() + (ETHV63API().as_behavior(), ETHAPI().as_behavior())

    @cached_property
    def eth_api(self) -> AnyETHAPI:
        if self.connection.has_protocol(ETHProtocolV63):
            return self.connection.get_logic(ETHV63API.name, ETHV63API)
        if self.connection.has_protocol(ETHProtocolV64):
            return self.connection.get_logic(ETHV64API.name, ETHV64API)
        elif self.connection.has_protocol(ETHProtocol):
            return self.connection.get_logic(ETHAPI.name, ETHAPI)
        else:
            raise Exception("Unreachable code")

    def get_extra_stats(self) -> Tuple[str, ...]:
        basic_stats = super().get_extra_stats()
        eth_stats = self.eth_api.get_extra_stats()
        return basic_stats + eth_stats


class ETHProxyPeer(BaseProxyPeer):
    """
    A ``ETHPeer`` that can be used from any process instead of the actual peer pool peer.
    Any action performed on the ``ETHProxyPeer`` is delegated to the actual peer in the pool.
    This does not yet mimic all APIs of the real peer.
    """

    def __init__(self,
                 session: SessionAPI,
                 event_bus: EndpointAPI,
                 eth_api: ProxyETHAPI) -> None:
        super().__init__(session, event_bus)

        self.eth_api = eth_api

    @classmethod
    def from_session(cls,
                     session: SessionAPI,
                     event_bus: EndpointAPI,
                     broadcast_config: BroadcastConfig) -> 'ETHProxyPeer':
        return cls(
            session,
            event_bus,
            ProxyETHAPI(session, event_bus, broadcast_config)
        )


class ETHPeerFactory(BaseChainPeerFactory):
    peer_class = ETHPeer

    async def get_handshakers(self) -> Tuple[HandshakerAPI[Any], ...]:
        headerdb = self.context.headerdb
        wait = self.cancel_token.cancellable_wait

        head = await wait(headerdb.coro_get_canonical_head())
        total_difficulty = await wait(headerdb.coro_get_score(head.hash))
        genesis_hash = await wait(
            headerdb.coro_get_canonical_block_hash(BlockNumber(GENESIS_BLOCK_NUMBER))
        )

        handshake_v63_params = StatusV63Payload(
            head_hash=head.hash,
            total_difficulty=total_difficulty,
            genesis_hash=genesis_hash,
            network_id=self.context.network_id,
            version=ETHProtocolV63.version,
        )

        fork_blocks = forkid.extract_fork_blocks(self.context.vm_configuration)
        our_forkid = forkid.make_forkid(genesis_hash, head.block_number, fork_blocks)

        handshake_params = StatusPayload(
            head_hash=head.hash,
            total_difficulty=total_difficulty,
            genesis_hash=genesis_hash,
            network_id=self.context.network_id,
            version=ETHProtocol.version,
            fork_id=our_forkid
        )

        return (
            ETHV63Handshaker(handshake_v63_params),
            ETHHandshaker(handshake_params, head.block_number, fork_blocks),
        )


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
        NewPooledTransactionHashes,
        GetPooledTransactions,
    })

    # SendX events that need to be forwarded to peer.sub_proto.send(event.command)
    send_event_types = frozenset({
        SendBlockHeadersEvent,
        SendBlockBodiesEvent,
        SendNodeDataEvent,
        SendReceiptsEvent,
        SendPooledTransactionsEvent,
        SendTransactionsEvent,
    })

    async def run(self) -> None:

        for event_type in self.send_event_types:
            self.run_daemon_event(event_type, self.handle_send_command)

        self.run_daemon_request(GetBlockHeadersRequest, self.handle_get_block_headers_request)
        self.run_daemon_request(GetReceiptsRequest, self.handle_get_receipts_request)
        self.run_daemon_request(GetBlockBodiesRequest, self.handle_get_block_bodies_request)
        self.run_daemon_request(GetNodeDataRequest, self.handle_get_node_data_request)
        self.run_daemon_request(
            GetPooledTransactionsRequest,
            self.handle_get_pooled_transactions_request
        )

        await super().run()

    async def handle_get_block_headers_request(
            self,
            event: GetBlockHeadersRequest) -> Tuple[BlockHeaderAPI, ...]:
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

    async def handle_get_pooled_transactions_request(
            self,
            event: GetPooledTransactionsRequest) -> NodeDataBundles:

        return await self.with_node_and_timeout(
            event.session,
            event.timeout,
            lambda peer: peer.eth_api.get_pooled_transactions(event.transaction_hashes)
        )

    async def handle_native_peer_message(self,
                                         session: SessionAPI,
                                         cmd: CommandAPI[Any]) -> None:

        # These are broadcasted without a specific target. We shouldn't worry if they are consumed
        # or not (e.g. transaction pool is enabled or disabled etc)
        if isinstance(cmd, GetBlockHeaders):
            await self.event_bus.broadcast(
                GetBlockHeadersEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, GetBlockBodies):
            await self.event_bus.broadcast(
                GetBlockBodiesEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, GetReceipts):
            await self.event_bus.broadcast(
                GetReceiptsEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, GetNodeData):
            await self.event_bus.broadcast(
                GetNodeDataEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, NewBlock):
            await self.event_bus.broadcast(
                NewBlockEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, NewBlockHashes):
            await self.event_bus.broadcast(
                NewBlockHashesEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, Transactions):
            await self.event_bus.broadcast(
                TransactionsEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, NewPooledTransactionHashes):
            await self.event_bus.broadcast(
                NewPooledTransactionHashesEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING
            )
        elif isinstance(cmd, GetPooledTransactions):
            await self.event_bus.broadcast(
                GetPooledTransactionsEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING
            )
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
