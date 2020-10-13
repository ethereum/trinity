import operator
from typing import (
    Any,
    Tuple,
    Type,
)

from lahja import EndpointAPI

from eth_typing import (
    BlockNumber,
    Hash32,
)

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
from trinity.protocol.fh.api import FirehoseAPI
from trinity.protocol.fh.commands import StatusPayload as FirehoseStatusPayload
from trinity.protocol.fh.handshaker import FirehoseHandshaker
from trinity.protocol.fh.proto import FirehoseProtocol

from . import forkid

from .api import AnyETHAPI, ETHV63API, ETHV65API, ETHV64API
from .commands import (
    GetBlockHeadersV65,
    GetBlockBodiesV65,
    GetReceiptsV65,
    GetNodeDataV65,
    NewBlock,
    NewBlockHashes,
    Transactions,
    NewPooledTransactionHashes,
    GetPooledTransactionsV65,
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
from .proto import ETHProtocolV63, ETHProtocolV64, ETHProtocolV65, BaseETHProtocol
from .proxy import ProxyETHAPI
from .handshaker import ETHV63Handshaker, ETHHandshaker


class ETHPeer(BaseChainPeer):
    max_headers_fetch = MAX_HEADERS_FETCH

    supported_sub_protocols: Tuple[Type[BaseETHProtocol], ...] = (
        ETHProtocolV63,
        ETHProtocolV64,
        ETHProtocolV65,
    )
    sub_proto: BaseETHProtocol = None
    eth_api: AnyETHAPI
    fh_api: FirehoseAPI

    def get_behaviors(self) -> Tuple[BehaviorAPI, ...]:
        return super().get_behaviors() + (
            ETHV63API().as_behavior(),
            ETHV64API().as_behavior(),
            ETHV65API().as_behavior(),
            FirehoseAPI().as_behavior(),
        )

    def _pre_run(self) -> None:
        super()._pre_run()

        # XXX: Not sure if it's better to have self.fh_api raise AttributeError or be None when
        # there's no firehose support
        if self.connection.has_protocol(FirehoseProtocol):
            self.fh_api = self.connection.get_logic(FirehoseAPI.name, FirehoseAPI)

        if self.connection.has_protocol(ETHProtocolV63):
            self.eth_api = self.connection.get_logic(ETHV63API.name, ETHV63API)
        elif self.connection.has_protocol(ETHProtocolV64):
            self.eth_api = self.connection.get_logic(ETHV64API.name, ETHV64API)
        elif self.connection.has_protocol(ETHProtocolV65):
            self.eth_api = self.connection.get_logic(ETHV65API.name, ETHV65API)
        else:
            raise Exception("Unreachable code")

    def get_extra_stats(self) -> Tuple[str, ...]:
        """
        Return extra stats for this peer.

        Raises PeerConnectionLost if the peer is no longer alive.
        """
        basic_stats = super().get_extra_stats()
        eth_stats = self.eth_api.get_extra_stats()

        if self.connection.has_logic(FirehoseAPI.name):
            fh_stats = self.fh_api.get_extra_stats()
        else:
            fh_stats = ()

        return basic_stats + eth_stats + fh_stats


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
        genesis_hash = await headerdb.coro_get_canonical_block_hash(
            BlockNumber(GENESIS_BLOCK_NUMBER))
        head = await headerdb.coro_get_canonical_head()

        fork_blocks = forkid.extract_fork_blocks(self.context.vm_configuration)
        our_forkid = forkid.make_forkid(genesis_hash, head.block_number, fork_blocks)

        eth_handshakers = await self._get_eth_handshakers(
            genesis_hash, head, our_forkid, fork_blocks)
        firehose_handshakers = self._get_firehose_handshakers(
            genesis_hash, head, our_forkid, fork_blocks)

        return eth_handshakers + firehose_handshakers

    async def _get_eth_handshakers(
            self,
            genesis_hash: Hash32,
            head: BlockHeaderAPI,
            our_forkid: forkid.ForkID,
            fork_blocks: Tuple[BlockNumber, ...]) -> Tuple[HandshakerAPI[Any], ...]:
        total_difficulty = await self.context.headerdb.coro_get_score(head.hash)

        handshake_v63_params = StatusV63Payload(
            head_hash=head.hash,
            total_difficulty=total_difficulty,
            genesis_hash=genesis_hash,
            network_id=self.context.network_id,
            version=ETHProtocolV63.version,
        )

        handshake_params = StatusPayload(
            head_hash=head.hash,
            total_difficulty=total_difficulty,
            genesis_hash=genesis_hash,
            network_id=self.context.network_id,
            version=ETHProtocolV65.version,
            fork_id=our_forkid
        )

        highest_eth_protocol = max(
            self.peer_class.supported_sub_protocols, key=operator.attrgetter('version')
        )

        # ETHV63Handshaker is only used for V63 as it uses a different Status command.
        # ETHHandshaker is used for all other protocol versions above V63 that we currently
        # support. The `highest_eth_protocol` is what the handshake will report to the other peer
        # as our highest supported version. We need to pass this as a parameter to ensure the
        # handshake reports the version that is configured on the ETHPeer class.
        return (
            ETHV63Handshaker(handshake_v63_params),
            ETHHandshaker(handshake_params, head.block_number, fork_blocks, highest_eth_protocol)
        )

    def _get_firehose_handshakers(
            self,
            genesis_hash: Hash32,
            head: BlockHeaderAPI,
            our_forkid: forkid.ForkID,
            fork_blocks: Tuple[BlockNumber, ...]) -> Tuple[FirehoseHandshaker, ...]:
        handshake_params = FirehoseStatusPayload(
            version=FirehoseProtocol.version,
            network_id=self.context.network_id,
            fork_id=our_forkid,
        )
        return (FirehoseHandshaker(handshake_params, genesis_hash, head.block_number, fork_blocks),)


class ETHPeerPoolEventServer(PeerPoolEventServer[ETHPeer]):
    """
    ETH protocol specific ``PeerPoolEventServer``. See ``PeerPoolEventServer`` for more info.
    """

    subscription_msg_types = frozenset({
        GetBlockHeadersV65,
        GetBlockBodiesV65,
        GetReceiptsV65,
        GetNodeDataV65,
        Transactions,
        NewBlockHashes,
        NewBlock,
        NewPooledTransactionHashes,
        GetPooledTransactionsV65,
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
        if isinstance(cmd, GetBlockHeadersV65):
            await self.event_bus.broadcast(
                GetBlockHeadersEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, GetBlockBodiesV65):
            await self.event_bus.broadcast(
                GetBlockBodiesEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, GetReceiptsV65):
            await self.event_bus.broadcast(
                GetReceiptsEvent(session, cmd),
                FIRE_AND_FORGET_BROADCASTING,
            )
        elif isinstance(cmd, GetNodeDataV65):
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
        elif isinstance(cmd, GetPooledTransactionsV65):
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
