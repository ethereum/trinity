from abc import abstractmethod
import operator
import random
from typing import (
    Dict,
    List,
    Iterable,
    NamedTuple,
    Tuple,
    Type,
)

from lahja import EndpointAPI

from cancel_token import CancelToken

from eth_typing import (
    BlockNumber,
    Hash32,
)

from eth_utils.toolz import groupby
from eth_utils import (
    to_dict,
)

from eth.constants import GENESIS_BLOCK_NUMBER
from eth.vm.base import BaseVM

from p2p.abc import NodeAPI, CommandAPI
from p2p.disconnect import DisconnectReason
from p2p.exceptions import NoConnectedPeers
from p2p.peer import (
    BasePeer,
    BasePeerFactory,
)
from p2p.peer_backend import (
    BasePeerBackend,
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.service import BaseService
from p2p.tracking.connection import (
    BaseConnectionTracker,
    NoopConnectionTracker,
)

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.common.handlers import BaseChainExchangeHandler

from trinity.plugins.builtin.network_db.connection.tracker import ConnectionTrackerClient
from trinity.plugins.builtin.network_db.eth1_peer_db.tracker import (
    BaseEth1PeerTracker,
    EventBusEth1PeerTracker,
    NoopEth1PeerTracker,
)
from trinity._utils.errors import (
    pass_or_raise,
)

from .boot import DAOCheckBootManager
from .context import ChainContext
from .events import (
    ChainPeerMetaData,
    DisconnectPeerEvent,
    GetPeerMetaDataRequest,
    GetPeerPerfMetricsRequest,
)


class ChainInfo(NamedTuple):
    block_number: BlockNumber
    block_hash: Hash32
    total_difficulty: int
    genesis_hash: Hash32

    network_id: int


class BaseChainPeer(BasePeer):
    boot_manager_class = DAOCheckBootManager
    context: ChainContext

    head_td: int = None
    head_hash: Hash32 = None
    head_number: BlockNumber = None
    network_id: int = None
    genesis_hash: Hash32 = None

    @property
    @abstractmethod
    def requests(self) -> BaseChainExchangeHandler:
        ...

    @property
    @abstractmethod
    def max_headers_fetch(self) -> int:
        ...

    @property
    def headerdb(self) -> BaseAsyncHeaderDB:
        return self.context.headerdb

    @property
    def local_network_id(self) -> int:
        return self.context.network_id

    @property
    def vm_configuration(self) -> Tuple[Tuple[int, Type[BaseVM]], ...]:
        return self.context.vm_configuration

    _local_genesis_hash: Hash32 = None

    async def _get_local_genesis_hash(self) -> Hash32:
        if self._local_genesis_hash is None:
            self._local_genesis_hash = await self.wait(
                self.headerdb.coro_get_canonical_block_hash(BlockNumber(GENESIS_BLOCK_NUMBER))
            )
        return self._local_genesis_hash

    @property
    async def _local_chain_info(self) -> ChainInfo:
        head = await self.wait(self.headerdb.coro_get_canonical_head())
        total_difficulty = await self.wait(self.headerdb.coro_get_score(head.hash))
        genesis_hash = await self._get_local_genesis_hash()
        return ChainInfo(
            block_number=head.block_number,
            block_hash=head.hash,
            total_difficulty=total_difficulty,
            genesis_hash=genesis_hash,
            network_id=self.local_network_id,
        )

    def setup_connection_tracker(self) -> BaseConnectionTracker:
        if self.has_event_bus:
            return ConnectionTrackerClient(self.get_event_bus())
        else:
            self.logger.warning(
                "No event_bus set on peer.  Connection tracking falling back to "
                "`NoopConnectionTracker`."
            )
            return NoopConnectionTracker()

    @to_dict
    def collect_performance_metrics(self) -> Iterable[Tuple[Type[CommandAPI], float]]:
        for exchange in self.requests:
            yield exchange.response_cmd_type, exchange.tracker.items_per_second_ema.value


class BaseProxyPeer(BaseService):
    """
    Base class for peers that can be used from any process where the actual peer is not available.
    """

    def __init__(self,
                 remote: NodeAPI,
                 event_bus: EndpointAPI,
                 token: CancelToken = None):

        self.event_bus = event_bus
        self.remote = remote
        super().__init__(token)

    def __str__(self) -> str:
        return f"{self.__class__.__name__} {self.remote}"

    async def _run(self) -> None:
        self.logger.debug("Starting Proxy Peer %s", self)
        await self.cancellation()

    async def disconnect(self, reason: DisconnectReason) -> None:
        self.logger.debug("Forwarding `disconnect()` call from proxy to actual peer", self)
        await self.event_bus.broadcast(
            DisconnectPeerEvent(self.remote, reason),
            TO_NETWORKING_BROADCAST_CONFIG,
        )
        await self.cancel()


class BaseChainProxyPeer(BaseProxyPeer):

    def __init__(self,
                 remote: NodeAPI,
                 event_bus: EndpointAPI,
                 token: CancelToken = None):
        super().__init__(remote, event_bus, token)
        self._meta_data: ChainPeerMetaData = None
        self._perf_metrics: Dict[Type[CommandAPI], float] = None

    @property
    def perf_metrics(self) -> Dict[Type[CommandAPI], float]:
        """
        Return the latest available performance metrics from cache.
        """

        if self._perf_metrics is None:
            raise Exception("Run get_perf_metrics first")

        return self._perf_metrics

    async def get_meta_data(self, use_cache: bool=True) -> ChainPeerMetaData:

        if self._meta_data is None or not use_cache:
            await self.event_bus.wait_until_any_endpoint_subscribed_to(GetPeerMetaDataRequest)
            response = await self.wait(
                self.event_bus.request(
                    GetPeerMetaDataRequest(self.remote),
                    TO_NETWORKING_BROADCAST_CONFIG
                ),
            )

            pass_or_raise(response)

            self._meta_data = response.meta_data

        return self._meta_data

    async def get_perf_metrics(self) -> Dict[Type[CommandAPI], float]:
        await self.event_bus.wait_until_any_endpoint_subscribed_to(GetPeerPerfMetricsRequest)
        response = await self.wait(
            self.event_bus.request(
                GetPeerPerfMetricsRequest(self.remote),
                TO_NETWORKING_BROADCAST_CONFIG
            )
        )

        pass_or_raise(response)

        self._perf_metrics = response.metrics

        return response.metrics


class BaseChainPeerFactory(BasePeerFactory):
    context: ChainContext
    peer_class: Type[BaseChainPeer]


class BaseChainPeerPool(BasePeerPool):
    connected_nodes: Dict[NodeAPI, BaseChainPeer]  # type: ignore
    peer_factory_class: Type[BaseChainPeerFactory]
    peer_tracker: BaseEth1PeerTracker

    @property
    def highest_td_peer(self) -> BaseChainPeer:
        peers = tuple(self.connected_nodes.values())
        if not peers:
            raise NoConnectedPeers("No connected peers")
        peers_by_td = groupby(operator.attrgetter('head_td'), peers)
        max_td = max(peers_by_td.keys())
        return random.choice(peers_by_td[max_td])

    def get_peers(self, min_td: int) -> List[BaseChainPeer]:
        # TODO: Consider turning this into a method that returns an AsyncIterator, to make it
        # harder for callsites to get a list of peers while making blocking calls, as those peers
        # might disconnect in the meantime.
        peers = tuple(self.connected_nodes.values())
        return [peer for peer in peers if peer.head_td >= min_td]

    def setup_connection_tracker(self) -> BaseConnectionTracker:
        if self.has_event_bus:
            return ConnectionTrackerClient(self.get_event_bus())
        else:
            return NoopConnectionTracker()

    def setup_peer_backends(self) -> Tuple[BasePeerBackend, ...]:
        if self.has_event_bus:
            self.peer_tracker = EventBusEth1PeerTracker(self.get_event_bus())
        else:
            self.peer_tracker = NoopEth1PeerTracker()

        self.subscribe(self.peer_tracker)
        return super().setup_peer_backends() + (self.peer_tracker,)
