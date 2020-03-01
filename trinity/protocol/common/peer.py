from abc import abstractmethod
import asyncio
import functools
import operator
import random
from typing import (
    Container,
    Dict,
    List,
    Tuple,
    Type,
    Union,
)

from cached_property import cached_property

from lahja import EndpointAPI

from cancel_token import CancelToken, OperationCancelled

from eth_utils.logging import get_extended_debug_logger
from eth_utils.toolz import (
    excepts,
    groupby,
)

from eth_typing import BlockNumber, Hash32

from eth.abc import VirtualMachineAPI
from eth.constants import GENESIS_BLOCK_NUMBER
from eth.rlp.headers import BlockHeader

from p2p.abc import BehaviorAPI, NodeAPI, SessionAPI
from p2p.constants import (
    MAX_SEQUENTIAL_PEER_CONNECT,
    PEER_CONNECT_INTERVAL,
)
from p2p.disconnect import DisconnectReason
from p2p.discv5.typing import NodeID
from p2p.exceptions import (
    MalformedMessage,
    NoConnectedPeers,
    PeerConnectionLost,
    UnknownAPI,
)
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
from p2p.token_bucket import TokenBucket
from p2p.tracking.connection import (
    BaseConnectionTracker,
    NoopConnectionTracker,
)

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.exceptions import BaseForkIDValidationError, ENRMissingForkID
from trinity.protocol.common.abc import ChainInfoAPI, HeadInfoAPI
from trinity.protocol.common.api import ChainInfo, HeadInfo, choose_eth_or_les_api
from trinity.protocol.eth.api import ETHV63API, ETHAPI
from trinity.protocol.eth.forkid import (
    extract_fork_blocks,
    extract_forkid,
    validate_forkid,
)
from trinity.protocol.les.api import LESV1API, LESV2API

from trinity.components.builtin.network_db.connection.tracker import ConnectionTrackerClient
from trinity.components.builtin.network_db.eth1_peer_db.tracker import (
    BaseEth1PeerTracker,
    EventBusEth1PeerTracker,
    NoopEth1PeerTracker,
)

from .boot import DAOCheckBootManager
from .context import ChainContext
from .events import (
    DisconnectPeerEvent,
)


p2p_logger = get_extended_debug_logger('p2p')


class BaseChainPeer(BasePeer):
    boot_manager_class = DAOCheckBootManager
    context: ChainContext

    @cached_property
    def chain_api(self) -> Union[ETHAPI, ETHV63API, LESV1API, LESV2API]:
        return choose_eth_or_les_api(self.connection)

    @cached_property
    def head_info(self) -> HeadInfoAPI:
        return self.connection.get_logic(HeadInfo.name, HeadInfo)

    @cached_property
    def chain_info(self) -> ChainInfoAPI:
        return self.connection.get_logic(ChainInfo.name, ChainInfo)

    def get_behaviors(self) -> Tuple[BehaviorAPI, ...]:
        return (
            HeadInfo().as_behavior(),
            ChainInfo().as_behavior(),
        )

    @property
    @abstractmethod
    def max_headers_fetch(self) -> int:
        ...

    def setup_connection_tracker(self) -> BaseConnectionTracker:
        if self.has_event_bus:
            return ConnectionTrackerClient(self.get_event_bus())
        else:
            self.logger.warning(
                "No event_bus set on peer.  Connection tracking falling back to "
                "`NoopConnectionTracker`."
            )
            return NoopConnectionTracker()


class BaseProxyPeer(BaseService):
    """
    Base class for peers that can be used from any process where the actual peer is not available.
    """

    def __init__(self,
                 session: SessionAPI,
                 event_bus: EndpointAPI,
                 token: CancelToken = None):

        self.event_bus = event_bus
        self.session = session
        super().__init__(token)

    def __str__(self) -> str:
        return f"{self.__class__.__name__} {self.session}"

    async def _run(self) -> None:
        self.logger.debug("Starting Proxy Peer %s", self)
        await self.cancellation()

    async def disconnect(self, reason: DisconnectReason) -> None:
        self.logger.debug("Forwarding `disconnect()` call from proxy to actual peer: %s", self)
        await self.event_bus.broadcast(
            DisconnectPeerEvent(self.session, reason),
            TO_NETWORKING_BROADCAST_CONFIG,
        )
        await self.cancel()


class BaseChainPeerFactory(BasePeerFactory):
    context: ChainContext
    peer_class: Type[BaseChainPeer]


class BaseChainPeerPool(BasePeerPool):
    context: ChainContext
    connected_nodes: Dict[NodeAPI, BaseChainPeer]  # type: ignore
    peer_factory_class: Type[BaseChainPeerFactory]
    peer_tracker: BaseEth1PeerTracker

    async def maybe_connect_more_peers(self) -> None:
        rate_limiter = TokenBucket(
            rate=1 / PEER_CONNECT_INTERVAL,
            capacity=MAX_SEQUENTIAL_PEER_CONNECT,
        )

        # We set this to 0 so that upon startup (when our RoutingTable will have only a few
        # entries) we use the less restrictive filter function and get as many connection
        # candidates as possible.
        last_candidates_count = 0
        while self.is_operational:
            if self.is_full:
                await self.sleep(PEER_CONNECT_INTERVAL)
                continue

            await self.wait(rate_limiter.take())

            if last_candidates_count >= self.available_slots:
                head = await self.get_chain_head()
                genesis_hash = await self.get_genesis_hash()
                fork_blocks = extract_fork_blocks(self.vm_configuration)
                should_skip = functools.partial(
                    skip_candidate_if_on_list_or_fork_mismatch,
                    genesis_hash,
                    head.block_number,
                    fork_blocks,
                )
            else:
                self.logger.debug(
                    "Didn't get enough candidates last time, falling back to skipping "
                    "only peers that are blacklisted or already connected to")
                should_skip = skip_candidate_if_on_list  # type: ignore

            try:
                candidate_counts = await asyncio.gather(*(
                    self._add_peers_from_backend(backend, should_skip)
                    for backend in self.peer_backends
                ))
                last_candidates_count = sum(candidate_counts)
            except OperationCancelled:
                break
            except asyncio.CancelledError:
                # no need to log this exception, this is expected
                raise
            except Exception:
                self.logger.exception("unexpected error during peer connection")
                # Continue trying to connect to peers, even if there was a
                # surprising failure during one of the attempts.
                continue

    @property
    def vm_configuration(self) -> Tuple[Tuple[BlockNumber, Type[VirtualMachineAPI]], ...]:
        return self.context.vm_configuration

    async def get_chain_head(self) -> BlockHeader:
        return await self.wait(self.context.headerdb.coro_get_canonical_head())

    async def get_genesis_hash(self) -> Hash32:
        return await self.wait(
            self.context.headerdb.coro_get_canonical_block_hash(BlockNumber(GENESIS_BLOCK_NUMBER))
        )

    @property
    def highest_td_peer(self) -> BaseChainPeer:
        peers = tuple(self.connected_nodes.values())
        if not peers:
            raise NoConnectedPeers("No connected peers")

        td_getter = excepts(
            (PeerConnectionLost, UnknownAPI),
            operator.attrgetter('head_info.head_td'),
            lambda _: 0,
        )
        peers_by_td = groupby(td_getter, peers)
        max_td = max(peers_by_td.keys())
        return random.choice(peers_by_td[max_td])

    def get_peers(self, min_td: int) -> List[BaseChainPeer]:
        # TODO: Consider turning this into a method that returns an AsyncIterator, to make it
        # harder for callsites to get a list of peers while making blocking calls, as those peers
        # might disconnect in the meantime.
        peers = tuple(self.connected_nodes.values())
        return [peer for peer in peers if peer.head_info.head_td >= min_td]

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


def skip_candidate_if_on_list(skip_list: Container[NodeID], candidate: NodeAPI) -> bool:
    # This shouldn't happen as we don't keep ENRs with no endpoint information, but we check it
    # here just in case.
    if candidate.address is None:
        p2p_logger.warning("Skipping connection candidate with no endpoint info: %s", candidate)
        return True
    if candidate.id in skip_list:
        p2p_logger.debug2("Skipping connection candidate (%s) as it's on skip list", candidate)
        return True
    return False


def skip_candidate_if_on_list_or_fork_mismatch(
        genesis_hash: Hash32,
        head: BlockNumber,
        fork_blocks: Tuple[BlockNumber, ...],
        skip_list: Container[NodeID],
        candidate: NodeAPI) -> bool:
    if skip_candidate_if_on_list(skip_list, candidate):
        return True

    # For now we accept candidates which don't specify a ForkID in their ENR, but we may want to
    # change that if we realize we're getting too many chain-mismatch errors when connecting.
    try:
        candidate_forkid = extract_forkid(candidate.enr)
    except ENRMissingForkID:
        p2p_logger.debug("Accepting connection candidate (%s) with no ForkID", candidate)
        return False
    except MalformedMessage as e:
        # Logging as a warning just in case there's a bug in our code that fails to deserialize
        # valid ForkIDs. If this becomes too noisy, we should consider reducing the severity.
        p2p_logger.warning(
            "Unable to extract ForkID from ENR of %s (%s), accepting as connection candidate "
            "anyway",
            candidate,
            e,
        )
        return False

    try:
        validate_forkid(candidate_forkid, genesis_hash, head, fork_blocks)
    except BaseForkIDValidationError as e:
        p2p_logger.debug("Skipping forkid-incompatible connection candidate (%s): %s", candidate, e)
        return True

    p2p_logger.debug("Accepting forkid-compatible connection candidate (%s)", candidate)
    return False
