from abc import abstractmethod
import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    cast,
    FrozenSet,
    Iterable,
    Optional,
    Type,
    Tuple,
)

from sqlalchemy.orm import (
    relationship,
    Session as BaseSession,
)
from sqlalchemy import (
    Column,
    Boolean,
    Integer,
    DateTime,
    String,
)
from sqlalchemy.orm.exc import NoResultFound

from lahja import (
    BroadcastConfig,
    EndpointAPI,
)

from eth_typing import Hash32

from eth_utils import (
    humanize_seconds,
    get_extended_debug_logger,
    to_hex,
    to_tuple,
)

from p2p.abc import CommandAPI, NodeAPI
from p2p.discv5.typing import NodeID
from p2p.events import PeerCandidatesRequest
from p2p.kademlia import Node
from p2p.peer_backend import BasePeerBackend
from p2p.peer import (
    BasePeer,
    PeerSubscriber,
)

from trinity.constants import (
    NETWORKDB_EVENTBUS_ENDPOINT,
)
from trinity.db.orm import (
    get_tracking_database,
    Base,
)
from trinity.components.builtin.network_db.connection.tracker import (
    BlacklistRecord,
)

from .events import (
    TrackPeerEvent,
)


class Remote(Base):
    __tablename__ = 'remotes'

    id = Column(Integer, primary_key=True)

    # The hex-encoded NodeID, with the 0x prefix
    node_id = Column(String, unique=True, nullable=False, index=True)
    enr = Column(String, unique=True, nullable=False, index=False)
    is_outbound = Column(Boolean, nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, index=True)

    last_connected_at = Column(DateTime(timezone=True), nullable=True)

    genesis_hash = Column(String, nullable=False, index=True)
    protocol = Column(String, nullable=False, index=True)
    protocol_version = Column(Integer, nullable=False, index=True)
    network_id = Column(Integer, nullable=False, index=True)

    blacklist = relationship(
        "BlacklistRecord",
        primaryjoin="Remote.node_id==foreign(BlacklistRecord.node_id)",
        uselist=False,
    )


class BaseEth1PeerTracker(BasePeerBackend, PeerSubscriber):
    logger = get_extended_debug_logger('trinity.protocol.common.connection.PeerTracker')

    msg_queue_maxsize = 100
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset()

    def register_peer(self, peer: BasePeer) -> None:
        # prevent circular import
        from trinity.protocol.common.peer import BaseChainPeer
        peer = cast(BaseChainPeer, peer)
        self.logger.debug(
            'tracking %s peer connection: %s',
            'inbound' if peer.inbound else 'outbound',
            peer,
        )
        self.track_peer_connection(
            remote=peer.remote,
            is_outbound=not peer.inbound,
            last_connected_at=None,
            genesis_hash=peer.chain_info.genesis_hash,
            protocol=peer.sub_proto.name,
            protocol_version=peer.sub_proto.version,
            network_id=peer.chain_info.network_id,
        )

    def deregister_peer(self, peer: BasePeer) -> None:
        """
        At disconnection we check whether our session with the peer was long
        enough to warrant recording statistics about them in our peer database.
        """
        # prevent circular import
        from trinity.protocol.common.peer import BaseChainPeer
        peer = cast(BaseChainPeer, peer)
        if peer.p2p_api.remote_disconnect_reason is None and peer.p2p_api.remote_disconnect_reason is None:  # noqa: E501
            # we don't care about peers that don't properly disconnect
            self.logger.debug(
                'Not tracking disconnecting peer %s[%s] missing disconnect reason',
                peer,
                'inbound' if peer.inbound else 'outbound',
            )
            return
        elif peer.uptime < MIN_QUALIFYING_UPTIME:
            # we don't register a peer who connects for less than
            # `MIN_QUALIFYING_UPTIME` as having been a successful connection.
            self.logger.debug(
                'Not tracking disconnecting peer %s[%s][%s] due to insufficient uptime (%s < %s)',
                peer,
                peer.p2p_api.local_disconnect_reason,
                'inbound' if peer.inbound else 'outbound',
                humanize_seconds(peer.uptime),
                humanize_seconds(MIN_QUALIFYING_UPTIME),
            )
            return
        else:
            self.logger.debug(
                'Tracking disconnecting peer %s[%s][%s] with uptime: %s',
                peer,
                'inbound' if peer.inbound else 'outbound',
                peer.p2p_api.local_disconnect_reason,
                humanize_seconds(peer.uptime),
            )

        self.track_peer_connection(
            remote=peer.remote,
            is_outbound=not peer.inbound,
            last_connected_at=datetime.datetime.utcnow(),
            genesis_hash=peer.chain_info.genesis_hash,
            protocol=peer.sub_proto.name,
            protocol_version=peer.sub_proto.version,
            network_id=peer.chain_info.network_id,
        )

    @abstractmethod
    def track_peer_connection(self,
                              remote: NodeAPI,
                              is_outbound: bool,
                              last_connected_at: Optional[datetime.datetime],
                              genesis_hash: Hash32,
                              protocol: str,
                              protocol_version: int,
                              network_id: int) -> None:
        ...

    @abstractmethod
    async def get_peer_candidates(self,
                                  max_candidates: int,
                                  should_skip_fn: Callable[[NodeAPI], bool]) -> Tuple[NodeAPI, ...]:
        ...


class NoopEth1PeerTracker(BaseEth1PeerTracker):
    def track_peer_connection(self,
                              remote: NodeAPI,
                              is_outbound: bool,
                              last_connected_at: Optional[datetime.datetime],
                              genesis_hash: Hash32,
                              protocol: str,
                              protocol_version: int,
                              network_id: int) -> None:
        pass

    async def get_peer_candidates(self,
                                  max_candidates: int,
                                  should_skip_fn: Callable[[NodeAPI], bool]) -> Tuple[NodeAPI, ...]:
        return ()


MIN_QUALIFYING_UPTIME = 60


class SQLiteEth1PeerTracker(BaseEth1PeerTracker):
    def __init__(self,
                 session: BaseSession,
                 genesis_hash: Hash32 = None,
                 protocols: Tuple[str, ...] = None,
                 protocol_versions: Tuple[int, ...] = None,
                 network_id: int = None) -> None:
        self.session = session
        if genesis_hash is not None:
            self.genesis_hash = genesis_hash.hex()
        else:
            self.genesis_hash = genesis_hash
        self.protocols = protocols
        self.protocol_versions = protocol_versions
        self.network_id = network_id

    def track_peer_connection(self,
                              remote: NodeAPI,
                              is_outbound: bool,
                              last_connected_at: Optional[datetime.datetime],
                              genesis_hash: Hash32,
                              protocol: str,
                              protocol_version: int,
                              network_id: int) -> None:
        enr = repr(remote.enr)
        now = datetime.datetime.utcnow()

        if self._remote_exists(remote.id):
            self.logger.debug2("Updated ETH1 peer record: %s", remote)
            record = self._get_remote(remote.id)

            record.updated_at = now

            if last_connected_at is not None:
                record.last_connected_at = last_connected_at

            record.genesis_hash = genesis_hash.hex()
            record.protocol = protocol
            record.protocol_version = protocol_version
            record.network_id = network_id
        else:
            self.logger.debug2("New ETH1 peer record: %s", remote)
            record = Remote(
                node_id=to_hex(remote.id),
                enr=enr,
                is_outbound=is_outbound,
                created_at=now,
                updated_at=now,
                last_connected_at=last_connected_at,
                genesis_hash=genesis_hash.hex(),
                protocol=protocol,
                protocol_version=protocol_version,
                network_id=network_id,
            )

        self.session.add(record)
        self.session.commit()  # type: ignore

    @to_tuple
    def _get_candidate_filter_query(self) -> Iterable[Any]:
        yield Remote.is_outbound.is_(True)  # type: ignore

        if self.protocols is not None:
            if len(self.protocols) == 1:
                yield Remote.protocol == self.protocols[0]
            else:
                yield Remote.protocol.in_(self.protocols)  # type: ignore

        if self.protocol_versions is not None:
            if len(self.protocol_versions) == 1:
                yield Remote.protocol_version == self.protocol_versions[0]
            else:
                yield Remote.protocol_version.in_(self.protocol_versions)  # type: ignore

        if self.network_id is not None:
            yield Remote.network_id == self.network_id

        if self.genesis_hash is not None:
            yield Remote.genesis_hash == self.genesis_hash

    def _get_peer_candidates(self,
                             max_candidates: int,
                             should_skip_fn: Callable[[NodeAPI], bool]) -> Iterable[NodeAPI]:
        """
        Return up to `max_candidates` candidates sourced from peers whe have
        historically connected to which match the following criteria:

        * Matches all of: network_id, protocol, genesis_hash, protocol_version
        * Either has no blacklist record or existing blacklist record is expired.
        * Not in the set of remotes we are already connected to.
        """
        now = datetime.datetime.utcnow()
        metadata_filters = self._get_candidate_filter_query()

        # Query the database for peers that match our criteria.
        candidates = self.session.query(Remote).outerjoin(  # type: ignore
            # Join against the blacklist records with matching node ID
            Remote.blacklist,
        ).filter(
            # XXX: This is no longer necessary as the should_skip_fn() function now takes care of
            # skipping blacklisted peers, but not sure we want to get rid of this?
            # Either they have no blacklist record or the record is expired.
            ((Remote.blacklist == None) | (BlacklistRecord.expires_at <= now)),  # noqa: E711
            # They match our filters for network metadata
            *metadata_filters,
        ).order_by(
            # We want the ones that we have recently connected to succesfully to be first.
            Remote.last_connected_at.desc(),  # type: ignore
        )

        # Return them as an iterator to allow the consuming process to
        # determine how many records it wants to fetch.
        for candidate in candidates:
            node = Node.from_enr_repr(candidate.enr)
            if not should_skip_fn(node):
                yield node

    async def get_peer_candidates(self,
                                  max_candidates: int,
                                  should_skip_fn: Callable[[NodeAPI], bool]) -> Tuple[NodeAPI, ...]:
        # For now we fully evaluate the response in order to print debug
        # statistics.  Once this API is no longer experimental, this should be
        # adjusted to only consume a maximum of `max_candidates` from the
        # returned iterable.
        all_candidates = tuple(self._get_peer_candidates(max_candidates, should_skip_fn))
        candidates = all_candidates[:max_candidates]
        total_in_database = self.session.query(Remote).count()  # type: ignore
        self.logger.debug(
            "Eth1 Peer Candidate Request: req=%d  ret=%d  avail=%d  total=%d",
            max_candidates,
            len(candidates),
            len(all_candidates),
            total_in_database,
        )
        return candidates

    #
    # Helpers
    #
    def _get_remote(self, node_id: NodeID) -> Remote:
        return self.session.query(Remote).filter_by(node_id=to_hex(node_id)).one()  # type: ignore

    def _remote_exists(self, node_id: NodeID) -> bool:
        try:
            self._get_remote(node_id)
        except NoResultFound:
            return False
        else:
            return True


class MemoryEth1PeerTracker(SQLiteEth1PeerTracker):
    def __init__(self,
                 genesis_hash: Hash32 = None,
                 protocols: Tuple[str, ...] = None,
                 protocol_versions: Tuple[int, ...] = None,
                 network_id: int = None) -> None:
        session = get_tracking_database(Path(":memory:"))
        super().__init__(session, genesis_hash, protocols, protocol_versions, network_id)


TO_NETWORKDB_BROADCAST_CONFIG = BroadcastConfig(filter_endpoint=NETWORKDB_EVENTBUS_ENDPOINT)


class EventBusEth1PeerTracker(BaseEth1PeerTracker):
    def __init__(self,
                 event_bus: EndpointAPI,
                 config: BroadcastConfig = TO_NETWORKDB_BROADCAST_CONFIG) -> None:
        self.event_bus = event_bus
        self.config = config

    def track_peer_connection(self,
                              remote: NodeAPI,
                              is_outbound: bool,
                              last_connected_at: Optional[datetime.datetime],
                              genesis_hash: Hash32,
                              protocol: str,
                              protocol_version: int,
                              network_id: int) -> None:
        self.event_bus.broadcast_nowait(
            TrackPeerEvent(
                remote,
                is_outbound,
                last_connected_at,
                genesis_hash,
                protocol,
                protocol_version,
                network_id,
            ),
            self.config,
        )

    async def get_peer_candidates(self,
                                  max_candidates: int,
                                  should_skip_fn: Callable[[NodeAPI], bool]) -> Tuple[NodeAPI, ...]:
        response = await self.event_bus.request(
            PeerCandidatesRequest(max_candidates, should_skip_fn),
            self.config,
        )
        return response.candidates
