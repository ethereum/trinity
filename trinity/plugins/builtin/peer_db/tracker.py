from abc import abstractmethod
import datetime
import logging
from pathlib import Path
from typing import (
    Any,
    cast,
    FrozenSet,
    Iterable,
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
    ForeignKey,
)
from sqlalchemy.orm.exc import NoResultFound

from lahja import (
    BroadcastConfig,
    Endpoint,
)

from eth_typing import Hash32

from eth_utils import to_tuple

from p2p import protocol
from p2p.peer_backend import BasePeerBackend
from p2p.kademlia import Node
from p2p.peer import (
    BasePeer,
    PeerSubscriber,
)

from trinity.constants import (
    PEER_DB_EVENTBUS_ENDPOINT,
)
from trinity.db.orm import (
    get_tracking_database,
    Base,
)
from trinity.protocol.common.peer import BaseChainPeer

from .events import (
    TrackPeerEvent,
    TrackPeerMetaEvent,
    GetPeerCandidatesRequest,
)


class Remote(Base):
    __tablename__ = 'remotes'

    id = Column(Integer, primary_key=True)

    uri = Column(String, unique=True, nullable=False, index=True)
    is_outbound = Column(Boolean, nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False)
    last_connected_at = Column(DateTime(timezone=True), nullable=False)

    eth1_meta = relationship("Eth1Meta", back_populates="remote")


class Eth1Meta(Base):
    __tablename__ = 'eth1_metas'

    id = Column(Integer, primary_key=True)
    remote_id = Column(Integer, ForeignKey('remotes.id'), nullable=False, index=True)

    remote = relationship("Remote", back_populates="eth1_meta")

    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, index=True)

    genesis_hash = Column(String, nullable=False, index=True)
    protocol = Column(String, nullable=False, index=True)
    protocol_version = Column(Integer, nullable=False, index=True)
    network_id = Column(Integer, nullable=False, index=True)


class BaseEth1PeerTracker(BasePeerBackend, PeerSubscriber):
    logger = logging.getLogger('trinity.protocol.common.connection.PeerTracker')

    msg_queue_maxsize = 10
    subscription_msg_types: FrozenSet[Type[protocol.Command]] = frozenset()

    def register_peer(self, peer: BasePeer) -> None:
        peer = cast(BaseChainPeer, peer)
        self.logger.debug('tracking peer connection: %s', peer)
        self.track_peer_connection(peer.remote, not peer.inbound)
        self.update_peer_meta(
            remote=peer.remote,
            genesis_hash=peer.genesis_hash,
            protocol=peer.sub_proto.name,
            protocol_version=peer.sub_proto.version,
            network_id=peer.network_id,
        )
        self.logger.debug('finished tracking peer connection: %s', peer)

    def deregister_peer(self, peer: BasePeer) -> None:
        # TODO: this is where we can track actual performance data about peers
        pass

    @abstractmethod
    def track_peer_connection(self, remote: Node, is_outbound: bool) -> None:
        pass

    @abstractmethod
    def update_peer_meta(self,
                         remote: Node,
                         genesis_hash: Hash32,
                         protocol: str,
                         protocol_version: int,
                         network_id: int) -> None:
        pass

    @abstractmethod
    async def get_peer_candidates(self,
                                  num_requested: int,
                                  num_connected_peers: int) -> Iterable[Node]:
        pass


class NoopEth1PeerTracker(BaseEth1PeerTracker):
    def track_peer_connection(self, remote: Node, is_outbound: bool) -> None:
        pass

    def update_peer_meta(self,
                         remote: Node,
                         genesis_hash: Hash32,
                         protocol: str,
                         protocol_version: int,
                         network_id: int) -> None:
        pass

    async def get_peer_candidates(self,
                                  num_requested: int,
                                  num_connected_peers: int) -> Iterable[Node]:
        return ()


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

    def track_peer_connection(self, remote: Node, is_outbound: bool) -> None:
        uri = remote.uri()
        if self._remote_exists(uri):
            self.logger.debug("Updated peer: %s", remote)
            node = self._get_remote(uri)
            node.last_connected_at = datetime.datetime.utcnow()
            return
        else:
            self.logger.debug("New peer: %s", remote)
            now = datetime.datetime.utcnow()
            node = Remote(
                uri=uri,
                created_at=now,
                last_connected_at=now,
                is_outbound=is_outbound,
            )

        self.session.add(node)
        self.session.commit()

    def update_peer_meta(self,
                         remote: Node,
                         genesis_hash: Hash32,
                         protocol: str,
                         protocol_version: int,
                         network_id: int) -> None:
        uri = remote.uri()
        try:
            node = self._get_remote(uri)
        except NoResultFound:
            self.logger.warning("Attempt to track metadata for untracked peer: %s", remote)
            return

        if self._meta_exists(uri):
            self.logger.debug("Updated metadata: %s", remote)
            eth1_meta = self._get_meta(uri)
            eth1_meta.genesis_hash = genesis_hash.hex()
            eth1_meta.protocol = protocol
            eth1_meta.protocol_version = protocol_version
            eth1_meta.network_id = network_id
            eth1_meta.updated_at = datetime.datetime.utcnow()
        else:
            self.logger.debug("New metadata: %s", remote)
            now = datetime.datetime.utcnow()
            eth1_meta = Eth1Meta(
                created_at=now,
                updated_at=now,
                remote_id=node.id,
                genesis_hash=genesis_hash.hex(),
                protocol=protocol,
                protocol_version=protocol_version,
                network_id=network_id,
            )

        self.session.add(eth1_meta)
        self.session.commit()

    @to_tuple
    def _get_candidate_filter_query(self) -> Iterable[Any]:
        if self.protocols is not None:
            if len(self.protocols) == 1:
                yield Eth1Meta.protocol == self.protocols[0]
            else:
                yield Eth1Meta.protocol.in_(self.protocols)

        if self.protocol_versions is not None:
            if len(self.protocol_versions) == 1:
                yield Eth1Meta.protocol_version == self.protocol_versions[0]
            else:
                yield Eth1Meta.protocol_version.in_(self.protocol_versions)

        if self.network_id is not None:
            yield Eth1Meta.network_id == self.network_id

        if self.genesis_hash is not None:
            yield Eth1Meta.genesis_hash == self.genesis_hash

    def _get_peer_candidates(self, num_requested: int) -> Iterable[Node]:
        total_candidates = self.session.query(Remote).join(Eth1Meta).filter().count()
        filters = self._get_candidate_filter_query()
        candidates = self.session.query(Remote).join(Eth1Meta).filter(
            Remote.is_outbound.is_(True),
            *filters,
        ).all()[:num_requested]
        self.logger.debug(
            'Returning %d candidates from %d total',
            len(candidates),
            total_candidates,
        )
        return tuple(Node.from_uri(remote.uri) for remote in candidates)

    async def get_peer_candidates(self,
                                  num_requested: int,
                                  num_connected_peers: int) -> Iterable[Node]:
        return self._get_peer_candidates(num_requested)

    #
    # Helpers
    #
    def _get_remote(self, uri: str) -> Remote:
        return self.session.query(Remote).filter_by(uri=uri).one()

    def _remote_exists(self, uri: str) -> bool:
        try:
            self._get_remote(uri)
        except NoResultFound:
            return False
        else:
            return True

    def _get_meta(self, uri: str) -> Eth1Meta:
        return self.session.query(Eth1Meta).join(Remote).filter(
            Remote.uri == uri,
        ).one()

    def _meta_exists(self, uri: str) -> bool:
        try:
            self._get_meta(uri)
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


TO_PEER_DB_BROADCAST_CONFIG = BroadcastConfig(filter_endpoint=PEER_DB_EVENTBUS_ENDPOINT)


class EventBusEth1PeerTracker(BaseEth1PeerTracker):
    def __init__(self,
                 event_bus: Endpoint,
                 config: BroadcastConfig = TO_PEER_DB_BROADCAST_CONFIG) -> None:
        self.event_bus = event_bus
        self.config = config

    def track_peer_connection(self, remote: Node, is_outbound: bool) -> None:
        self.event_bus.broadcast(
            TrackPeerEvent(remote, is_outbound),
            self.config,
        )

    def update_peer_meta(self,
                         remote: Node,
                         genesis_hash: Hash32,
                         protocol: str,
                         protocol_version: int,
                         network_id: int) -> None:
        self.event_bus.broadcast(
            TrackPeerMetaEvent(remote, genesis_hash, protocol, protocol_version, network_id),
            self.config,
        )

    async def get_peer_candidates(self,
                                  num_requested: int,
                                  num_connected_peers: int) -> Iterable[Node]:
        response = await self.event_bus.request(
            GetPeerCandidatesRequest(num_requested, num_connected_peers),
            self.config,
        )
        return response.candidates
