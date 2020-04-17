from typing import Type

from lahja import EndpointAPI

from p2p.peer_pool import BasePeerPool

from trinity.chains.full import FullChain
from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.config import TrinityConfig
from trinity.db.eth1.chain import AsyncChainDB
from trinity.protocol.common.peer_pool_event_bus import PeerPoolEventServer
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPoolEventServer
from trinity.server import FullServer

from .base import Node


class FullNode(Node[ETHPeer]):
    _chain: FullChain = None
    _p2p_server: FullServer = None

    def __init__(self,
                 event_bus: EndpointAPI,
                 metrics_service: MetricsServiceAPI,
                 trinity_config: TrinityConfig) -> None:
        super().__init__(event_bus, metrics_service, trinity_config)
        self._node_key = trinity_config.nodekey
        self._node_port = trinity_config.port
        self._max_peers = trinity_config.max_peers

    @property
    def chain_class(self) -> Type[FullChain]:
        return self.chain_config.full_chain_class

    def get_chain(self) -> FullChain:
        return self.get_full_chain()

    def get_event_server(self) -> PeerPoolEventServer[ETHPeer]:
        """
        Return the ``PeerPoolEventServer`` of the FullNode
        """
        if self._event_server is None:
            self._event_server = ETHPeerPoolEventServer(
                self.event_bus, self.get_peer_pool())
        return self._event_server

    def get_p2p_server(self) -> FullServer:
        if self._p2p_server is None:
            self._p2p_server = FullServer(
                privkey=self._node_key,
                port=self._node_port,
                chain=self.get_full_chain(),
                chaindb=AsyncChainDB(self._base_db),
                headerdb=self.headerdb,
                base_db=self._base_db,
                network_id=self._network_id,
                max_peers=self._max_peers,
                event_bus=self.event_bus,
                metrics_registry=self.metrics_service.registry,
            )
        return self._p2p_server

    def get_peer_pool(self) -> BasePeerPool:
        return self.get_p2p_server().peer_pool
