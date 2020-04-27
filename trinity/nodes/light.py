from typing import (
    cast,
    Type,
)

from lahja import EndpointAPI

from eth_keys.datatypes import PrivateKey
from eth_utils import (
    ValidationError,
)

from p2p.peer_pool import BasePeerPool

from trinity.chains.light import (
    LightDispatchChain,
)
from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.config import (
    TrinityConfig,
)
from trinity.db.eth1.chain import AsyncChainDB
from trinity.nodes.base import Node
from trinity.protocol.common.peer_pool_event_bus import (
    PeerPoolEventServer,
)
from trinity.protocol.les.peer import (
    LESPeer,
    LESPeerPool,
    LESPeerPoolEventServer,
)
from trinity.server import LightServer
from trinity.sync.light.service import LightPeerChain


class LightNode(Node[LESPeer]):
    _chain: LightDispatchChain = None
    _peer_chain: LightPeerChain = None
    _p2p_server: LightServer = None

    network_id: int = None
    nodekey: PrivateKey = None

    def __init__(self,
                 event_bus: EndpointAPI,
                 metrics_service: MetricsServiceAPI,
                 trinity_config: TrinityConfig) -> None:
        super().__init__(event_bus, metrics_service, trinity_config)

        self._nodekey = trinity_config.nodekey
        self._port = trinity_config.port
        self._max_peers = trinity_config.max_peers

        self._peer_chain = LightPeerChain(
            self.headerdb,
            cast(LESPeerPool, self.get_peer_pool()),
        )

    @property
    def chain_class(self) -> Type[LightDispatchChain]:
        return self.chain_config.light_chain_class

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self._peer_chain)
        await super().run()

    def get_chain(self) -> LightDispatchChain:
        if self._chain is None:
            if self.chain_class is None:
                raise AttributeError("LightNode subclass must set chain_class")
            elif self._peer_chain is None:
                raise ValidationError("peer chain is not initialized!")
            else:
                self._chain = self.chain_class(self.headerdb, peer_chain=self._peer_chain)

        return self._chain

    def get_event_server(self) -> PeerPoolEventServer[LESPeer]:
        if self._event_server is None:
            self._event_server = LESPeerPoolEventServer(
                self.event_bus,
                self.get_peer_pool(),
                self._peer_chain
            )
        return self._event_server

    def get_p2p_server(self) -> LightServer:
        if self._p2p_server is None:
            self._p2p_server = LightServer(
                privkey=self._nodekey,
                port=self._port,
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
