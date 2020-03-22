import contextlib
from abc import abstractmethod
from pathlib import Path
from typing import (
    Generic,
    Iterator,
    TypeVar,
)

from async_service import Service
from cancel_token import CancelToken
from lahja import EndpointAPI

from eth.abc import AtomicDatabaseAPI

from p2p.abc import AsyncioServiceAPI
from p2p.peer_pool import BasePeerPool

from trinity.chains.base import AsyncChainAPI
from trinity.chains.full import FullChain
from trinity.db.manager import DBClient
from trinity.db.eth1.header import (
    AsyncHeaderDB,
)
from trinity.events import ShutdownRequest
from trinity.config import (
    Eth1ChainConfig,
    Eth1AppConfig,
    TrinityConfig,
)
from trinity.protocol.common.peer import BasePeer
from trinity.protocol.common.peer_pool_event_bus import (
    PeerPoolEventServer,
)

from .events import (
    NetworkIdRequest,
    NetworkIdResponse,
)
from ..components.builtin.metrics.abc import MetricsServiceAPI

TPeer = TypeVar('TPeer', bound=BasePeer)


class Node(Service, Generic[TPeer]):
    """
    Create usable nodes by adding subclasses that define the following
    unset attributes.
    """
    _full_chain: FullChain = None
    _event_server: PeerPoolEventServer[TPeer] = None

    def __init__(self,
                 event_bus: EndpointAPI,
                 metrics_service: MetricsServiceAPI,
                 trinity_config: TrinityConfig) -> None:
        self.trinity_config = trinity_config

        self._jsonrpc_ipc_path: Path = trinity_config.jsonrpc_ipc_path
        self._network_id = trinity_config.network_id

        self.event_bus = event_bus
        self.metrics_service = metrics_service
        self.master_cancel_token = CancelToken(type(self).__name__)

    async def handle_network_id_requests(self) -> None:
        async for req in self.event_bus.stream(NetworkIdRequest):
            # We are listening for all `NetworkIdRequest` events but we ensure to only send a
            # `NetworkIdResponse` to the callsite that made the request.  We do that by
            # retrieving a `BroadcastConfig` from the request via the
            # `event.broadcast_config()` API.
            await self.event_bus.broadcast(
                NetworkIdResponse(self._network_id),
                req.broadcast_config()
            )

    _chain_config: Eth1ChainConfig = None

    @property
    def chain_config(self) -> Eth1ChainConfig:
        """
        Convenience and caching mechanism for the `ChainConfig`.
        """
        if self._chain_config is None:
            app_config = self.trinity_config.get_app_config(Eth1AppConfig)
            self._chain_config = app_config.get_chain_config()
        return self._chain_config

    @abstractmethod
    def get_chain(self, base_db: AtomicDatabaseAPI) -> AsyncChainAPI:
        ...

    def get_full_chain(self, base_db: AtomicDatabaseAPI) -> FullChain:
        if self._full_chain is None:
            chain_class = self.chain_config.full_chain_class
            self._full_chain = chain_class(base_db)

        return self._full_chain

    @abstractmethod
    def get_event_server(self, base_db: AtomicDatabaseAPI) -> PeerPoolEventServer[TPeer]:
        """
        Return the ``PeerPoolEventServer`` of the node
        """
        ...

    @abstractmethod
    def get_peer_pool(self, base_db: AtomicDatabaseAPI) -> BasePeerPool:
        """
        Return the PeerPool instance of the node
        """
        ...

    @abstractmethod
    def get_p2p_server(self, base_db: AtomicDatabaseAPI) -> AsyncioServiceAPI:
        """
        This is the main service that will be run, when calling :meth:`run`.
        It's typically responsible for syncing the chain, with peer connections.
        """
        ...

    @contextlib.contextmanager
    def db_client_ctx(self) -> Iterator[AtomicDatabaseAPI]:
        with DBClient.connect(self.trinity_config.database_ipc_path) as db:
            yield db

    def get_headerdb(self, base_db: AtomicDatabaseAPI) -> AsyncHeaderDB:
        return AsyncHeaderDB(base_db)

    async def run(self) -> None:
        with self.db_client_ctx() as base_db:
            self.manager.run_daemon_task(self.handle_network_id_requests)
            self.manager.run_daemon_child_service(self.get_p2p_server(base_db).as_new_service())
            self.manager.run_daemon_child_service(self.get_event_server(base_db))
            self.manager.run_daemon_child_service(self.metrics_service)

            try:
                await self.manager.wait_finished()
            finally:
                self.master_cancel_token.trigger()
                await self.event_bus.broadcast(ShutdownRequest("Node exiting. Triggering shutdown"))
