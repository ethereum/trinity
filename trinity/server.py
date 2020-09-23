from abc import abstractmethod
import asyncio
from typing import (
    Generic,
    Tuple,
    Type,
    TypeVar,
)


from async_service import Service

from lahja import EndpointAPI

from eth_keys import datatypes
from eth_typing import BlockNumber

from eth.abc import AtomicDatabaseAPI, VirtualMachineAPI
from pyformance import MetricsRegistry

from p2p.constants import DEFAULT_MAX_PEERS, DEVP2P_V5
from p2p.exceptions import (
    HandshakeFailure,
    NoMatchingPeerCapabilities,
    PeerConnectionLost,
)
from p2p.handshake import receive_dial_in, DevP2PHandshakeParams

from trinity._utils.version import construct_trinity_client_identifier
from trinity.chains.base import AsyncChainAPI
from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.common.context import ChainContext
from trinity.protocol.common.peer import BasePeerPool
from trinity.protocol.eth.peer import ETHPeerPool
from trinity.protocol.les.peer import LESPeerPool
from trinity._utils.logging import get_logger


BOUND_IP = '0.0.0.0'

TPeerPool = TypeVar('TPeerPool', bound=BasePeerPool)
T_VM_CONFIGURATION = Tuple[Tuple[BlockNumber, Type[VirtualMachineAPI]], ...]

COMMON_RECEIVE_HANDSHAKE_EXCEPTIONS = (
    asyncio.TimeoutError,
    PeerConnectionLost,
    HandshakeFailure,
    NoMatchingPeerCapabilities,
    asyncio.IncompleteReadError,
)


class BaseServer(Service, Generic[TPeerPool]):
    """Server listening for incoming connections"""
    _tcp_listener = None
    peer_pool: TPeerPool

    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 port: int,
                 chain: AsyncChainAPI,
                 chaindb: BaseAsyncChainDB,
                 headerdb: BaseAsyncHeaderDB,
                 base_db: AtomicDatabaseAPI,
                 network_id: int,
                 max_peers: int = DEFAULT_MAX_PEERS,
                 event_bus: EndpointAPI = None,
                 metrics_registry: MetricsRegistry = None,
                 ) -> None:
        self.logger = get_logger(self.__module__ + '.' + self.__class__.__name__)
        # cross process event bus
        self.event_bus = event_bus
        self.metrics_registry = metrics_registry

        # setup parameters for the base devp2p handshake.
        self.p2p_handshake_params = DevP2PHandshakeParams(
            client_version_string=construct_trinity_client_identifier(),
            listen_port=port,
            version=DEVP2P_V5,
        )

        # chain information
        self.chain = chain
        self.chaindb = chaindb
        self.headerdb = headerdb
        self.base_db = base_db

        # node information
        self.privkey = privkey
        self.port = port
        self.network_id = network_id
        self.max_peers = max_peers

        self.ready = asyncio.Event()
        # child services
        self.peer_pool = self._make_peer_pool()

    @abstractmethod
    def _make_peer_pool(self) -> TPeerPool:
        ...

    async def run(self) -> None:
        self.logger.info("Running server...")
        self.logger.info(
            "enode://%s@%s:%s",
            self.privkey.public_key.to_hex()[2:],
            BOUND_IP,
            self.port,
        )
        self.logger.info('network: %s', self.network_id)
        self.logger.info('peers: max_peers=%s', self.max_peers)

        # TODO: Support IPv6 addresses as well.
        tcp_listener = await asyncio.start_server(
            self.receive_handshake,
            host=BOUND_IP,
            port=self.port,
        )
        try:
            async with tcp_listener:
                self.manager.run_daemon_child_service(self.peer_pool)
                self.ready.set()
                await tcp_listener.serve_forever()
        finally:
            self.logger.info("TCP Listener finished, cancelling Server")
            self.manager.cancel()

    async def receive_handshake(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        try:
            try:
                await self._receive_handshake(reader, writer)
            except BaseException:
                if not reader.at_eof():
                    reader.feed_eof()
                writer.close()
                raise
        except COMMON_RECEIVE_HANDSHAKE_EXCEPTIONS as e:
            peername = writer.get_extra_info("peername")
            self.logger.debug("Could not complete handshake with %s: %s", peername, e)
        except asyncio.CancelledError:
            # This exception should just bubble.
            raise
        except Exception:
            peername = writer.get_extra_info("peername")
            self.logger.exception("Unexpected error handling handshake with %s", peername)

    async def _receive_handshake(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        factory = self.peer_pool.get_peer_factory()
        handshakers = await factory.get_handshakers()
        connection = await receive_dial_in(
            reader=reader,
            writer=writer,
            private_key=self.privkey,
            p2p_handshake_params=self.p2p_handshake_params,
            protocol_handshakers=handshakers,
        )

        async with self.peer_pool.lock_node_for_handshake(connection.remote):
            peer = factory.create_peer(connection)
            await self.peer_pool.add_inbound_peer(peer)


class FullServer(BaseServer[ETHPeerPool]):

    def _make_peer_pool(self) -> ETHPeerPool:
        context = ChainContext(
            headerdb=self.headerdb,
            network_id=self.network_id,
            vm_configuration=self.chain.vm_configuration,
            client_version_string=self.p2p_handshake_params.client_version_string,
            listen_port=self.p2p_handshake_params.listen_port,
            p2p_version=self.p2p_handshake_params.version,
        )
        return ETHPeerPool(
            privkey=self.privkey,
            context=context,
            max_peers=self.max_peers,
            event_bus=self.event_bus,
            metrics_registry=self.metrics_registry,
        )


class LightServer(BaseServer[LESPeerPool]):

    def _make_peer_pool(self) -> LESPeerPool:
        context = ChainContext(
            headerdb=self.headerdb,
            network_id=self.network_id,
            vm_configuration=self.chain.vm_configuration,
            client_version_string=self.p2p_handshake_params.client_version_string,
            listen_port=self.p2p_handshake_params.listen_port,
            p2p_version=self.p2p_handshake_params.version,
        )
        return LESPeerPool(
            privkey=self.privkey,
            context=context,
            max_peers=self.max_peers,
            event_bus=self.event_bus,
            metrics_registry=self.metrics_registry,
        )
