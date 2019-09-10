import asyncio
import functools
import operator
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Sequence,
    Set,
    Tuple,
    Type,
)

from async_generator import asynccontextmanager

from cancel_token import CancelToken, OperationCancelled

from eth_keys import keys

from eth_utils.toolz import groupby

from p2p.abc import (
    CapacityLimiterAPI,
    ConnectionAPI,
    ConnectionPoolAPI,
    HandlerSubscriptionAPI,
    NodeAPI,
    PoolChangedAPI,
    PoolManagerAPI,
)
from p2p.disconnect import DisconnectReason
from p2p.exceptions import (
    HandshakeFailure,
    IneligiblePeer,
    NoMatchingPeerCapabilities,
    PeerConnectionLost,
    UnreachablePeer
)
from p2p.handler_subscription import HandlerSubscription
from p2p.handshake import (
    dial_out,
    receive_dial_in,
    DevP2PHandshakeParams,
    Handshaker,
)
from p2p.kademlia import Node, Address
from p2p.pool import ConnectionPool
from p2p.resource_lock import ResourceLock
from p2p.service import BaseService, run_service


# A function that is given the pool and a remote which returns `True` if we should connect.
CandidateFilterFn = Callable[[ConnectionPool, NodeAPI], bool]
ConnectionFilterFn = Callable[[ConnectionPool, ConnectionAPI], bool]


EXPECTED_RECEIVE_ERRORS = (
    HandshakeFailure,
    NoMatchingPeerCapabilities,
    OperationCancelled,
    asyncio.TimeoutError,
    PeerConnectionLost,
)
EXPECTED_DIAL_ERRORS = (
    HandshakeFailure,
    NoMatchingPeerCapabilities,
    OperationCancelled,
    PeerConnectionLost,
    asyncio.TimeoutError,
    UnreachablePeer,
)

HandshakerProviderFn = Callable[[], Awaitable[Handshaker]]
PeerProviderFn = Callable[[ConnectionPoolAPI], Awaitable[Tuple[NodeAPI, ...]]]
OnConnectFn = Callable[[ConnectionPoolAPI, ConnectionAPI], Awaitable[Any]]
OnDisconnectFn = OnConnectFn


class PoolChanged(PoolChangedAPI):
    """
    Helper for monitoring changes in the connection pool.

    Usable either as an async context manager or an awaitable.
    """
    def __init__(self, condition: asyncio.Condition) -> None:
        self._condition = condition

    def __await__(self) -> Generator[Any, Any, None]:
        return self._do_await().__await__()

    async def _do_await(self) -> None:
        async with self._condition:
            await self._condition.wait()

    async def __aenter__(self) -> None:
        await self._condition.acquire()

    async def __aexit__(self,
                        exc_type: Type[BaseException],
                        exc_value: BaseException,
                        exc_tb: TracebackType) -> None:
        try:
            await self._condition.wait()
        finally:
            self._condition.release()


class PoolManager(BaseService, PoolManagerAPI):
    _on_connect_handlers: Set[OnConnectFn]
    _on_disconnect_handlers: Set[OnDisconnectFn]

    def __init__(self,
                 pool: ConnectionPool,
                 private_key: keys.PrivateKey,
                 p2p_handshake_params: DevP2PHandshakeParams,
                 handshaker_providers: Sequence[HandshakerProviderFn],
                 capacity_limiter: CapacityLimiterAPI,
                 token: CancelToken) -> None:
        super().__init__(token=token)
        self.pool = pool
        self._private_key = private_key
        self.p2p_handshake_params = p2p_handshake_params
        self.handshaker_providers = handshaker_providers
        self._pool_changed = asyncio.Condition()

        self._on_connect_handlers = set()
        self._on_disconnect_handlers = set()

        self._handshake_locks = ResourceLock()

        self._capacity_limiter = capacity_limiter

    @property
    def public_key(self) -> keys.PublicKey:
        return self._private_key.public_key

    async def _run(self) -> None:
        try:
            await self.cancellation()
        except asyncio.CancelledError:
            pass

    def wait_pool_changed(self) -> PoolChangedAPI:
        """
        Wait for the connection pool to change.

        This can be directly awaited
        """
        return PoolChanged(self._pool_changed)

    def on_connect(self, handler_fn: OnConnectFn) -> HandlerSubscriptionAPI:
        self._on_connect_handlers.add(handler_fn)
        cancel_fn = functools.partial(self._on_connect_handlers.remove, handler_fn)
        return HandlerSubscription(cancel_fn)

    def on_disconnect(self, handler_fn: OnDisconnectFn) -> HandlerSubscriptionAPI:
        self._on_disconnect_handlers.add(handler_fn)
        cancel_fn = functools.partial(self._on_disconnect_handlers.remove, handler_fn)
        return HandlerSubscription(cancel_fn)

    async def add_connection(self, connection: ConnectionAPI) -> None:
        """
        Adds the provided connection to the pool and runs the connection service.
        """
        self.run_task(self._run_connection(connection))
        await connection.events.started.wait()

    @asynccontextmanager
    async def listen(self, host: str, port: int) -> AsyncIterator[NodeAPI]:
        server = await asyncio.start_server(
            self._receive_handshake,
            host=host,
            port=port,
        )
        node = Node(pubkey=self.public_key, address=Address(ip=host, udp_port=port))
        try:
            yield node
        finally:
            server.close()
            # This check for whether `server.connections` is populated is to
            # fix a bug where calling `server.wait_closed()` throws an error
            # due to it trying to do an `asyncio.gather(...)` on an empty set
            # of coroutines.
            # mypy doesn't recognize this having the `connections` property.
            if hasattr(server, 'connections') and server.connections:  # type: ignore
                await server.wait_closed()

    async def seek_connections(self,
                               providers: Sequence[PeerProviderFn],
                               candidate_filters: Sequence[CandidateFilterFn] = ()) -> None:
        while self.is_operational:
            for provider in providers:
                candidates = await provider(self.pool)
                # TODO: enforce NodeAPI based filters
                # TODO: concurrent connection attempts
                for remote in candidates:
                    if not all(filter_fn(self.pool, remote) for filter_fn in candidate_filters):
                        continue

                    # This ensures that we stop dialing once at capacity.
                    await self._capacity_limiter.wait_for_capacity()

                    try:
                        await self.dial(remote)
                    except EXPECTED_DIAL_ERRORS:
                        continue

    async def dial(self, remote: NodeAPI) -> ConnectionAPI:
        if self._handshake_locks.is_locked(remote):
            self.logger.debug2("Skipping %s; already shaking hands", remote)
            raise IneligiblePeer(f"Already shaking hands with {remote}")

        # Try to acquire a token, potentially timing out if the pool is at capacity.
        await self.wait(self._capacity_limiter.acquire(), timeout=1)

        async with self._handshake_locks.lock(remote):
            handshakers = await asyncio.gather(*(
                provider() for provider in self.handshaker_providers
            ))

            connection = await dial_out(
                remote,
                self._private_key,
                self.p2p_handshake_params,
                handshakers,
                token=self.cancel_token,
            )
            self.logger.info('Established dial-out connection with: %s', connection.session)

            await self.add_connection(connection)
            return connection

    async def _run_connection(self, connection: ConnectionAPI) -> None:
        async with run_service(connection):

            # Run the `on_connect` handlers
            for on_connect_handler_fn in self._on_connect_handlers:
                await on_connect_handler_fn(self.pool, connection)

            # Early exit if the connection ends up cancelled as a result of one
            # of the `on_connect` handlers.
            if connection.is_cancelled:
                return

            # Start the protocol streams
            connection.start_protocol_streams()

            # signal that the pool has changed.
            async with self._pool_changed:
                self.pool.add(connection)
                self._pool_changed.notify_all()

            try:
                await connection.events.finished.wait()
            except asyncio.CancelledError:
                pass
            finally:
                # Run the `on_disconnect` handlers
                for on_disconnect_handler_fn in self._on_disconnect_handlers:
                    await on_disconnect_handler_fn(self.pool, connection)

                # signal that the pool has changed
                async with self._pool_changed:
                    self.pool.remove(connection)
                    self._pool_changed.notify_all()

                # release the capacity token
                await self._capacity_limiter.release()

    async def _receive_handshake(self,
                                 reader: asyncio.StreamReader,
                                 writer: asyncio.StreamWriter) -> None:
        # TODO: locking to ensure concurrent dial/receive are not a problem
        handshakers = await asyncio.gather(*(
            provider() for provider in self.handshaker_providers
        ))

        try:
            try:
                connection = await receive_dial_in(
                    reader=reader,
                    writer=writer,
                    private_key=self._private_key,
                    p2p_handshake_params=self.p2p_handshake_params,
                    protocol_handshakers=handshakers,
                    token=self.cancel_token,
                )
            except BaseException:
                if not reader.at_eof():
                    reader.feed_eof()
                writer.close()
                raise

            self.logger.info('Established dial-in connection with: %s', connection.session)
        except EXPECTED_RECEIVE_ERRORS as e:
            self.logger.debug("Could not complete handshake: %s", e)
        except Exception as e:
            self.logger.exception("Unexpected error handling handshake")
        else:
            # acquire a capacity token
            if self._handshake_locks.is_locked(connection.remote):
                self.logger.debug2("Skipping %s; already shaking hands", connection.remote)
                connection.get_base_protocol().send_disconnect(DisconnectReason.already_connected)
                return

            async with self._handshake_locks.lock(connection.remote):
                # acquire a capacity token
                try:
                    await self.wait(self._capacity_limiter.acquire(), timeout=1)
                except asyncio.TimeoutError:
                    connection.get_base_protocol().send_disconnect(DisconnectReason.too_many_peers)
                else:
                    await self.add_connection(connection)


async def enforce_dial_in_to_out_ratio(pool: ConnectionPool,
                                       connection: ConnectionAPI,
                                       *,
                                       ratio: float) -> None:
    # we count the incoming connection too since it shouldn't be in the pool
    # yet.
    if connection.is_dial_out:
        return
    total_connections = len(pool) + 1
    dial_in_count = len(tuple(
        connection
        for connection
        in pool
        if connection.is_dial_in
    )) + 1
    if total_connections > 1 and dial_in_count / total_connections > ratio:
        connection.get_base_protocol().send_disconnect(DisconnectReason.too_many_peers)
        connection.cancel_nowait()
        connection.logger.debug(
            'Pool dial-in to dial-out ratio too too high. Disconnecting %s.',
            connection.session,
        )


async def enforce_ip_address_diversity(pool: ConnectionPool,
                                       connection: ConnectionAPI,
                                       *,
                                       max_connections_per_ip: int) -> None:
    # connect to no more then 2 nodes with the same IP
    nodes_by_ip = groupby(
        operator.attrgetter('remote.address.ip'),
        pool,
    )
    matching_ip_nodes = nodes_by_ip.get(connection.remote.address.ip, [])
    if len(matching_ip_nodes) > 2:
        connection.logger.debug(
            'Too many connections from same IP.  Disconnecting %s',
            connection.session,
        )
        connection.get_base_protocol().send_disconnect(DisconnectReason.too_many_peers)
