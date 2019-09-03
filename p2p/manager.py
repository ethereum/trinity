import asyncio
import contextlib
import functools
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

from p2p.abc import (
    BehaviorAPI,
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
from p2p.service import run_service


# A function that is given the pool and a remote which returns `True` if we should connect.
CandidateFilterFn = Callable[[ConnectionPool, NodeAPI], bool]
ConnectionFilterFn = Callable[[ConnectionPool, ConnectionAPI], bool]


EXPECTED_RECEIVE_ERRORS = (
    HandshakeFailure,
    OperationCancelled,
    TimeoutError,
    PeerConnectionLost,
)
EXPECTED_DIAL_ERRORS = (
    HandshakeFailure,
    NoMatchingPeerCapabilities,
    OperationCancelled,
    PeerConnectionLost,
    TimeoutError,
    UnreachablePeer,
)

HandshakerProviderFn = Callable[[], Awaitable[Handshaker]]
PeerProviderFn = Callable[[ConnectionPoolAPI], Awaitable[Tuple[NodeAPI, ...]]]
OnConnectFn = Callable[[ConnectionPoolAPI, ConnectionAPI], Awaitable[Any]]
OnDisconnectFn = OnConnectFn


async def _default_sleep_fn(manager: 'PoolManager') -> None:
    return


class PoolChanged(PoolChangedAPI):
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


class PoolManager(PoolManagerAPI):
    _on_connect_handlers: Set[OnConnectFn]
    _on_disconnect_handlers: Set[OnDisconnectFn]
    _behaviors: Set[BehaviorAPI]

    def __init__(self,
                 pool: ConnectionPool,
                 private_key: keys.PrivateKey,
                 p2p_handshake_params: DevP2PHandshakeParams,
                 handshaker_providers: Sequence[HandshakerProviderFn],
                 token: CancelToken) -> None:
        super().__init__(token=token)
        self.pool = pool
        self._private_key = private_key
        self.p2p_handshake_params = p2p_handshake_params
        self.handshaker_providers = handshaker_providers
        self._pool_changed = asyncio.Condition()

        self._on_connect_handlers = set()
        self._on_disconnect_handlers = set()
        self._behaviors = set()

        self._handshake_locks = ResourceLock()

    @property
    def public_key(self) -> keys.PublicKey:
        return self._private_key.public_key

    async def _run(self) -> None:
        try:
            await self.cancellation()
        except asyncio.CancelledError:
            pass

    def wait_pool_changed(self) -> PoolChangedAPI:
        return PoolChanged(self._pool_changed)

    def on_connect(self, handler_fn: OnConnectFn) -> HandlerSubscriptionAPI:
        self._on_connect_handlers.add(handler_fn)
        cancel_fn = functools.partial(self._on_connect_handlers.remove, handler_fn)
        return HandlerSubscription(cancel_fn)

    def on_disconnect(self, handler_fn: OnDisconnectFn) -> HandlerSubscriptionAPI:
        self._on_disconnect_handlers.add(handler_fn)
        cancel_fn = functools.partial(self._on_disconnect_handlers.remove, handler_fn)
        return HandlerSubscription(cancel_fn)

    def add_behavior(self, behavior: BehaviorAPI) -> HandlerSubscription:
        self._behaviors.add(behavior)
        cancel_fn = functools.partial(self._behaviors.remove, behavior)
        return HandlerSubscription(cancel_fn)

    @asynccontextmanager
    async def listen(self, host: str, port: int) -> AsyncIterator[NodeAPI]:
        server = await asyncio.start_server(
            self.receive_handshake,
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
            if server.connections:  # type: ignore
                await server.wait_closed()

    async def seek_connections(self,
                               providers: Sequence[PeerProviderFn],
                               candidate_filters: Sequence[CandidateFilterFn] = (),
                               sleep_fn: Callable[['PoolManager'], Awaitable[Any]] = _default_sleep_fn,  # noqa: E501
                               ) -> None:
        while self.is_operational:
            # The `sleep_fn` allows
            await sleep_fn(self)
            for provider in providers:
                candidates = await provider(self.pool)
                # TODO: enforce NodeAPI based filters
                for remote in candidates:
                    if not all(filter_fn(self.pool, remote) for filter_fn in candidate_filters):
                        continue

                    # TODO: locking to ensure concurrent dials aren't a problem
                    try:
                        await self.dial(remote)
                    except EXPECTED_DIAL_ERRORS:
                        continue

    async def dial(self, remote: NodeAPI) -> ConnectionAPI:
        if self._handshake_locks.is_locked(remote):
            self.logger.debug2("Skipping %s; already shaking hands", remote)
            raise IneligiblePeer(f"Already shaking hands with {remote}")

        async with self._handshake_locks(remote):
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
            self.logger.info('Established dial-out connection with: %s', connection.remote)
            self.run_task(self._run_connection(connection))
            await connection.events.started.wait()
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

            # Apply behaviors to the connection
            with contextlib.ExitStack() as stack:
                for behavior in self._behaviors:
                    if behavior.applies_to(connection):
                        stack.enter_context(behavior.apply(connection))

                # Start the protocol streams (this must occur after behaviors
                # have been applied)
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

    async def receive_handshake(self,
                                reader: asyncio.StreamReader,
                                writer: asyncio.StreamWriter) -> None:
        # TODO: locking to ensure concurrent dial/receive are not a problem
        handshakers = await asyncio.gather(*(
            provider() for provider in self.handshaker_providers
        ))

        def _cleanup_reader_and_writer() -> None:
            if not reader.at_eof():
                reader.feed_eof()
            writer.close()

        try:
            connection = await receive_dial_in(
                reader=reader,
                writer=writer,
                private_key=self._private_key,
                p2p_handshake_params=self.p2p_handshake_params,
                protocol_handshakers=handshakers,
                token=self.cancel_token,
            )
            self.logger.info('Established dial-in connection with: %s', connection.remote)
        except EXPECTED_RECEIVE_ERRORS as e:
            self.logger.debug("Could not complete handshake: %s", e)
            _cleanup_reader_and_writer()
        except Exception as e:
            self.logger.exception("Unexpected error handling handshake")
            _cleanup_reader_and_writer()
        else:
            if self._handshake_locks.is_locked(connection.remote):
                self.logger.debug2("Skipping %s; already shaking hands", connection.remote)
                raise IneligiblePeer(f"Already shaking hands with {connection.remote}")

            async with self._handshake_locks(connection.remote):
                self.run_task(self._run_connection(connection))
                await connection.events.started.wait()


async def sleep_till_not_full(manager: PoolManager,
                              *,
                              max_connections: int) -> None:
    if len(manager.pool) < max_connections:
        return
    while True:
        await manager.wait_pool_changed()
        if len(manager.pool) < max_connections:
            return


async def enforce_dial_in_to_out_ratio(pool: ConnectionPool,
                                       connection: ConnectionAPI,
                                       *,
                                       ratio: float) -> None:
    # we count the incoming connection too since it shouldn't be in the pool
    # yet.
    if connection.is_dial_out:
        connection.logger.info('IGNORING DIAL OUT CONNECTION: %s', connection.remote)
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
        connection.logger.info('DISCONNECTING: %s', connection.remote)
    else:
        connection.logger.info('ACCEPTING: %s / %s', dial_in_count, total_connections)


async def enforce_max_connections(pool: ConnectionPool,
                                  connection: ConnectionAPI,
                                  *,
                                  max_connections: int) -> None:
    if len(pool) >= max_connections:
        connection.get_base_protocol().send_disconnect(DisconnectReason.too_many_peers)
