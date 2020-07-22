from __future__ import annotations
import asyncio
import contextlib
from typing import Any, AsyncIterator, cast

from cached_property import cached_property

from async_service import (
    background_asyncio_service,
    Service,
)

from p2p.abc import CommandAPI, ConnectionAPI, HandlerFn
from p2p.asyncio_utils import create_task
from p2p import constants
from p2p.disconnect import DisconnectReason
from p2p.exceptions import PeerConnectionLost
from p2p.logic import Application, BaseLogic, CommandHandler
from p2p.p2p_proto import Disconnect, Ping, Pong
from p2p.qualifiers import always
from p2p._utils import get_logger


class PongWhenPinged(CommandHandler[Ping]):
    """
    Sends a `Pong` message anytime a `Ping` message is received.
    """
    command_type = Ping

    async def handle(self, connection: ConnectionAPI, cmd: Ping) -> None:
        connection.logger.debug2("Received ping on %s, replying with pong", connection)
        try:
            connection.get_base_protocol().send(Pong(None))
        except PeerConnectionLost:
            connection.logger.debug2("%s disconnected while sending pong", connection)


class PingAndDisconnectIfIdle(Service):

    def __init__(self, connection: ConnectionAPI, idle_timeout: float) -> None:
        self.connection = connection
        self.idle_timeout = idle_timeout

    async def run(self) -> None:
        msg_received = asyncio.Event()

        async def set_msg_received(connection: ConnectionAPI, cmd: CommandAPI[Any]) -> None:
            msg_received.set()

        conn = self.connection
        half_timeout = self.idle_timeout / 2
        with conn.add_msg_handler(cast(HandlerFn, set_msg_received)):
            while conn.get_manager().is_running:
                try:
                    await asyncio.wait_for(msg_received.wait(), timeout=half_timeout)
                    conn.logger.debug2("Received msg on %s, restarting idle monitor", conn)
                    msg_received.clear()
                    continue
                except asyncio.TimeoutError:
                    pass

                try:
                    _send_ping(conn)
                except PeerConnectionLost:
                    conn.logger.debug(
                        "Lost peer connection to %s while sending ping to check idle connection",
                        conn,
                    )
                    conn.get_manager().cancel()
                    return

                try:
                    await asyncio.wait_for(msg_received.wait(), timeout=half_timeout)
                    conn.logger.debug2("Received msg on %s, restarting idle monitor", conn)
                    msg_received.clear()
                    continue
                except asyncio.TimeoutError:
                    pass

                conn.logger.info(
                    "Reached idle limit (%.2f) on %s, disconnecting", half_timeout * 2, conn)
                conn.get_manager().cancel()
                return


class DisconnectIfIdle(BaseLogic):
    """
    Cancels the connection if we receive no messages on it for CONN_IDLE_TIMEOUT seconds.

    After CONN_IDLE_TIMEOUT/2 seconds without receiving any messages, we send a ping. If after
    CONN_IDLE_TIMEOUT/2 we still haven't received any messages, cancel the connection.
    """
    qualifier = always  # always valid for all connections.

    def __init__(self, idle_timeout: float) -> None:
        self.idle_timeout = idle_timeout

    @contextlib.asynccontextmanager
    async def apply(self, connection: ConnectionAPI) -> AsyncIterator[asyncio.Task[Any]]:
        service = PingAndDisconnectIfIdle(connection, self.idle_timeout)
        async with background_asyncio_service(service) as manager:
            task_name = f'PingAndDisconnectIfIdleService/{connection.remote}'
            yield create_task(manager.wait_finished(), name=task_name)


class P2PAPI(Application):
    name = 'p2p'
    qualifier = always  # always valid for all connections.

    local_disconnect_reason: DisconnectReason = None
    remote_disconnect_reason: DisconnectReason = None

    def __init__(self) -> None:
        self.logger = get_logger('p2p.p2p_api.P2PAPI')
        self.add_child_behavior(PongWhenPinged().as_behavior())
        self.add_child_behavior(DisconnectIfIdle(constants.CONN_IDLE_TIMEOUT).as_behavior())

    #
    # Properties from handshake
    #
    @cached_property
    def safe_client_version_string(self) -> str:
        return self.connection.safe_client_version_string

    @cached_property
    def client_version_string(self) -> str:
        return self.connection.client_version_string

    #
    # Disconnect API
    #
    def disconnect(self, reason: DisconnectReason) -> None:
        self.logger.debug(
            "Sending Disconnect to remote peer %s; reason: %s",
            self.connection,
            reason.name,
        )
        self.send_disconnect(reason)
        self.local_disconnect_reason = reason

    #
    # Sending Pings
    #
    def send_ping(self) -> None:
        _send_ping(self.connection)

    def send_pong(self) -> None:
        self.connection.get_base_protocol().send(Pong(None))

    def send_disconnect(self, reason: DisconnectReason) -> None:
        self.logger.debug2("Sending Disconnect on %s", self.connection)
        self.connection.get_base_protocol().send(Disconnect(reason))


def _send_ping(connection: ConnectionAPI) -> None:
    connection.get_base_protocol().send(Ping(None))
