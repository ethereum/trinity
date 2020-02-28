from cached_property import cached_property

from p2p.abc import ConnectionAPI
from p2p.logic import Application, CommandHandler
from p2p.disconnect import DisconnectReason
from p2p.p2p_proto import Disconnect, Ping, Pong
from p2p.qualifiers import always
from p2p._utils import get_logger


class PongWhenPinged(CommandHandler[Ping]):
    """
    Sends a `Pong` message anytime a `Ping` message is received.
    """
    command_type = Ping

    async def handle(self, connection: ConnectionAPI, cmd: Ping) -> None:
        connection.get_base_protocol().send(Pong(None))


class P2PAPI(Application):
    name = 'p2p'
    qualifier = always  # always valid for all connections.

    local_disconnect_reason: DisconnectReason = None
    remote_disconnect_reason: DisconnectReason = None

    def __init__(self) -> None:
        self.logger = get_logger('p2p.p2p_api.P2PAPI')
        self.add_child_behavior(PongWhenPinged().as_behavior())

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
        self.connection.get_base_protocol().send(Ping(None))

    def send_pong(self) -> None:
        self.connection.get_base_protocol().send(Pong(None))

    def send_disconnect(self, reason: DisconnectReason) -> None:
        self.connection.get_base_protocol().send(Disconnect(reason))
