from abc import abstractmethod
import contextlib
import logging
from typing import (
    cast,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Type,
)

from p2p.abc import (
    BehaviorAPI,
    CommandAPI,
    CommandHandlerFn,
    ConnectionAPI,
)
from p2p.disconnect import DisconnectReason
from p2p.p2p_proto import Disconnect, Ping
from p2p.typing import Payload


class CommandHandler(BehaviorAPI):
    """
    Base class to reduce boilerplate for Behaviors that want to register a
    handler against a single command.
    """
    cmd_type: Type[CommandAPI]
    logger = logging.getLogger('p2p.behaviors.CommandHandler')

    def applies_to(self, connection: ConnectionAPI) -> bool:
        return any(
            protocol.supports_command(self.cmd_type)
            for protocol
            in connection.get_multiplexer().get_protocols()
        )

    @contextlib.contextmanager
    def apply(self, connection: ConnectionAPI) -> Iterator[None]:
        if self.cmd_type is None:
            raise TypeError(f"No cmd_type specified for {self}")

        with connection.add_command_handler(self.cmd_type, self.handle):
            yield

    @abstractmethod
    async def handle(self, connection: ConnectionAPI, msg: Payload) -> None:
        ...


def command_handler(command_type: Type[CommandAPI],
                    *,
                    name: str = None) -> Callable[[CommandHandlerFn], Type[CommandHandler]]:
    """
    Decorator that can be used to construct a CommandHandler from a simple
    function.
    """
    if name is None:
        name = f'handle_{command_type.__name__}'

    def decorator(fn: CommandHandlerFn) -> Type[CommandHandler]:
        return type(
            name,
            (CommandHandler,),
            {
                'cmd_type': command_type,
                'handle': staticmethod(fn),
            },
        )()
    return decorator


class Application(BehaviorAPI):
    """
    A collection of behaviors.
    """
    _behaviors: List[BehaviorAPI]

    def __init__(self, name: str) -> None:
        self.name = name
        self._behaviors = []

    def register(self, behavior: BehaviorAPI) -> None:
        self._behaviors.append(behavior)

    def applies_to(self, connection: ConnectionAPI) -> bool:
        return any(
            behavior.applies_to(connection)
            for behavior
            in self._behaviors
        )

    @contextlib.contextmanager
    def apply(self, connection: ConnectionAPI) -> Iterator[None]:
        with contextlib.ExitStack() as stack:
            for behavior in self._behaviors:
                if behavior.applies_to(connection):
                    stack.enter_context(behavior.apply(connection))
            yield


class PingPongBehavior(CommandHandler):
    cmd_type = Ping

    async def handle(self, connection: ConnectionAPI, msg: Payload) -> None:
        connection.get_base_protocol().send_pong()


class DisconnectBehavior(CommandHandler):
    cmd_type = Disconnect
    disconnect_reason: DisconnectReason = None

    async def handle(self, connection: ConnectionAPI, msg: Payload) -> None:
        msg = cast(Dict[str, Any], msg)
        try:
            reason = DisconnectReason(msg['reason'])
        except TypeError:
            self.logger.info('Unrecognized reason: %s', msg['reason_name'])
        else:
            self.disconnect_reason = reason

        connection.cancel_nowait()


base_protocol_app = Application('p2p-protocol')
base_protocol_app.register(PingPongBehavior())
base_protocol_app.register(DisconnectBehavior())
