from typing import Any, AsyncIterator, TypeVar

from async_generator import asynccontextmanager

from p2p.abc import ConnectionAPI, LogicAPI
from p2p.exceptions import UnknownProtocol
from p2p.logic import BaseLogic

from .abc import ExchangeAPI


TExchange = TypeVar("TExchange", bound=ExchangeAPI[Any, Any, Any])


class ExchangeLogic(BaseLogic):
    """
    A thin wrapper around an exchange which handles running the services and
    checking whether it's applicable to the connection
    """
    exchange: TExchange

    def __init__(self, exchange: TExchange) -> None:
        self.exchange = exchange

    def qualifier(self, connection: ConnectionAPI, logic: LogicAPI) -> bool:
        try:
            protocol = connection.get_protocol_for_command_type(
                self.exchange.get_request_cmd_type()
            )
        except UnknownProtocol:
            return False
        else:
            return protocol.supports_command(self.exchange.get_response_cmd_type())

    @asynccontextmanager
    async def __call__(self, connection: ConnectionAPI) -> AsyncIterator[None]:
        async with self.exchange.run_exchange(connection):
            yield
