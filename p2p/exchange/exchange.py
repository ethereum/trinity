from __future__ import annotations
import asyncio
from functools import partial
import contextlib
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Type,
)

from async_service import background_asyncio_service

from p2p.abc import ConnectionAPI
from p2p.asyncio_utils import create_task

from .abc import ExchangeAPI, NormalizerAPI, ValidatorAPI
from .candidate_stream import ResponseCandidateStream
from .manager import ExchangeManager
from .typing import TResult, TRequestCommand, TResponseCommand


class BaseExchange(ExchangeAPI[TRequestCommand, TResponseCommand, TResult]):
    _request_command_type: Type[TRequestCommand]
    _response_command_type: Type[TResponseCommand]

    _manager: ExchangeManager[TRequestCommand, TResponseCommand, TResult]

    def __init__(self) -> None:
        self.tracker = self.tracker_class()

    @contextlib.asynccontextmanager
    async def run_exchange(
            self, connection: ConnectionAPI) -> AsyncIterator[asyncio.Task[Any]]:
        protocol = connection.get_protocol_for_command_type(self.get_request_cmd_type())

        response_stream: ResponseCandidateStream[TRequestCommand, TResponseCommand] = ResponseCandidateStream(  # noqa: E501
            connection,
            protocol,
            self.get_response_cmd_type(),
        )
        async with background_asyncio_service(response_stream) as response_stream_manager:
            self._manager = ExchangeManager(
                connection,
                response_stream,
            )
            name = f'{self.__class__.__name__}/{connection.remote}'
            yield create_task(response_stream_manager.wait_finished(), name=name)

    async def get_result(
            self,
            request: TRequestCommand,
            normalizer: NormalizerAPI[TResponseCommand, TResult],
            result_validator: ValidatorAPI[TResult],
            payload_validator: Callable[[TRequestCommand, TResponseCommand], None],
            timeout: float = None) -> TResult:
        """
        This is a light convenience wrapper around the ExchangeManager's get_result() method.

        It makes sure that:
        - the manager service is running
        - the payload validator is primed with the request payload
        """
        # bind the outbound request payload to the payload validator
        message_validator = partial(payload_validator, request.payload)

        return await self._manager.get_result(
            request,
            normalizer,
            result_validator.validate_result,
            message_validator,
            self.tracker,
            timeout,
        )

    @classmethod
    def get_response_cmd_type(cls) -> Type[TResponseCommand]:
        return cls._response_command_type

    @classmethod
    def get_request_cmd_type(cls) -> Type[TRequestCommand]:
        return cls._request_command_type

    @property
    def is_requesting(self) -> bool:
        return self._manager.is_requesting
