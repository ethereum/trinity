import asyncio
from concurrent import futures
import logging
from typing import (
    Callable,
    TypeVar,
)

from eth_utils import (
    ValidationError,
)

from p2p.abc import ConnectionAPI

from p2p.exceptions import PeerConnectionLost

from .abc import (
    ExchangeManagerAPI,
    NormalizerAPI,
    PerformanceTrackerAPI,
    ResponseCandidateStreamAPI,
)
from .typing import TRequestCommand, TResponseCommand


TResult = TypeVar('TResult')


class ExchangeManager(ExchangeManagerAPI[TRequestCommand, TResponseCommand, TResult]):
    logger = logging.getLogger('p2p.exchange.ExchangeManager')

    _response_stream: ResponseCandidateStreamAPI[TRequestCommand, TResponseCommand] = None

    def __init__(self,
                 connection: ConnectionAPI,
                 response_stream: ResponseCandidateStreamAPI[TRequestCommand, TResponseCommand],
                 ) -> None:
        self._connection = connection
        self._response_stream = response_stream

    async def get_result(
            self,
            request: TRequestCommand,
            normalizer: NormalizerAPI[TResponseCommand, TResult],
            validate_result: Callable[[TResult], None],
            payload_validator: Callable[[TResponseCommand], None],
            tracker: PerformanceTrackerAPI[TRequestCommand, TResult],
            timeout: float = None) -> TResult:

        stream = self._response_stream
        if not stream.is_alive:
            raise PeerConnectionLost(
                f"Response stream closed before sending request to {self._connection}"
            )

        loop = asyncio.get_event_loop()

        with futures.ThreadPoolExecutor() as executor:
            async for payload in stream.payload_candidates(request, tracker, timeout=timeout):
                try:
                    payload_validator(payload)

                    if normalizer.is_normalization_slow:
                        result = await loop.run_in_executor(
                            executor,
                            normalizer.normalize_result,
                            payload
                        )
                    else:
                        result = normalizer.normalize_result(payload)

                    validate_result(result)
                except ValidationError as err:
                    self.logger.debug(
                        "Response validation failed for pending %s request from connection %s: %s",
                        stream.response_cmd_name,
                        self._connection,
                        err,
                    )
                    # If this response was just for the wrong request, we'll
                    # catch the right one later.  Otherwise, this request will
                    # eventually time out.
                    continue
                else:
                    tracker.record_response(
                        stream.last_response_time,
                        request,
                        result,
                    )
                    stream.complete_request()
                    return result

        raise PeerConnectionLost(f"Response stream of {self._connection} was apparently closed")

    @property
    def service(self) -> ResponseCandidateStreamAPI[TRequestCommand, TResponseCommand]:
        """
        This service that needs to be running for calls to execute properly
        """
        return self._response_stream

    @property
    def is_requesting(self) -> bool:
        return self._response_stream is not None and self._response_stream.is_pending
