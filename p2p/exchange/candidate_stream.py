import asyncio
import logging
import time
from typing import (
    Any,
    AsyncIterator,
    Tuple,
    Type,
)

from async_service import Service

from p2p.abc import (
    ConnectionAPI,
    ProtocolAPI,
)
from p2p.exceptions import (
    ConnectionBusy,
)

from .abc import (
    PerformanceTrackerAPI,
    ResponseCandidateStreamAPI,
)
from .constants import (
    ROUND_TRIP_TIMEOUT,
    NUM_QUEUED_REQUESTS,
)
from .typing import (
    TRequestCommand,
    TResponseCommand,
)


class ResponseCandidateStream(
        ResponseCandidateStreamAPI[TRequestCommand, TResponseCommand],
        Service):
    logger = logging.getLogger('p2p.exchange.ResponseCandidateStream')

    response_timeout: float = ROUND_TRIP_TIMEOUT

    _pending_request: Tuple[float, 'asyncio.Queue[TResponseCommand]'] = None

    def __init__(
            self,
            connection: ConnectionAPI,
            request_protocol: ProtocolAPI,
            response_cmd_type: Type[TResponseCommand]) -> None:
        # This style of initialization keeps `mypy` happy.
        self._connection = connection
        self.request_protocol = request_protocol
        self.response_cmd_type = response_cmd_type
        self._lock = asyncio.Lock()

    def __repr__(self) -> str:
        return f'<ResponseCandidateStream({self._connection!s}, {self.response_cmd_type!r})>'

    @property
    def is_alive(self) -> bool:
        return self.manager.is_running

    async def payload_candidates(
            self,
            request: TRequestCommand,
            tracker: PerformanceTrackerAPI[TRequestCommand, Any],
            *,
            timeout: float = None) -> AsyncIterator[TResponseCommand]:
        """
        Make a request and iterate through candidates for a valid response.

        To mark a response as valid, use `complete_request`. After that call, payload
        candidates will stop arriving.
        """
        total_timeout = self.response_timeout if timeout is None else timeout

        # The _lock ensures that we never have two concurrent requests to a
        # single peer for a single command pair in flight.
        try:
            await asyncio.wait_for(
                self._lock.acquire(),
                timeout=total_timeout * NUM_QUEUED_REQUESTS,
            )
        except asyncio.TimeoutError:
            raise ConnectionBusy(
                f"Timed out waiting for {self.response_cmd_name} request lock "
                f"or connection: {self._connection}"
            )

        start_at = time.perf_counter()

        try:
            queue = self._request(request)
            while self.is_pending:
                timeout_remaining = max(0, total_timeout - (time.perf_counter() - start_at))

                try:
                    yield await asyncio.wait_for(queue.get(), timeout=timeout_remaining)
                except asyncio.TimeoutError:
                    tracker.record_timeout(total_timeout)
                    raise
        finally:
            self._lock.release()

    @property
    def response_cmd_name(self) -> str:
        return self.response_cmd_type.__name__

    def complete_request(self) -> None:
        if self._pending_request is None:
            self.logger.warning("`complete_request` was called when there was no pending request")
        self._pending_request = None

    #
    # Service API
    #
    async def run(self) -> None:
        self.logger.debug("Launching %r", self)

        # mypy doesn't recognizet the `TResponseCommand` as being an allowed
        # variant of the expected `Payload` type.
        with self._connection.add_command_handler(self.response_cmd_type, self._handle_msg):  # type: ignore  # noqa: E501
            await self.manager.wait_finished()

    async def _handle_msg(self, connection: ConnectionAPI, cmd: TResponseCommand) -> None:
        if self._pending_request is None:
            self.logger.debug(
                "Got unexpected %s payload from %s", self.response_cmd_name, self._connection
            )
            return

        send_time, queue = self._pending_request
        self.last_response_time = time.perf_counter() - send_time
        queue.put_nowait(cmd)

    def _request(self, request: TRequestCommand) -> 'asyncio.Queue[TResponseCommand]':
        if not self._lock.locked():
            # This is somewhat of an invariant check but since there the
            # linkage between the lock and this method are loose this sanity
            # check seems appropriate.
            raise Exception("Invariant: cannot issue a request without an acquired lock")

        # TODO: better API for getting at the protocols from the connection....
        self.request_protocol.send(request)

        queue: 'asyncio.Queue[TResponseCommand]' = asyncio.Queue()
        self._pending_request = (time.perf_counter(), queue)
        return queue

    @property
    def is_pending(self) -> bool:
        return self._pending_request is not None
