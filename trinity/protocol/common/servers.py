from abc import abstractmethod
import asyncio
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    Tuple,
    Type,
)

from async_service import Service
from eth.abc import BlockHeaderAPI
from lahja import EndpointAPI

from eth_typing import Hash32

from eth.exceptions import (
    HeaderNotFound,
)
from eth_typing import (
    BlockNumber,
)
from lahja import (
    BroadcastConfig,
)

from p2p.abc import CommandAPI, SessionAPI

from trinity._utils.headers import sequence_builder
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.common.payloads import BlockHeadersQuery
from trinity._utils.logging import get_logger

from .events import PeerPoolMessageEvent


class BaseIsolatedRequestServer(Service):
    """
    Monitor commands from peers, to identify inbound requests that should receive a response.
    Handle those inbound requests by querying our local database and replying.
    """
    logger = get_logger('trinity.protocol.common.servers.IsolatedRequestServer')

    def __init__(
            self,
            event_bus: EndpointAPI,
            broadcast_config: BroadcastConfig,
            subscribed_events: Iterable[Type[PeerPoolMessageEvent]]) -> None:
        self.event_bus = event_bus
        self.broadcast_config = broadcast_config
        self._subscribed_events = subscribed_events

    async def run(self) -> None:
        for event_type in self._subscribed_events:
            self.manager.run_daemon_task(self.handle_stream, event_type)

        await self.manager.wait_finished()

    async def handle_stream(self, event_type: Type[PeerPoolMessageEvent]) -> None:
        while self.manager.is_running:
            async for event in self.event_bus.stream(event_type):
                self.manager.run_task(self._quiet_handle_msg, event.session, event.command)

    async def _quiet_handle_msg(
            self,
            session: SessionAPI,
            cmd: CommandAPI[Any]) -> None:
        try:
            await self._handle_msg(session, cmd)
        except asyncio.CancelledError:
            # catch and re-raise to avoid reporting via the except below and
            # treated as unexpected.
            raise
        except Exception:
            self.logger.exception("Unexpected error when processing msg from %s", session)

    @abstractmethod
    async def _handle_msg(self,
                          session: SessionAPI,
                          cmd: CommandAPI[Any]) -> None:
        ...


class BasePeerRequestHandler:
    logger = get_logger('trinity.protocol.common.servers.PeerRequestHandler')

    def __init__(self, db: BaseAsyncHeaderDB) -> None:
        self.db = db

    async def lookup_headers(self,
                             query: BlockHeadersQuery) -> Tuple[BlockHeaderAPI, ...]:
        """
        Lookup :max_headers: headers starting at :block_number_or_hash:, skipping :skip: items
        between each, in reverse order if :reverse: is True.
        """
        try:
            block_numbers = await self._get_block_numbers_for_query(query)
        except HeaderNotFound:
            self.logger.debug(
                "Peer requested starting header %r that is unavailable, returning nothing",
                query.block_number_or_hash)
            return tuple()

        headers: Tuple[BlockHeaderAPI, ...] = tuple([
            header
            async for header
            in self._generate_available_headers(block_numbers)
        ])
        return headers

    async def _get_block_numbers_for_query(self,
                                           query: BlockHeadersQuery,
                                           ) -> Tuple[BlockNumber, ...]:
        """
        Generate the block numbers for a given `HeaderRequest`.
        """
        if isinstance(query.block_number_or_hash, bytes):
            header = await self.db.coro_get_block_header_by_hash(Hash32(query.block_number_or_hash))
            start_number = header.block_number
        elif isinstance(query.block_number_or_hash, int):
            start_number = query.block_number_or_hash
        else:
            actual_type = type(query.block_number_or_hash)
            raise TypeError(f"Invariant: unexpected type for 'block_number_or_hash': {actual_type}")

        return sequence_builder(
            start_number=start_number,
            max_length=query.max_headers,
            skip=query.skip,
            reverse=query.reverse,
        )

    async def _generate_available_headers(
            self, block_numbers: Tuple[BlockNumber, ...]) -> AsyncIterator[BlockHeaderAPI]:
        """
        Generates the headers requested, halting on the first header that is not locally available.
        """
        for block_num in block_numbers:
            try:
                yield await self.db.coro_get_canonical_block_header_by_number(block_num)
            except HeaderNotFound:
                self.logger.debug(
                    "Peer requested header number %s that is unavailable, stopping search.",
                    block_num,
                )
                break
