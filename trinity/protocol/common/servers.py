from abc import abstractmethod
from typing import (
    AsyncIterator,
    Iterable,
    Tuple,
    Type,
)

from lahja import EndpointAPI

from eth_utils import get_extended_debug_logger

from eth.exceptions import (
    HeaderNotFound,
)
from eth_typing import (
    BlockNumber,
)
from eth.rlp.headers import BlockHeader
from lahja import (
    BroadcastConfig,
)

from p2p.abc import CommandAPI, SessionAPI
from p2p.peer import (
    BasePeer,
    PeerSubscriber,
)
from p2p.typing import Payload
from p2p.service import Service

from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.common.events import PeerPoolMessageEvent
from trinity.protocol.common.peer import BasePeerPool
from trinity.protocol.common.requests import BaseHeaderRequest


class BaseRequestServer(Service, PeerSubscriber):
    """
    Monitor commands from peers, to identify inbound requests that should receive a response.
    Handle those inbound requests by querying our local database and replying.
    """
    logger = get_extended_debug_logger('trinity.protocol.common.servers.RequestServer')

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize = 2000

    def __init__(
            self,
            peer_pool: BasePeerPool) -> None:
        self._peer_pool = peer_pool

    async def run(self) -> None:
        self.manager.run_daemon_task(self._handle_msg_loop)
        with self.subscribe(self._peer_pool):
            await self.manager.wait_forever()

    async def _handle_msg_loop(self) -> None:
        while self.manager.is_running:
            peer, cmd, msg = await self.msg_queue.get()
            self.manager.run_task(self._quiet_handle_msg, peer, cmd, msg)

    async def _quiet_handle_msg(
            self,
            peer: BasePeer,
            cmd: CommandAPI,
            msg: Payload) -> None:
        try:
            await self._handle_msg(peer, cmd, msg)
        except Exception:
            self.logger.exception("Unexpected error when processing msg from %s", peer)

    @abstractmethod
    async def _handle_msg(self, peer: BasePeer, cmd: CommandAPI, msg: Payload) -> None:
        """
        Identify the command, and react appropriately.
        """
        ...


class BaseIsolatedRequestServer(Service):
    """
    Monitor commands from peers, to identify inbound requests that should receive a response.
    Handle those inbound requests by querying our local database and replying.
    """
    logger = get_extended_debug_logger('trinity.protocol.common.servers.IsolatedRequestServer')

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

        await self.manager.wait_forever()

    async def handle_stream(self, event_type: Type[PeerPoolMessageEvent]) -> None:
        while self.manager.is_running:
            async for event in self.event_bus.stream(event_type):
                self.manager.run_task(self._quiet_handle_msg, event.session, event.cmd, event.msg)

    async def _quiet_handle_msg(
            self,
            session: SessionAPI,
            cmd: CommandAPI,
            msg: Payload) -> None:
        try:
            await self._handle_msg(session, cmd, msg)
        except Exception:
            self.logger.exception("Unexpected error when processing msg from %s", session)

    @abstractmethod
    async def _handle_msg(self,
                          session: SessionAPI,
                          cmd: CommandAPI,
                          msg: Payload) -> None:
        ...


class BasePeerRequestHandler:
    logger = get_extended_debug_logger('trinity.protocol.common.servers.PeerRequestHandler')

    def __init__(self, db: BaseAsyncHeaderDB) -> None:
        self.db = db

    async def lookup_headers(self,
                             request: BaseHeaderRequest) -> Tuple[BlockHeader, ...]:
        """
        Lookup :max_headers: headers starting at :block_number_or_hash:, skipping :skip: items
        between each, in reverse order if :reverse: is True.
        """
        try:
            block_numbers = await self._get_block_numbers_for_request(request)
        except HeaderNotFound:
            self.logger.debug(
                "Peer requested starting header %r that is unavailable, returning nothing",
                request.block_number_or_hash)
            return tuple()

        headers: Tuple[BlockHeader, ...] = tuple([
            header
            async for header
            in self._generate_available_headers(block_numbers)
        ])
        return headers

    async def _get_block_numbers_for_request(self,
                                             request: BaseHeaderRequest,
                                             ) -> Tuple[BlockNumber, ...]:
        """
        Generate the block numbers for a given `HeaderRequest`.
        """
        if isinstance(request.block_number_or_hash, bytes):
            header = await self.db.coro_get_block_header_by_hash(
                request.block_number_or_hash,
            )
            return request.generate_block_numbers(header.block_number)
        elif isinstance(request.block_number_or_hash, int):
            # We don't need to pass in the block number to
            # `generate_block_numbers` since the request is based on a numbered
            # block identifier.
            return request.generate_block_numbers()
        else:
            actual_type = type(request.block_number_or_hash)
            raise TypeError(f"Invariant: unexpected type for 'block_number_or_hash': {actual_type}")

    async def _generate_available_headers(
            self, block_numbers: Tuple[BlockNumber, ...]) -> AsyncIterator[BlockHeader]:
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
