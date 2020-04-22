import asyncio
from contextlib import contextmanager
from typing import (
    AsyncIterator,
    Iterator,
    Set,
)

from async_service import Service

from eth_utils import ValidationError

from p2p.exceptions import NoConnectedPeers
from p2p.peer import BasePeer, PeerSubscriber

from trinity.protocol.common.peer import BaseChainPeer, BaseChainPeerPool
from trinity._utils.logging import get_logger


class BaseChainTipMonitor(Service, PeerSubscriber):
    """
    Monitor for potential changes to the tip of the chain: a new peer or a new block

    Subclass must specify :attr:`subscription_msg_types`
    """
    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize = 2000

    def __init__(self, peer_pool: BaseChainPeerPool) -> None:
        self.logger = get_logger('trinity.protocol.common.monitors.BaseChainTipMonitor')
        self._peer_pool = peer_pool
        # There is one event for each subscriber, each one gets set any time new tip info arrives
        self._subscriber_notices: Set[asyncio.Event] = set()

    async def wait_tip_info(self) -> AsyncIterator[BaseChainPeer]:
        """
        This iterator waits until there is potentially new tip information.
        New tip information means a new peer connected or a new block arrived.
        Then it yields the peer with the highest total difficulty.
        It continues indefinitely, until this service is cancelled.
        """
        if self.manager.is_cancelled:
            raise ValidationError("%s is cancelled, new tip info is impossible", self)
        elif not self.manager.is_running:
            await self.manager.wait_started()

        with self._subscriber() as new_tip_event:
            while self.manager.is_running:
                try:
                    highest_td_peer = self._peer_pool.highest_td_peer
                except NoConnectedPeers:
                    # no peers are available right now, skip the new tip info yield
                    pass
                else:
                    yield highest_td_peer

                await new_tip_event.wait()
                new_tip_event.clear()

    def register_peer(self, peer: BasePeer) -> None:
        self._notify_tip()

    async def _handle_msg_loop(self) -> None:
        new_tip_types = tuple(self.subscription_msg_types)
        while self.manager.is_running:
            peer, cmd = await self.msg_queue.get()
            if isinstance(cmd, new_tip_types):
                self._notify_tip()

    def _notify_tip(self) -> None:
        for new_tip_event in self._subscriber_notices:
            new_tip_event.set()

    async def run(self) -> None:
        self.manager.run_daemon_task(self._handle_msg_loop)
        with self.subscribe(self._peer_pool):
            await self.manager.wait_finished()

    @contextmanager
    def _subscriber(self) -> Iterator[asyncio.Event]:
        new_tip_event = asyncio.Event()
        self._subscriber_notices.add(new_tip_event)
        try:
            yield new_tip_event
        finally:
            self._subscriber_notices.remove(new_tip_event)
