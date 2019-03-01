import asyncio
from contextlib import contextmanager
from typing import (
    AsyncIterator,
    Iterator,
    Set,
)

from cancel_token import CancelToken
from eth_utils import ValidationError

from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.endpoint import TrinityEventBusEndpoint
from trinity.protocol.common.events import PeerPoolMessageEvent
from trinity.protocol.common.peer import BaseChainPeer
from trinity.protocol.eth.peer import ETHProxyPeerPool


class BaseChainTipMonitor(BaseService, PeerSubscriber):
    """
    Monitor for potential changes to the tip of the chain: a new peer or a new block

    Subclass must specify :attr:`subscription_msg_types`
    """
    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize = 2000

    def __init__(
            self,
            event_bus: TrinityEventBusEndpoint,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._event_bus = event_bus
        # TODO: This should be a BaseProxyPeerPool and it should be passed in
        self._proxy_peer_pool = ETHProxyPeerPool(event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        # There is one event for each subscriber, each one gets set any time new tip info arrives
        self._subscriber_notices: Set[asyncio.Event] = set()

    async def wait_tip_info(self) -> AsyncIterator[BaseChainPeer]:
        """
        This iterator waits until there is potentially new tip information.
        New tip information means a new peer connected or a new block arrived.
        Then it yields the peer with the highest total difficulty.
        It continues indefinitely, until this service is cancelled.
        """
        if self.is_cancelled:
            raise ValidationError("%s is cancelled, new tip info is impossible", self)
        elif not self.is_running:
            await self.events.started.wait()

        with self._subscriber() as new_tip_event:
            while self.is_operational:
                try:
                    highest_td_peer = await self._proxy_peer_pool.get_hightest_td_peer()
                except TimeoutError:
                    self.logger.warning("Timed out waiting for the hightest td peer from the pool")
                    pass
                else:
                    # if no peers are available right now, we'll just don't yield anything this time
                    if highest_td_peer is not None:
                        yield highest_td_peer

                await self.wait(new_tip_event.wait())
                new_tip_event.clear()

    def register_peer(self, peer: BasePeer) -> None:
        self._notify_tip()

    async def _handle_msg_loop(self) -> None:
        new_tip_types = tuple(self.subscription_msg_types)
        async for ev in self.wait_iter(self._event_bus.stream(PeerPoolMessageEvent)):
            if isinstance(ev.cmd, new_tip_types):
                self._notify_tip()

    def _notify_tip(self) -> None:
        for new_tip_event in self._subscriber_notices:
            new_tip_event.set()

    async def _run(self) -> None:
        self.run_daemon_task(self._handle_msg_loop())
        await self.cancellation()

    @contextmanager
    def _subscriber(self) -> Iterator[asyncio.Event]:
        new_tip_event = asyncio.Event()
        self._subscriber_notices.add(new_tip_event)
        try:
            yield new_tip_event
        finally:
            self._subscriber_notices.remove(new_tip_event)
