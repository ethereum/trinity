import asyncio
from contextlib import contextmanager
from typing import (
    AsyncIterator,
    Generic,
    Iterator,
    Set,
    Type,
    TypeVar,
)

from cancel_token import CancelToken
from eth_utils import ValidationError
from lahja import BaseEvent

from p2p.exceptions import NoConnectedPeers
from p2p.service import BaseService

from trinity.protocol.common.events import PeerJoinedEvent
from trinity.protocol.common.peer import BaseChainProxyPeer
from trinity.protocol.eth.peer import BaseProxyPeerPool


TProxyPeer = TypeVar('TProxyPeer', bound=BaseChainProxyPeer)


class BaseChainTipMonitor(BaseService, Generic[TProxyPeer]):
    """
    Monitor for potential changes to the tip of the chain: a new peer or a new block

    Subclass must specify :attr:`subscription_msg_types`
    """
    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize = 2000

    monitor_event_type: Type[BaseEvent]

    def __init__(
            self,
            proxy_peer_pool: BaseProxyPeerPool[TProxyPeer],
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._proxy_peer_pool = proxy_peer_pool
        # There is one event for each subscriber, each one gets set any time new tip info arrives
        self._subscriber_notices: Set[asyncio.Event] = set()

    async def wait_tip_info(self) -> AsyncIterator[TProxyPeer]:
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
                    highest_td_peer = await self.wait(self._proxy_peer_pool.get_highest_td_peer())
                except TimeoutError:
                    self.logger.warning("Timed out waiting on peer with highest td")
                    pass
                except NoConnectedPeers:
                    # no peers are available right now, skip the new tip info yield
                    pass
                else:
                    yield highest_td_peer

                await self.wait(new_tip_event.wait())
                new_tip_event.clear()

    def _notify_tip(self) -> None:
        for new_tip_event in self._subscriber_notices:
            new_tip_event.set()

    async def _run(self) -> None:
        monitor_subscription = self._proxy_peer_pool.event_bus.subscribe(
            self.monitor_event_type,
            lambda ev: self._notify_tip()
        )
        joined_subscription = self._proxy_peer_pool.event_bus.subscribe(
            PeerJoinedEvent,
            lambda ev: self._notify_tip()
        )

        await self.cancellation()
        monitor_subscription.unsubscribe()
        joined_subscription.unsubscribe()

    @contextmanager
    def _subscriber(self) -> Iterator[asyncio.Event]:
        new_tip_event = asyncio.Event()
        self._subscriber_notices.add(new_tip_event)
        try:
            yield new_tip_event
        finally:
            self._subscriber_notices.remove(new_tip_event)
