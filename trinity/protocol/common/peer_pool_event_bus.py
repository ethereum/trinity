from typing import (
    cast,
    Generic,
    TypeVar,
)
from cancel_token import (
    CancelToken,
)
from lahja import (
    Endpoint,
)

from p2p.kademlia import (
    from_uris,
)
from p2p.peer import (
    BasePeer,
    IdentifiablePeer,
    PeerSubscriber,
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.protocol import (
    Command,
)
from p2p.service import (
    BaseService,
)

from .events import (
    ConnectToNodeCommand,
    PeerCountRequest,
    PeerCountResponse,
    PeerPoolMessageEvent,
    PeerJoinedEvent,
    PeerLeftEvent,
)


TPeer = TypeVar('TPeer', bound=BasePeer)


class BasePeerPoolEventBusRequestHandler(BaseService, Generic[TPeer]):
    """
    Base class that handles requests that are coming through the event bus and are meant to be
    handled by the peer pool. Subclasses should extend this class with custom event handlers.
    """

    def __init__(self,
                 event_bus: Endpoint,
                 peer_pool: BasePeerPool,
                 token: CancelToken = None) -> None:
        super().__init__(token)
        # TODO: Make both plublic (intended to use in derived classes)
        self._peer_pool = peer_pool
        self._event_bus = event_bus

    async def accept_connect_commands(self) -> None:
        async for command in self.wait_iter(self._event_bus.stream(ConnectToNodeCommand)):
            self.logger.debug('Received request to connect to %s', command.node)
            self.run_task(self._peer_pool.connect_to_nodes(from_uris([command.node])))

    async def handle_peer_count_requests(self) -> None:
        async for req in self.wait_iter(self._event_bus.stream(PeerCountRequest)):
            self._event_bus.broadcast(
                PeerCountResponse(len(self._peer_pool)),
                req.broadcast_config()
            )

    async def _run(self) -> None:
        self.logger.info("Running BasePeerPoolEventBusRequestHandler")

        self.run_daemon_task(self.handle_peer_count_requests())
        self.run_daemon_task(self.accept_connect_commands())

        await self.cancel_token.wait()

    def maybe_return_peer(self, dto_peer: IdentifiablePeer) -> TPeer:

        try:
            peer = self._peer_pool.connected_nodes[dto_peer.uri]
        except KeyError:
            self.logger.warning("Peer %s does not exist in the pool anymore", dto_peer.uri)
            return None
        else:
            if not peer.is_operational:
                self.logger.warning("Peer %s is not operational", peer)
                return None
            else:
                return cast(TPeer, peer)


class BasePeerPoolMessageRelayer(BaseService, PeerSubscriber):
    """
    Base class that relays peer pool events on the event bus. The following events are exposed
    on the event bus:
      - Incoming peer messages -> :class:`~trinity.protocol.common.events.PeerPoolMessageEvent`
      - Peer joined the pool -> :class:`~trinity.protocol.common.events.PeerJoinedEvent`
      - Peer left the pool -> :class:`~trinity.protocol.common.events.PeerLeftEvent`

    Subclasses should extend this to relay events specific to other peer pools.
    """

    msg_queue_maxsize: int = 2000

    subscription_msg_types = frozenset({Command})

    def __init__(self,
                 peer_pool: BasePeerPool,
                 event_bus: Endpoint,
                 token: CancelToken = None) -> None:
        super().__init__(token)
        self._event_bus = event_bus
        self._peer_pool = peer_pool

    async def _run(self) -> None:
        self.logger.info("Running BasePeerPoolMessageRelayer")

        with self.subscribe(self._peer_pool):
            while self.is_operational:
                peer, cmd, msg = await self.wait(self.msg_queue.get())
                self._event_bus.broadcast(PeerPoolMessageEvent(peer.to_dto(), cmd, msg))

    def register_peer(self, peer: BasePeer) -> None:
        self._event_bus.broadcast(PeerJoinedEvent(peer.to_dto()))

    def deregister_peer(self, peer: BasePeer) -> None:
        self._event_bus.broadcast(PeerLeftEvent(peer.to_dto()))
