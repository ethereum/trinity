from cancel_token import (
    CancelToken,
)
from lahja import (
    Endpoint,
)

from p2p.kademlia import (
    from_uris,
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.service import (
    BaseService,
)

from .events import (
    ConnectToNodeCommand,
    PeerCountRequest,
    PeerCountResponse,
)


class BasePeerPoolEventBusRequestHandler(BaseService):
    """
    Base request handler that picks up requests from the event bus and delegates them to the
    peer pool either to perform some action or resolve to some response.
    Subclasses should extend this class with custom event handlers.
    """

    def __init__(self,
                 event_bus: Endpoint,
                 peer_pool: BasePeerPool,
                 token: CancelToken = None) -> None:
        super().__init__(token)
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
