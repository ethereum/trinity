import asyncio

from trinity._utils.shutdown import (
    exit_with_service_and_endpoint,
)
from trinity.constants import (
    PEER_DB_EVENTBUS_ENDPOINT,
    TrackingBackend,
)
from trinity.endpoint import (
    TrinityEventBusEndpoint,
)
from trinity.extensibility.plugin import (
    BaseIsolatedPlugin,
)
from trinity.db.orm import get_tracking_database
from trinity.tracking import get_nodedb_path

from .server import PeerDBServer
from .tracker import (
    BaseEth1PeerTracker,
    NoopEth1PeerTracker,
    SQLiteEth1PeerTracker,
    MemoryEth1PeerTracker,
)


class PeerDBPlugin(BaseIsolatedPlugin):
    tracker: BaseEth1PeerTracker

    @property
    def name(self) -> str:
        return "Peer Database"

    @property
    def normalized_name(self) -> str:
        return PEER_DB_EVENTBUS_ENDPOINT

    def on_ready(self, manager_eventbus: TrinityEventBusEndpoint) -> None:
        self.start()

    def _get_tracker(self) -> BaseEth1PeerTracker:
        config = self.context.trinity_config

        if config.tracking_backend is TrackingBackend.sqlite3:
            session = get_tracking_database(get_nodedb_path(config))

            # TODO: correctly determine protocols and versions
            protocols = ('eth',)
            protocol_versions = (61,)

            # TODO: get genesis_hash
            return SQLiteEth1PeerTracker(
                session,
                network_id=self.context.trinity_config.network_id,
                protocols=protocols,
                protocol_versions=protocol_versions,
            )
        elif config.tracking_backend is TrackingBackend.memory:
            return MemoryEth1PeerTracker()
        elif config.tracking_backend is TrackingBackend.disabled:
            return NoopEth1PeerTracker()
        else:
            raise Exception(f"INVARIANT: {config.tracking_backend}")

    def do_start(self) -> None:
        tracker = self._get_tracker()

        loop = asyncio.get_event_loop()
        peer_db_service = PeerDBServer(
            event_bus=self.event_bus,
            tracker=tracker,
        )
        asyncio.ensure_future(exit_with_service_and_endpoint(peer_db_service, self.event_bus))
        asyncio.ensure_future(peer_db_service.run())
        loop.run_forever()
        loop.close()
