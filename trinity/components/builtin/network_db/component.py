from argparse import (
    Namespace,
    ArgumentParser,
    _SubParsersAction,
)
from typing import Iterable

from asyncio_run_in_process import run_in_process
from sqlalchemy.orm import Session

from eth_utils import to_tuple, get_extended_debug_logger

from p2p.service import AsyncioManager
from p2p.service import ServiceAPI
from p2p.tracking.connection import (
    BaseConnectionTracker,
    NoopConnectionTracker,
)

from trinity.config import (
    TrinityConfig,
)
from trinity.db.orm import get_tracking_database
from trinity.events import ShutdownRequest
from trinity.extensibility import (
    BaseApplicationComponent,
    BaseCommandComponent,
    ComponentService,
)
from trinity.db.network import (
    get_networkdb_path,
)
from trinity.exceptions import BadDatabaseError

from .connection.server import ConnectionTrackerServer
from .connection.tracker import (
    SQLiteConnectionTracker,
    MemoryConnectionTracker,
)
from .cli import (
    TrackingBackend,
    NormalizeTrackingBackend,
)
from .eth1_peer_db.server import PeerDBServer
from .eth1_peer_db.tracker import (
    BaseEth1PeerTracker,
    NoopEth1PeerTracker,
    SQLiteEth1PeerTracker,
    MemoryEth1PeerTracker,
)


class ClearNetworkDBComponent(BaseCommandComponent):
    name = "Clear Network Database"
    logger = get_extended_debug_logger('trinity.components.network_db.ClearNetworkDBComponent')

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        # Command to wipe the on-disk database
        remove_db_parser = subparser.add_parser(
            'remove-network-db',
            help='Remove the on-disk sqlite database that tracks data about the p2p network',
        )
        remove_db_parser.set_defaults(func=cls.clear_node_db)

    @classmethod
    def clear_node_db(cls, args: Namespace, trinity_config: TrinityConfig) -> None:
        db_path = get_networkdb_path(trinity_config)

        if db_path.exists():
            cls.logger.info("Removing network database at: %s", db_path.resolve())
            db_path.unlink()
        else:
            cls.logger.info("No network database found at: %s", db_path.resolve())


class NetworkDBComponent(BaseApplicationComponent):
    name = "Network Database"

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:
        tracking_parser = arg_parser.add_argument_group('network db')
        tracking_parser.add_argument(
            '--network-tracking-backend',
            help=(
                "Configure whether nodes are tracked and how. (sqlite3: persistent "
                "tracking across runs from an on-disk sqlite3 database, memory: tracking "
                "only in memory, do-not-track: no tracking)"
            ),
            action=NormalizeTrackingBackend,
            choices=('sqlite3', 'memory', 'do-not-track'),
            default=TrackingBackend.SQLITE3,
            type=str,
        )
        tracking_parser.add_argument(
            '--disable-networkdb-component',
            help=(
                "Disables the builtin 'Networkt Database' component. "
                "**WARNING**: disabling this API without a proper replacement "
                "will cause your trinity node to crash."
            ),
            action='store_true',
        )
        tracking_parser.add_argument(
            '--disable-blacklistdb',
            help=(
                "Disables the blacklist database server component of the Network Database "
                "component. **WARNING**: disabling this API without a proper replacement "
                "will cause your trinity node to crash."
            ),
            action='store_true',
        )
        tracking_parser.add_argument(
            '--disable-eth1-peer-db',
            help=(
                "Disables the ETH1.0 peer database server component of the Network Database "
                "component. **WARNING**: disabling this API without a proper replacement "
                "will cause your trinity node to crash."
            ),
            action='store_true',
        )
        tracking_parser.add_argument(
            '--enable-experimental-eth1-peer-tracking',
            help=(
                "Enables the experimental tracking of metadata about successful "
                "connections to Eth1 peers."
            ),
            action='store_true',
        )

    @property
    def is_enabled(self) -> bool:
        return not self._boot_info.args.disable_networkdb_component

    async def run(self) -> None:
        network_db_service = NetworkDBService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, network_db_service)


class NetworkDBService(ComponentService):
    async def run_component_service(self) -> None:
        try:
            tracker_services = self._get_services()
        except BadDatabaseError as err:
            self.logger.exception(f"Unrecoverable error in Network Component: {err}")
        else:
            for service in tracker_services:
                self.manager.run_daemon_child_service(service)

    _session: Session = None

    def _get_database_session(self) -> Session:
        if self._session is None:
            try:
                get_tracking_database(get_networkdb_path(self.boot_info.trinity_config))
            except BadDatabaseError as err:
                self.event_bus.broadcast_nowait(ShutdownRequest(
                    "Error loading network database.  Trying removing database "
                    f"with `remove-network-db` command:\n{err}"
                ))
        return self._session

    #
    # Blacklist Server
    #
    def _get_blacklist_tracker(self) -> BaseConnectionTracker:
        backend = self.boot_info.args.network_tracking_backend

        if backend is TrackingBackend.SQLITE3:
            session = self._get_database_session()
            return SQLiteConnectionTracker(session)
        elif backend is TrackingBackend.MEMORY:
            return MemoryConnectionTracker()
        elif backend is TrackingBackend.DO_NOT_TRACK:
            return NoopConnectionTracker()
        else:
            raise Exception(f"INVARIANT: {backend}")

    def _get_blacklist_service(self) -> ConnectionTrackerServer:
        tracker = self._get_blacklist_tracker()
        return ConnectionTrackerServer(
            event_bus=self.event_bus,
            tracker=tracker,
        )

    #
    # Eth1 Peer Server
    #
    def _get_eth1_tracker(self) -> BaseEth1PeerTracker:
        if not self.boot_info.args.enable_experimental_eth1_peer_tracking:
            return NoopEth1PeerTracker()

        backend = self.boot_info.args.network_tracking_backend

        if backend is TrackingBackend.SQLITE3:
            session = self._get_database_session()

            # TODO: correctly determine protocols and versions
            protocols = ('eth',)
            protocol_versions = (63,)

            # TODO: get genesis_hash
            return SQLiteEth1PeerTracker(
                session,
                network_id=self.boot_info.trinity_config.network_id,
                protocols=protocols,
                protocol_versions=protocol_versions,
            )
        elif backend is TrackingBackend.MEMORY:
            return MemoryEth1PeerTracker()
        elif backend is TrackingBackend.DO_NOT_TRACK:
            return NoopEth1PeerTracker()
        else:
            raise Exception(f"INVARIANT: {backend}")

    def _get_eth1_peer_server(self) -> PeerDBServer:
        tracker = self._get_eth1_tracker()

        return PeerDBServer(
            event_bus=self.event_bus,
            tracker=tracker,
        )

    @to_tuple
    def _get_services(self) -> Iterable[ServiceAPI]:
        if self.boot_info.args.disable_blacklistdb:
            # Allow this component to be disabled for extreme cases such as the
            # user swapping in an equivalent experimental version.
            self.logger.warning("Blacklist Database disabled via CLI flag")
            return
        else:
            yield self._get_blacklist_service()

        if self.boot_info.args.disable_eth1_peer_db:
            # Allow this component to be disabled for extreme cases such as the
            # user swapping in an equivalent experimental version.
            self.logger.warning("ETH1 Peer Database disabled via CLI flag")
        else:
            yield self._get_eth1_peer_server()
