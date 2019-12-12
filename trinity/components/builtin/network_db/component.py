from argparse import (
    Namespace,
    ArgumentParser,
    _SubParsersAction,
)
import asyncio
import logging
from typing import ClassVar, Iterable

from async_exit_stack import AsyncExitStack
from async_service import background_asyncio_service, Service
from eth_utils import ValidationError, to_tuple
from lahja import EndpointAPI
from sqlalchemy.orm import Session

from p2p.tracking.connection import (
    BaseConnectionTracker,
    NoopConnectionTracker,
)

from trinity.boot_info import BootInfo
from trinity.config import TrinityConfig
from trinity.db.orm import get_tracking_database
from trinity.extensibility import (
    AsyncioIsolatedComponent,
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


class NetworkDBComponent(AsyncioIsolatedComponent):
    name = "Network Database"

    endpoint_name = "network-db"

    logger = logging.getLogger('trinity.components.network_db.NetworkDB')

    @property
    def is_enabled(self) -> bool:
        return not self._boot_info.args.disable_networkdb_component

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
                "Disables the builtin 'Network Database' component. "
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

        # Command to wipe the on-disk database
        remove_db_parser = subparser.add_parser(
            'remove-network-db',
            help='Remove the on-disk sqlite database that tracks data about the p2p network',
        )
        remove_db_parser.set_defaults(func=cls.clear_node_db)

    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        try:
            get_tracking_database(get_networkdb_path(boot_info.trinity_config))
        except BadDatabaseError as err:
            raise ValidationError(
                "Error loading network database.  Trying removing database "
                f"with `remove-network-db` command:\n{err}"
            ) from err

    @classmethod
    def clear_node_db(cls, args: Namespace, trinity_config: TrinityConfig) -> None:
        db_path = get_networkdb_path(trinity_config)

        if db_path.exists():
            cls.logger.info("Removing network database at: %s", db_path.resolve())
            db_path.unlink()
        else:
            cls.logger.info("No network database found at: %s", db_path.resolve())

    _session: ClassVar[Session] = None

    @classmethod
    def _get_database_session(cls, boot_info: BootInfo) -> Session:
        if cls._session is None:
            cls._session = get_tracking_database(get_networkdb_path(boot_info.trinity_config))
        return cls._session

    #
    # Blacklist Server
    #
    @classmethod
    def _get_blacklist_tracker(cls, boot_info: BootInfo) -> BaseConnectionTracker:
        backend = boot_info.args.network_tracking_backend

        if backend is TrackingBackend.SQLITE3:
            session = cls._get_database_session(boot_info)
            return SQLiteConnectionTracker(session)
        elif backend is TrackingBackend.MEMORY:
            return MemoryConnectionTracker()
        elif backend is TrackingBackend.DO_NOT_TRACK:
            return NoopConnectionTracker()
        else:
            raise Exception(f"INVARIANT: {backend}")

    @classmethod
    def _get_blacklist_service(cls,
                               boot_info: BootInfo,
                               event_bus: EndpointAPI) -> ConnectionTrackerServer:
        tracker = cls._get_blacklist_tracker(boot_info)
        return ConnectionTrackerServer(
            event_bus=event_bus,
            tracker=tracker,
        )

    #
    # Eth1 Peer Server
    #
    @classmethod
    def _get_eth1_tracker(cls, boot_info: BootInfo) -> BaseEth1PeerTracker:
        if not boot_info.args.enable_experimental_eth1_peer_tracking:
            return NoopEth1PeerTracker()

        backend = boot_info.args.network_tracking_backend

        if backend is TrackingBackend.SQLITE3:
            session = cls._get_database_session(boot_info)

            # TODO: correctly determine protocols and versions
            protocols = ('eth',)
            protocol_versions = (63,)

            # TODO: get genesis_hash
            return SQLiteEth1PeerTracker(
                session,
                network_id=boot_info.trinity_config.network_id,
                protocols=protocols,
                protocol_versions=protocol_versions,
            )
        elif backend is TrackingBackend.MEMORY:
            return MemoryEth1PeerTracker()
        elif backend is TrackingBackend.DO_NOT_TRACK:
            return NoopEth1PeerTracker()
        else:
            raise Exception(f"INVARIANT: {backend}")

    @classmethod
    def _get_eth1_peer_server(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> PeerDBServer:
        tracker = cls._get_eth1_tracker(boot_info)

        return PeerDBServer(
            event_bus=event_bus,
            tracker=tracker,
        )

    @classmethod
    @to_tuple
    def _get_services(cls,
                      boot_info: BootInfo,
                      event_bus: EndpointAPI) -> Iterable[Service]:
        if boot_info.args.disable_blacklistdb:
            # Allow this component to be disabled for extreme cases such as the
            # user swapping in an equivalent experimental version.
            cls.logger.warning("Blacklist Database disabled via CLI flag")
            return
        else:
            yield cls._get_blacklist_service(boot_info, event_bus)

        if boot_info.args.disable_eth1_peer_db:
            # Allow this component to be disabled for extreme cases such as the
            # user swapping in an equivalent experimental version.
            cls.logger.warning("ETH1 Peer Database disabled via CLI flag")
        else:
            yield cls._get_eth1_peer_server(boot_info, event_bus)

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        try:
            tracker_services = cls._get_services(boot_info, event_bus)
        except BadDatabaseError as err:
            cls.logger.exception(f"Unrecoverable error in Network Component: {err}")

        async with AsyncExitStack() as stack:
            tracker_managers = tuple([
                await stack.enter_async_context(background_asyncio_service(service))
                for service in tracker_services
            ])
            await asyncio.gather(*(
                manager.wait_finished()
                for manager in tracker_managers
            ))
