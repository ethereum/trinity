import asyncio

from async_service import background_asyncio_service
from lahja import EndpointAPI

from trinity.config import Eth1AppConfig
from trinity.components.builtin.metrics.component import metrics_service_from_args
from trinity.components.builtin.metrics.service.asyncio import AsyncioMetricsService
from trinity.components.builtin.metrics.service.noop import NOOP_METRICS_SERVICE
from trinity.constants import SYNC_BEAM
from trinity.db.manager import DBClient
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity.sync.beam.importer import (
    make_pausing_beam_chain,
    BlockImportServer,
)


class BeamChainExecutionComponent(AsyncioIsolatedComponent):
    """
    Subscribe to events that request a block import: ``DoStatelessBlockImport``.
    Use the beam sync importer, which knows what to do when the state trie
    is missing data like: accounts, storage or bytecode.

    The beam sync importer blocks when data is missing, so it's important to run
    in an isolated process.
    """
    _beam_chain = None

    name = "Beam Sync Chain Execution"

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.args.sync_mode.upper() == SYNC_BEAM.upper()

    async def do_run(self, event_bus: EndpointAPI) -> None:
        trinity_config = self._boot_info.trinity_config
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        base_db = DBClient.connect(trinity_config.database_ipc_path)

        if self._boot_info.args.enable_metrics:
            metrics_service = metrics_service_from_args(
                self._boot_info.args, AsyncioMetricsService)
        else:
            # Use a NoopMetricsService so that no code branches need to be taken if metrics
            # are disabled
            metrics_service = NOOP_METRICS_SERVICE

        with base_db:
            beam_chain = make_pausing_beam_chain(
                chain_config.vm_configuration,
                chain_config.chain_id,
                chain_config.consensus_context_class,
                base_db,
                event_bus,
                metrics_service.registry,
                loop=asyncio.get_event_loop(),
            )

            import_server = BlockImportServer(event_bus, beam_chain)

            async with background_asyncio_service(metrics_service):
                async with background_asyncio_service(import_server) as manager:
                    await manager.wait_finished()
