import asyncio

from async_service import background_asyncio_service
from lahja import EndpointAPI

from trinity.config import (
    Eth1AppConfig,
)
from trinity.components.builtin.metrics.registry import NoopMetricsRegistry
from trinity.constants import (
    SYNC_BEAM,
)
from trinity.db.manager import DBClient
from trinity.extensibility import (
    AsyncioIsolatedComponent,
)
from trinity.sync.beam.importer import (
    make_pausing_beam_chain,
    BlockPreviewServer,
)


class BeamChainPreviewComponent(AsyncioIsolatedComponent):
    """
    Subscribe to events that request a block import: ``DoStatelessBlockPreview``.
    On every preview, run through all the transactions, downloading the
    necessary data to execute them with the EVM.

    The beam sync previewer blocks when data is missing, so it's important to run
    in an isolated process.
    """
    _beam_chain = None
    shard_num: int

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.args.sync_mode.upper() == SYNC_BEAM.upper()

    async def do_run(self, event_bus: EndpointAPI) -> None:
        trinity_config = self._boot_info.trinity_config
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        base_db = DBClient.connect(trinity_config.database_ipc_path)

        with base_db:
            loop = asyncio.get_event_loop()

            beam_chain = make_pausing_beam_chain(
                chain_config.vm_configuration,
                chain_config.chain_id,
                chain_config.consensus_context_class,
                base_db,
                event_bus,
                # We only want to collect metrics about blocks being imported, so here we use the
                # NoopMetricsRegistry.
                NoopMetricsRegistry(),
                # these preview executions are lower priority than the primary block import
                loop=loop,
                urgent=False,
            )

            preview_server = BlockPreviewServer(event_bus, beam_chain, self.shard_num)

            async with background_asyncio_service(preview_server) as manager:
                await manager.wait_finished()


class BeamChainPreviewComponent0(BeamChainPreviewComponent):
    shard_num = 0
    name = "Beam Sync Chain Preview 0"


class BeamChainPreviewComponent1(BeamChainPreviewComponent):
    shard_num = 1
    name = "Beam Sync Chain Preview 1"


class BeamChainPreviewComponent2(BeamChainPreviewComponent):
    shard_num = 2
    name = "Beam Sync Chain Preview 2"


class BeamChainPreviewComponent3(BeamChainPreviewComponent):
    shard_num = 3
    name = "Beam Sync Chain Preview 3"
