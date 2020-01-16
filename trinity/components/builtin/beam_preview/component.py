import asyncio

from async_service import run_asyncio_service
from lahja import EndpointAPI

from trinity.boot_info import BootInfo
from trinity.config import (
    Eth1AppConfig,
)
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

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        trinity_config = boot_info.trinity_config
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
                # these preview executions are lower priority than the primary block import
                loop=loop,
                urgent=False,
            )

            preview_server = BlockPreviewServer(event_bus, beam_chain, cls.shard_num)

            await run_asyncio_service(preview_server)


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
