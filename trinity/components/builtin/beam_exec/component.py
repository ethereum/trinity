import asyncio

from async_service import background_asyncio_service
from lahja import EndpointAPI

from trinity.config import Eth1AppConfig
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
        # type ignored to fix https://github.com/ethereum/trinity/issues/1520
        return self._boot_info.args.sync_mode.upper() == SYNC_BEAM.upper()  # type: ignore

    async def do_run(self, event_bus: EndpointAPI) -> None:
        trinity_config = self._boot_info.trinity_config
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        base_db = DBClient.connect(trinity_config.database_ipc_path)

        with base_db:
            beam_chain = make_pausing_beam_chain(
                chain_config.vm_configuration,
                chain_config.chain_id,
                chain_config.consensus_context_class,
                base_db,
                event_bus,
                loop=asyncio.get_event_loop(),
            )

            import_server = BlockImportServer(event_bus, beam_chain)

            async with background_asyncio_service(import_server) as manager:
                await manager.wait_finished()
