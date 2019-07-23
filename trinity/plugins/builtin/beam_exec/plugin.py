import asyncio

from lahja import EndpointAPI

from trinity._utils.shutdown import exit_with_services
from trinity.config import Eth1AppConfig
from trinity.constants import SYNC_BEAM
from trinity.db.eth1.manager import (
    create_db_consumer_manager
)
from trinity.extensibility import (
    AsyncioIsolatedPlugin,
)
from trinity.sync.beam.importer import (
    make_pausing_beam_chain,
    BlockImportServer,
)


class BeamChainExecutionPlugin(AsyncioIsolatedPlugin):
    """
    Subscribe to events that request a block import: ``DoStatelessBlockImport``.
    Use the beam sync importer, which knows what to do when the state trie
    is missing data like: accounts, storage or bytecode.

    The beam sync importer blocks when data is missing, so it's important to run
    in an isolated process.
    """
    _beam_chain = None

    @property
    def name(self) -> str:
        return "Beam Sync Chain Execution"

    def on_ready(self, manager_eventbus: EndpointAPI) -> None:
        if self.boot_info.args.sync_mode.upper() == SYNC_BEAM.upper():
            self.start()

    async def do_start(self) -> None:
        trinity_config = self.boot_info.trinity_config
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        db_manager = create_db_consumer_manager(trinity_config.database_ipc_path)

        self._beam_chain = make_pausing_beam_chain(
            chain_config.vm_configuration,
            chain_config.chain_id,
            db_manager.get_db(),  # type: ignore
            self.event_bus,
            self._loop,
        )

        import_server = BlockImportServer(self.event_bus, self._beam_chain)
        asyncio.ensure_future(exit_with_services(import_server, self._event_bus_service))
        await import_server.run()
