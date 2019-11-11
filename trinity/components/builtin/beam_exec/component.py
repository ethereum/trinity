from asyncio_run_in_process import run_in_process

from p2p.service import AsyncioManager

from trinity.config import Eth1AppConfig
from trinity.constants import SYNC_BEAM
from trinity.db.manager import DBClient
from trinity.extensibility import BaseApplicationComponent, ComponentService
from trinity.sync.beam.importer import (
    make_pausing_beam_chain,
    BlockImportServer,
)


class BeamChainExecutionComponent(BaseApplicationComponent):
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

    async def run(self) -> None:
        service = BeamExecutionService(self._boot_info, self.name)

        await run_in_process(AsyncioManager.run_service, service)


class BeamExecutionService(ComponentService):
    async def run_component_service(self) -> None:
        trinity_config = self.boot_info.trinity_config
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        self._beam_chain = make_pausing_beam_chain(
            chain_config.vm_configuration,
            chain_config.chain_id,
            DBClient.connect(trinity_config.database_ipc_path),
            self.event_bus,
        )

        import_server = BlockImportServer(self.event_bus, self._beam_chain)
        manager = self.manager.run_child_service(import_server)
        await manager.wait_forever()
