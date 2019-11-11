import asyncio

from asyncio_run_in_process import open_in_process
from async_exit_stack import AsyncExitStack

from p2p.service import AsyncioManager

from trinity._utils.os import friendly_filename_or_url
from trinity.config import (
    Eth1AppConfig,
)
from trinity.constants import (
    SYNC_BEAM,
)
from trinity.db.manager import DBClient
from trinity.extensibility import BaseApplicationComponent, ComponentService
from trinity.sync.beam.importer import (
    make_pausing_beam_chain,
    BlockPreviewServer,
)


class BaseBeamPreviewService(ComponentService):
    shard_num: int

    @property
    def ipc_filename(self) -> str:
        return friendly_filename_or_url(f"{self._component_name} {self.shard_num}")

    async def run_component_service(self) -> None:
        trinity_config = self.boot_info.trinity_config
        app_config = trinity_config.get_app_config(Eth1AppConfig)
        chain_config = app_config.get_chain_config()

        base_db = DBClient.connect(trinity_config.database_ipc_path)

        self._beam_chain = make_pausing_beam_chain(
            chain_config.vm_configuration,
            chain_config.chain_id,
            base_db,
            self.event_bus,
            # these preview executions are lower priority than the primary block import
            urgent=False,
        )

        import_server = BlockPreviewServer(self.event_bus, self._beam_chain, self.shard_num)
        manager = self.manager.run_child_service(import_server)
        await manager.wait_forever()


class BeamChainPreview0(BaseBeamPreviewService):
    shard_num = 0


class BeamChainPreview1(BaseBeamPreviewService):
    shard_num = 1


class BeamChainPreview2(BaseBeamPreviewService):
    shard_num = 2


class BeamChainPreview3(BaseBeamPreviewService):
    shard_num = 3


class BeamChainPreviewComponent(BaseApplicationComponent):
    """
    Subscribe to events that request a block import: ``DoStatelessBlockPreview``.
    On every preview, run through all the transactions, downloading the
    necessary data to execute them with the EVM.

    The beam sync previewer blocks when data is missing, so it's important to run
    in an isolated process.
    """
    name = "Beam Sync Chain Preview"

    @property
    def is_enabled(self) -> bool:
        return self._boot_info.args.sync_mode.upper() == SYNC_BEAM.upper()

    async def run(self) -> None:
        service_0 = BeamChainPreview0(self._boot_info, self.name)
        service_1 = BeamChainPreview1(self._boot_info, self.name)
        service_2 = BeamChainPreview2(self._boot_info, self.name)
        service_3 = BeamChainPreview3(self._boot_info, self.name)

        async with AsyncExitStack() as stack:
            proc_0 = await stack.enter_async_context(
                open_in_process(AsyncioManager.run_service, service_0),
            )
            proc_1 = await stack.enter_async_context(
                open_in_process(AsyncioManager.run_service, service_1),
            )
            proc_2 = await stack.enter_async_context(
                open_in_process(AsyncioManager.run_service, service_2),
            )
            proc_3 = await stack.enter_async_context(
                open_in_process(AsyncioManager.run_service, service_3),
            )

            await asyncio.gather(
                proc_0.wait(),
                proc_1.wait(),
                proc_2.wait(),
                proc_3.wait(),
            )
