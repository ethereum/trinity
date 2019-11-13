from abc import abstractmethod

from eth_utils import get_extended_debug_logger

from lahja import AsyncioEndpoint, ConnectionConfig

from p2p.service import Service

from trinity._utils.logging import (
    set_logger_levels,
    setup_child_process_logging,
)
from trinity._utils.os import friendly_filename_or_url

from .component import TrinityBootInfo


class ComponentService(Service):
    logger = get_extended_debug_logger('trinity.extensibility.ComponentService')

    _explicit_ipc_filename: str = None

    def __init__(self, boot_info: TrinityBootInfo, component_name: str) -> None:
        self.boot_info = boot_info
        self._component_name = component_name

    @property
    def ipc_filename(self) -> str:
        if self._explicit_ipc_filename is not None:
            return self._explicit_ipc_filename
        else:
            return friendly_filename_or_url(self._component_name)

    async def run(self) -> None:
        # setup cross process logging
        setup_child_process_logging(
            self.boot_info.trinity_config.logging_ipc_path,
            self.boot_info.log_level,
        )
        if self.boot_info.args.log_levels:
            set_logger_levels(self.boot_info.args.log_levels)

        # setup the lahja endpoint
        self._connection_config = ConnectionConfig.from_name(
            self.ipc_filename,
            self.boot_info.trinity_config.ipc_dir
        )

        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self.event_bus = endpoint
            try:
                await self.run_component_service()
            except KeyboardInterrupt:
                self.manager.cancel()

    @abstractmethod
    async def run_component_service(self) -> None:
        ...
