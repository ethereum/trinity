from abc import abstractmethod
import logging

from lahja import AsyncioEndpoint, ConnectionConfig

from p2p.service import Service

from trinity._utils.logging import (
    setup_log_levels,
    setup_queue_logging,
)

from .component import TrinityBootInfo


class ComponentService(Service):
    def __init__(self, boot_info: TrinityBootInfo, endpoint_name: str) -> None:
        self.boot_info = boot_info
        self._endpoint_name = endpoint_name

    async def run(self) -> None:
        # setup cross process logging
        log_queue = self.boot_info.boot_kwargs['log_queue']
        level = self.boot_info.boot_kwargs.get('log_level', logging.INFO)
        setup_queue_logging(log_queue, level)
        if self.boot_info.args.log_levels:
            setup_log_levels(self.boot_info.args.log_levels)

        # setup the lahja endpoint
        self._connection_config = ConnectionConfig.from_name(
            self._endpoint_name,
            self.boot_info.trinity_config.ipc_dir
        )

        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            self.event_bus = endpoint
            await self.run_component_service()

    @abstractmethod
    async def run_component_service(self) -> None:
        ...
