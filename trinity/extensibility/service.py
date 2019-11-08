from abc import abstractmethod
import logging

from lahja import AsyncioEndpoint, EndpointAPI, ConnectionConfig

from p2p.trio_service import Service, ManagerAPI

from trinity._utils.logging import (
    setup_log_levels,
    setup_queue_logging,
)

from .component import TrinityBootInfo


class ComponentService(Service):
    def __init__(self, boot_info: TrinityBootInfo, endpoint_name: str) -> None:
        self._boot_info = boot_info
        self._endpoint_name = endpoint_name

    async def run(self, manager: ManagerAPI) -> None:
        # setup cross process logging
        log_queue = self._boot_info.boot_kwargs['log_queue']
        level = self._boot_info.boot_kwargs.get('log_level', logging.INFO)
        setup_queue_logging(log_queue, level)
        if self.boot_info.args.log_levels:
            setup_log_levels(self.boot_info.args.log_levels)

        # setup the lahja endpoint
        self._connection_config = ConnectionConfig.from_name(
            self._endpoint_name,
            self._boot_info.trinity_config.ipc_dir
        )

        async with AsyncioEndpoint.serve(self._connection_config) as endpoint:
            await self.run_component_service(endpoint)

    @staticmethod
    @abstractmethod
    async def run_component_service(boot_info: TrinityBootInfo, endpoint: EndpointAPI) -> None:
        ...
