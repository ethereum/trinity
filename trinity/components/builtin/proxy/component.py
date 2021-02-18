from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
import logging

from async_service import background_asyncio_service
from lahja import EndpointAPI

from trinity.components.builtin.metrics.service.noop import NOOP_METRICS_SERVICE
from trinity.config import (
    TrinityConfig,
    Eth1AppConfig,
)
from trinity.constants import APP_IDENTIFIER_ETH1
from trinity.extensibility import Application, AsyncioIsolatedComponent
from trinity._utils.logging import (
    IPCListener,
)

from .node import ProxyNode


class ProxyApplicationComponent(Application):
    logger = logging.getLogger('trinity.components.proxy.ProxyApplicationComponent')

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:

        attach_parser = subparser.add_parser(
            'proxy',
            help='Run in proxy mode',
        )

        attach_parser.set_defaults(func=cls.run_proxy_node)

    @classmethod
    def run_proxy_node(cls, args: Namespace, trinity_config: TrinityConfig) -> None:
        cls.logger.info("Running proxy client...")

        from trinity.bootstrap import (
            construct_boot_info,
            display_launch_logs,
            run as run_components,
        )
        from trinity.main import trinity_boot, get_base_db
        from trinity.components.registry import BASE_COMPONENTS

        boot_info, handlers = construct_boot_info(
            APP_IDENTIFIER_ETH1,
            (ProxyNodeComponent,),
            (Eth1AppConfig,),
        )
        trinity_config = boot_info.trinity_config

        # This prints out the ASCII "trinity" header in the terminal
        display_launch_logs(trinity_config)

        # Setup the log listener which child processes relay their logs through
        with IPCListener(*handlers).run(trinity_config.logging_ipc_path):
            trinity_boot(boot_info)
            run_components(BASE_COMPONENTS + (ProxyNodeComponent,), boot_info, get_base_db)


class ProxyNodeComponent(AsyncioIsolatedComponent):
    logger = logging.getLogger('trinity.components.proxy.ProxyNodeComponent')
    name = 'Proxy Node'

    @property
    def is_enabled(self) -> bool:
        return True

    async def do_run(self, event_bus: EndpointAPI) -> None:
        boot_info = self._boot_info
        trinity_config = boot_info.trinity_config

        if boot_info.args.enable_metrics:
            raise Exception("Metrics not supported")
        else:
            metrics_service = NOOP_METRICS_SERVICE

        node = ProxyNode(
            event_bus=event_bus,
            metrics_service=metrics_service,
            trinity_config=trinity_config,
        )
        self.logger.info("Running....")
        async with background_asyncio_service(node) as manager:
            await manager.wait_finished()
