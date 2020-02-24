from abc import (
    ABC,
    abstractmethod
)
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio
import logging
from typing import AsyncIterator, Iterable, Type, TYPE_CHECKING, Union

from async_generator import asynccontextmanager

from lahja import AsyncioEndpoint, ConnectionConfig, TrioEndpoint

from trinity._utils.os import friendly_filename_or_url
from trinity.boot_info import BootInfo
from trinity.cli_parser import parser, subparser
from trinity.config import BaseAppConfig, Eth1AppConfig, TrinityConfig
from trinity.constants import APP_IDENTIFIER_ETH1, SYNC_FULL
from trinity.initialization import initialize_data_dir, is_data_dir_initialized

if TYPE_CHECKING:
    from trinity.extensibility.trio import TrioIsolatedComponent  # noqa: F401
    from trinity.extensibility.asyncio import AsyncioIsolatedComponent  # noqa: F401

logger = logging.getLogger('trinity.extensibility.component')


class BaseComponentAPI(ABC):
    @classmethod
    @abstractmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        """
        Give the component a chance to amend the Trinity CLI argument parser.
        """
        ...

    @classmethod
    @abstractmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        """
        Give the component a chance to do runtime validation of the command line arguments.
        """
        ...


class Application(BaseComponentAPI):
    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        pass


class ComponentAPI(BaseComponentAPI):
    name: str

    @abstractmethod
    def __init__(self, boot_info: BootInfo) -> None:
        ...

    @property
    @abstractmethod
    def is_enabled(self) -> bool:
        ...

    @abstractmethod
    async def run(self) -> None:
        ...


class BaseComponent(ComponentAPI):
    def __init__(self, boot_info: BootInfo) -> None:
        if not hasattr(self, 'name'):
            raise AttributeError(f"No name attribute defined for {self.__class__}")
        self._boot_info = boot_info

    def __str__(self) -> str:
        return f"<Component[{self.name}]>"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(boot_info={self._boot_info})"

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        pass

    @classmethod
    def validate_cli(cls, boot_info: BootInfo) -> None:
        pass


class BaseIsolatedComponent(BaseComponent):
    """
    A :class:`~trinity.extensibility.component.BaseIsolatedComponent` runs in an isolated process
    and hence provides security and flexibility by not making assumptions about its internal
    operations.

    Such components are free to use non-blocking asyncio as well as synchronous calls. When an
    isolated component is stopped it does first receive a SIGINT followed by a SIGTERM soon after.
    It is up to the component to handle these signals accordingly.
    """
    endpoint_name: str = None

    @classmethod
    def get_endpoint_name(cls) -> str:
        if cls.endpoint_name is None:
            return friendly_filename_or_url(cls.name)
        else:
            return cls.endpoint_name


@asynccontextmanager
async def run_component(component: ComponentAPI) -> AsyncIterator[None]:
    task = asyncio.ensure_future(component.run())
    logger.debug("Starting component: %s", component.name)
    try:
        yield
    finally:
        logger.debug("Stopping component: %s", component.name)
        if not task.done():
            logger.debug("Cancelling component: %s", component.name)
            task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        logger.debug("Stopped component: %s", component.name)


async def run_standalone_component(
    component: Union[Type['TrioIsolatedComponent'], Type['AsyncioIsolatedComponent']],
    endpoint_type: Union[Type[TrioEndpoint], Type[AsyncioEndpoint]],
    app_identifier: str,
    app_config_types: Iterable[Type[BaseAppConfig]],
) -> None:

    # Require a root dir to be specified as we don't want to mess with the default one.
    for action in parser._actions:
        if action.dest == 'trinity_root_dir':
            action.required = True
            break

    component.configure_parser(parser, subparser)
    args = parser.parse_args()
    # FIXME: Figure out a way to avoid having to set this.
    args.sync_mode = SYNC_FULL

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
    if args.log_levels is not None:
        for name, level in args.log_levels.items():
            logging.getLogger(name).setLevel(level)

    trinity_config = TrinityConfig.from_parser_args(args, app_identifier, app_config_types)
    trinity_config.trinity_root_dir.mkdir(exist_ok=True)
    if not is_data_dir_initialized(trinity_config):
        initialize_data_dir(trinity_config)
    boot_info = BootInfo(
        args=args,
        trinity_config=trinity_config,
        child_process_log_level=None,
        logger_levels=None,
        profile=False,
    )
    conn_config = ConnectionConfig.from_name(component.get_endpoint_name(), trinity_config.ipc_dir)

    async with endpoint_type.serve(conn_config) as endpoint:
        await component.do_run(boot_info, endpoint)


async def run_standalone_eth1_component(
    component: Union[Type['TrioIsolatedComponent'], Type['AsyncioIsolatedComponent']],
) -> None:
    from trinity.extensibility.trio import TrioIsolatedComponent  # noqa: F811
    from trinity.extensibility.asyncio import AsyncioIsolatedComponent  # noqa: F811
    if issubclass(component, TrioIsolatedComponent):
        endpoint_type: Union[Type[TrioEndpoint], Type[AsyncioEndpoint]] = TrioEndpoint
    elif issubclass(component, AsyncioIsolatedComponent):
        endpoint_type = AsyncioEndpoint
    else:
        raise ValueError("Unknown component type: %s", type(component))
    await run_standalone_component(component, endpoint_type, APP_IDENTIFIER_ETH1, (Eth1AppConfig,))
