from abc import (
    ABC,
    abstractmethod
)
from argparse import (
    ArgumentParser,
    _SubParsersAction,
)
import asyncio
import contextlib
import logging
import os
import pathlib
import signal
from typing import AsyncIterator, Optional, Tuple, Type, TYPE_CHECKING, Union

from asyncio_run_in_process.typing import SubprocessKwargs


from lahja import AsyncioEndpoint, ConnectionConfig, EndpointAPI, TrioEndpoint

from trinity._utils.os import friendly_filename_or_url
from trinity._utils.logging import get_logger
from trinity.boot_info import BootInfo
from trinity.cli_parser import parser, subparser
from trinity.config import BaseAppConfig, BeaconAppConfig, Eth1AppConfig, TrinityConfig
from trinity.constants import APP_IDENTIFIER_BEACON, APP_IDENTIFIER_ETH1, SYNC_FULL
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

    def get_subprocess_kwargs(self) -> Optional[SubprocessKwargs]:
        # By default we want every child process its own process group leader as we don't want a
        # Ctrl-C in the terminal to send a SIGINT to each one of our process, as that is already
        # handled by open_in_process().
        start_new_session = True
        if os.getenv('TRINITY_SINGLE_PROCESS_GROUP') == "1":
            # This is needed because some of our integration tests rely on all processes being in
            # a single process group.
            start_new_session = False
        return {'start_new_session': start_new_session}

    @classmethod
    def get_endpoint_name(cls) -> str:
        if cls.endpoint_name is None:
            return friendly_filename_or_url(cls.name)
        else:
            return cls.endpoint_name


async def _cleanup_component_task(component_name: str, task: "asyncio.Future[None]") -> None:
    logger.debug("Stopping component: %s", component_name)
    if not task.done():
        # XXX: This could be a component that crashed and sent a ShutdownRequest to trinity, and
        # in that case by cancelling it we will throw away the exception that caused it to crash.
        # Unfortunately there's no way to distinguish between a component that crashed and just
        # hasn't terminated yet and one that is still running, as in both cases all we have is a
        # task that is not done(), so the best thing we can do is make sure our *Component base
        # classes log any exceptions coming from subclasses (i.e. the do_run() method) before
        # propagating them.
        logger.debug("%s component not done yet, cancelling it", component_name)
        task.cancel()
    else:
        logger.debug("%s component already done", component_name)
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.debug("Stopped component: %s", component_name)


@contextlib.asynccontextmanager
async def run_component(component: ComponentAPI) -> AsyncIterator[None]:
    task = asyncio.ensure_future(component.run())
    logger.debug("Starting component: %s", component.name)
    try:
        yield
    finally:
        await _cleanup_component_task(component.name, task)


@contextlib.asynccontextmanager
async def _run_asyncio_component_in_proc(
        component: 'AsyncioIsolatedComponent',
        event_bus: EndpointAPI,
) -> AsyncIterator[None]:
    """
    Run the given AsyncioIsolatedComponent in the same process as ourselves.
    """
    task = asyncio.ensure_future(component.do_run(event_bus))
    logger.info("Starting component: %s", component.name)
    try:
        yield
    finally:
        await _cleanup_component_task(component.name, task)


@contextlib.asynccontextmanager
async def _run_trio_component_in_proc(
        component: 'TrioIsolatedComponent',
        event_bus: EndpointAPI,
) -> AsyncIterator[None]:
    """
    Run the given TrioIsolatedComponent in the same process as ourselves.
    """
    import trio
    logger.info("Starting component: %s", component.name)
    async with trio.open_nursery() as nursery:
        nursery.start_soon(component.do_run, event_bus)
        yield
        nursery.cancel_scope.cancel()
        logger.debug("Stopped component: %s", component.name)


def _setup_standalone_component(
    component_type: Union[Type['TrioIsolatedComponent'], Type['AsyncioIsolatedComponent']],
    app_identifier: str,
) -> Tuple[Union['TrioIsolatedComponent', 'AsyncioIsolatedComponent'], Tuple[str, ...]]:
    if app_identifier == APP_IDENTIFIER_ETH1:
        app_cfg: Type[BaseAppConfig] = Eth1AppConfig
    elif app_identifier == APP_IDENTIFIER_BEACON:
        app_cfg = BeaconAppConfig
    else:
        raise ValueError("Unknown app identifier: %s", app_identifier)

    # Require a root dir to be specified as we don't want to mess with the default one.
    for action in parser._actions:
        if action.dest == 'trinity_root_dir':
            action.required = True
            break

    component_type.configure_parser(parser, subparser)
    parser.add_argument(
        '--connect-to-endpoints',
        help="A list of event bus IPC files for components we should connect to",
        nargs='+',
        default=tuple(),
    )
    args = parser.parse_args()
    # FIXME: Figure out a way to avoid having to set this.
    args.sync_mode = SYNC_FULL
    args.enable_metrics = False

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
    if args.log_levels is not None:
        for name, level in args.log_levels.items():
            get_logger(name).setLevel(level)

    trinity_config = TrinityConfig.from_parser_args(args, app_identifier, (app_cfg,))
    trinity_config.trinity_root_dir.mkdir(exist_ok=True)
    if not is_data_dir_initialized(trinity_config):
        initialize_data_dir(trinity_config)
    boot_info = BootInfo(
        args=args,
        trinity_config=trinity_config,
        min_log_level=None,
        logger_levels=None,
        profile=False,
    )
    return component_type(boot_info), args.connect_to_endpoints


@contextlib.asynccontextmanager
async def _run_eventbus_for_component(
    component: Union['TrioIsolatedComponent', 'AsyncioIsolatedComponent'],
    connect_to_endpoints: Tuple[str, ...],
) -> AsyncIterator[None]:
    from trinity.extensibility.trio import TrioIsolatedComponent
    from trinity.extensibility.asyncio import AsyncioIsolatedComponent
    if isinstance(component, TrioIsolatedComponent):
        endpoint_type: Union[Type[TrioEndpoint], Type[AsyncioEndpoint]] = TrioEndpoint
    elif isinstance(component, AsyncioIsolatedComponent):
        endpoint_type = AsyncioEndpoint
    else:
        raise ValueError("Unknown component type: %s", type(component))
    trinity_config = component._boot_info.trinity_config
    conn_config = ConnectionConfig.from_name(
        component.get_endpoint_name(), trinity_config.ipc_dir)
    async with endpoint_type.serve(conn_config) as event_bus:
        for endpoint in connect_to_endpoints:
            path = pathlib.Path(endpoint)
            if not path.is_socket():
                raise ValueError("Invalid IPC path: {path}")
            connection_config = ConnectionConfig(name=path.stem, path=path)
            logger.info("Attempting to connect to eventbus endpoint at %s", connection_config)
            await event_bus.connect_to_endpoints(connection_config)
        yield event_bus


def run_asyncio_eth1_component(component_type: Type['AsyncioIsolatedComponent']) -> None:
    import asyncio
    loop = asyncio.get_event_loop()
    got_sigint = asyncio.Event()
    loop.add_signal_handler(signal.SIGINT, got_sigint.set)
    loop.add_signal_handler(signal.SIGTERM, got_sigint.set)

    async def run() -> None:
        component, connect_to_endpoints = _setup_standalone_component(
            component_type, APP_IDENTIFIER_ETH1)
        async with _run_eventbus_for_component(component, connect_to_endpoints) as event_bus:
            async with _run_asyncio_component_in_proc(component, event_bus) as task:
                await asyncio.wait(
                    [got_sigint.wait(), task],
                    return_when=asyncio.FIRST_COMPLETED
                )

    loop.run_until_complete(run())


def _run_trio_component(component_type: Type['TrioIsolatedComponent'], app_identifier: str) -> None:
    import trio

    async def run() -> None:
        component, connect_to_endpoints = _setup_standalone_component(
            component_type, app_identifier)
        with trio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signal_aiter:
            async with _run_eventbus_for_component(component, connect_to_endpoints) as event_bus:
                async with _run_trio_component_in_proc(component, event_bus):
                    async for sig in signal_aiter:
                        return

    trio.run(run)


def run_trio_eth1_component(component_type: Type['TrioIsolatedComponent']) -> None:
    _run_trio_component(component_type, APP_IDENTIFIER_ETH1)


def run_trio_eth2_component(component_type: Type['TrioIsolatedComponent']) -> None:
    _run_trio_component(component_type, APP_IDENTIFIER_BEACON)
