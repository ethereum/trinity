import asyncio
import argparse
import argcomplete
import logging
import multiprocessing
import os
import shutil
import signal
from typing import (
    Callable,
    Dict,
    Sequence,
    Tuple,
    Type,
    cast,
)

from asyncio_run_in_process import open_in_process
from async_service import AsyncioManager

from eth.db.backends.level import LevelDB

from eth_utils import ValidationError

from trinity.db.manager import DBManager
from trinity.exceptions import (
    AmbigiousFileSystem,
    MissingPath,
)
from trinity.initialization import (
    initialize_data_dir,
    is_data_dir_initialized,
)
from trinity.boot_info import BootInfo
from trinity.cli_parser import (
    parser,
    subparser,
)
from trinity.config import (
    BaseAppConfig,
    TrinityConfig,
)
from trinity.extensibility import (
    BaseComponentAPI,
    BaseIsolatedComponent,
    ComponentAPI,
    ComponentManager,
)
from trinity.network_configurations import (
    PRECONFIGURED_NETWORKS,
)
from trinity._utils.ipc import (
    remove_dangling_ipc_files,
    wait_for_ipc,
)
from trinity._utils.logging import (
    child_process_logging,
    enable_warnings_by_default,
    set_logger_levels,
    setup_file_logging,
    setup_stderr_logging,
    IPCListener,
)
from trinity._utils.version import (
    construct_trinity_client_identifier,
    is_prerelease,
)


TRINITY_HEADER = "\n".join((
    "\n"
    r"      ______     _       _ __       ",
    r"     /_  __/____(_)___  (_) /___  __",
    r"      / / / ___/ / __ \/ / __/ / / /",
    r"     / / / /  / / / / / / /_/ /_/ / ",
    r"    /_/ /_/  /_/_/ /_/_/\__/\__, /  ",
    r"                           /____/   ",
))

TRINITY_AMBIGIOUS_FILESYSTEM_INFO = (
    "Could not initialize data directory\n\n"
    "   One of these conditions must be met:\n"
    "   * HOME environment variable set\n"
    "   * XDG_TRINITY_ROOT environment variable set\n"
    "   * TRINITY_DATA_DIR environment variable set\n"
    "   * --data-dir command line argument is passed\n"
    "\n"
    "   In case the data directory is outside of the trinity root directory\n"
    "   Make sure all paths are pre-initialized as Trinity won't attempt\n"
    "   to create directories outside of the trinity root directory\n"
)


BootFn = Callable[[BootInfo], Tuple[multiprocessing.Process, ...]]
SubConfigs = Sequence[Type[BaseAppConfig]]


def load_trinity_config_from_parser_args(parser: argparse.ArgumentParser,
                                         args: argparse.Namespace,
                                         app_identifier: str,
                                         sub_configs: SubConfigs) -> TrinityConfig:
    try:
        return TrinityConfig.from_parser_args(args, app_identifier, sub_configs)
    except AmbigiousFileSystem:
        parser.error(TRINITY_AMBIGIOUS_FILESYSTEM_INFO)


def ensure_data_dir_is_initialized(trinity_config: TrinityConfig) -> None:
    if not is_data_dir_initialized(trinity_config):
        # TODO: this will only work as is for chains with known genesis
        # parameters.  Need to flesh out how genesis parameters for custom
        # chains are defined and passed around.
        try:
            initialize_data_dir(trinity_config)
        except AmbigiousFileSystem:
            parser.error(TRINITY_AMBIGIOUS_FILESYSTEM_INFO)
        except MissingPath as e:
            parser.error(
                "\n"
                f"It appears that {e.path} does not exist. "
                "Trinity does not attempt to create directories outside of its root path. "
                "Either manually create the path or ensure you are using a data directory "
                "inside the XDG_TRINITY_ROOT path"
            )


def configure_parsers(parser: argparse.ArgumentParser,
                      subparser: argparse._SubParsersAction,
                      component_types: Tuple[Type[BaseComponentAPI], ...]) -> None:
    for component_cls in component_types:
        component_cls.configure_parser(parser, subparser)


def parse_and_validate_cli() -> argparse.Namespace:
    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    if not args.genesis and args.network_id not in PRECONFIGURED_NETWORKS:
        parser.error(
            f"Unsupported network id: {args.network_id}. To use a network besides "
            "mainnet, ropsten or goerli, you must supply a genesis file with a flag, like "
            "`--genesis path/to/genesis.json`, also you must specify a data "
            "directory with `--data-dir path/to/data/directory`"
        )

    return args


def resolve_common_log_level_or_error(args: argparse.Namespace) -> str:
    # The `common_log_level` is derived from `--log-level <Level>` / `-l <Level>` without
    # specifying any module. If present, it is used for both `stderr` and `file` logging.
    common_log_level = args.log_levels and args.log_levels.get(None)
    has_ambigous_logging_config = ((
        common_log_level is not None and
        args.stderr_log_level is not None
    ) or (
        common_log_level is not None and
        args.file_log_level is not None
    ))

    if has_ambigous_logging_config:
        parser.error(
            f"""\n
            Ambiguous logging configuration: The `--log-level (-l)` flag sets the
            log level for both file and stderr logging.
            To configure different log level for file and stderr logging,
            remove the `--log-level` flag and use `--stderr-log-level` and/or
            `--file-log-level` separately.
            Alternatively, remove the `--stderr-log-level` and/or `--file-log-level`
            flags to share one single log level across both handlers.
            """
        )
    else:
        return common_log_level


LoggingResult = Tuple[Tuple[logging.Handler, ...], int, Dict[str, int]]


def install_logging(args: argparse.Namespace,
                    trinity_config: TrinityConfig,
                    common_log_level: str) -> LoggingResult:
    # Setup logging to stderr
    stderr_logger_level = (
        args.stderr_log_level
        if args.stderr_log_level is not None
        else (common_log_level if common_log_level is not None else logging.INFO)
    )
    handler_stderr = setup_stderr_logging(stderr_logger_level)

    # Setup file based logging
    file_logger_level = (
        args.file_log_level
        if args.file_log_level is not None
        else (common_log_level if common_log_level is not None else logging.DEBUG)
    )
    handler_file = setup_file_logging(trinity_config.logfile_path, file_logger_level)

    # Set the individual logger levels that have been specified.
    logger_levels = {} if args.log_levels is None else args.log_levels
    # async-service's DEBUG logs completely drowns our stuff (i.e. more than 95% of all our DEBUG
    # logs), so unless explicitly overridden, we limit it to INFO.
    if 'async_service' not in logger_levels:
        logger_levels['async_service'] = logging.INFO
    set_logger_levels(logger_levels)

    min_log_level = min(
        stderr_logger_level,
        file_logger_level,
        *logger_levels.values(),
    )
    # We need to use our minimum level on the root logger to ensure anything logged via a
    # sub-logger using the default level will reach all our handlers. The handlers will then filter
    # those based on their configured levels.
    logger = logging.getLogger()
    logger.setLevel(min_log_level)

    return (handler_stderr, handler_file), min_log_level, logger_levels


def validate_component_cli(component_types: Tuple[Type[BaseComponentAPI], ...],
                           boot_info: BootInfo) -> None:
    # Let the components do runtime validation
    for component_cls in component_types:
        try:
            component_cls.validate_cli(boot_info)
        except ValidationError as exc:
            parser.exit(message=str(exc))


async def run_db_manager(
        boot_info: BootInfo,
        get_base_db_fn: Callable[[BootInfo], LevelDB]) -> None:
    with child_process_logging(boot_info):
        trinity_config = boot_info.trinity_config
        manager = DBManager(get_base_db_fn(boot_info))
        with trinity_config.process_id_file('database'):
            with manager.run(trinity_config.database_ipc_path):
                loop = asyncio.get_event_loop()
                try:
                    await loop.run_in_executor(None, manager.wait_stopped)
                finally:
                    # We always need to call stop() before returning as asyncio can't cancel the
                    # thread started by run_in_executor() and that would prevent
                    # open_in_process(run_db_manager, ...) from returning.
                    manager.stop()


async def _run(
        boot_info: BootInfo,
        get_base_db_fn: Callable[[BootInfo], LevelDB],
        component_manager: AsyncioManager) -> None:
    logger = logging.getLogger('trinity')
    start_new_session = True
    if os.getenv('TRINITY_SINGLE_PROCESS_GROUP') == "1":
        # This is needed because some of our integration tests rely on all processes being in
        # a single process group.
        start_new_session = False
    async with open_in_process(
            run_db_manager,
            boot_info,
            get_base_db_fn,
            subprocess_kwargs={'start_new_session': start_new_session},
    ) as db_proc:
        logger.info("Started DB server process (pid=%d)", db_proc.pid)
        try:
            wait_for_ipc(boot_info.trinity_config.database_ipc_path)
        except TimeoutError:
            logger.error("Timeout waiting for database to start.  Exiting...")
            argparse.ArgumentParser().error(message="Timed out waiting for database start")
            return None

        try:
            await component_manager.run()
        finally:
            try:
                await component_manager.stop()
            finally:
                logger.info("Terminating DB process")
                db_proc.send_signal(signal.SIGINT)


def run(component_types: Tuple[Type[BaseComponentAPI], ...],
        boot_info: BootInfo,
        get_base_db_fn: Callable[[BootInfo], LevelDB]) -> None:
    runtime_component_types = tuple(
        cast(Type[BaseIsolatedComponent], component_cls)
        for component_cls in component_types
        if issubclass(component_cls, ComponentAPI)
    )

    trinity_config = boot_info.trinity_config

    component_manager_service = ComponentManager(
        boot_info,
        runtime_component_types,
    )
    component_manager_manager = AsyncioManager(component_manager_service)

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGTERM,
        component_manager_manager.cancel,
        'SIGTERM',
    )
    loop.add_signal_handler(
        signal.SIGINT,
        component_manager_service.shutdown,
        'CTRL+C',
    )

    logger = logging.getLogger()
    try:
        loop.run_until_complete(_run(boot_info, get_base_db_fn, component_manager_manager))
    except BaseException:
        logger.exception("Error during trinity run")
        raise
    finally:
        reason = component_manager_service.reason
        hint = f" ({reason})" if reason else f""
        logger.info('Shutting down Trinity%s', hint)
        remove_dangling_ipc_files(logger, trinity_config.ipc_dir)
        argparse.ArgumentParser().exit(message=f"Trinity shutdown complete{hint}\n")
        if trinity_config.trinity_tmp_root_dir:
            shutil.rmtree(trinity_config.trinity_root_dir)


BootPrologueData = Tuple[BootInfo, Tuple[logging.Handler, ...]]


def construct_boot_info(app_identifier: str,
                        component_types: Tuple[Type[BaseComponentAPI], ...],
                        sub_configs: Sequence[Type[BaseAppConfig]]) -> BootPrologueData:
    if is_prerelease():
        # this modifies the asyncio logger, but will be overridden by any custom settings below
        enable_warnings_by_default()

    configure_parsers(parser, subparser, component_types)

    args = parse_and_validate_cli()

    common_log_level = resolve_common_log_level_or_error(args)

    trinity_config = load_trinity_config_from_parser_args(parser,
                                                          args,
                                                          app_identifier,
                                                          sub_configs)

    ensure_data_dir_is_initialized(trinity_config)
    handlers, min_log_level, logger_levels = install_logging(
        args,
        trinity_config,
        common_log_level
    )

    boot_info = BootInfo(
        args=args,
        trinity_config=trinity_config,
        min_log_level=min_log_level,
        logger_levels=logger_levels,
        profile=bool(args.profile),
    )

    validate_component_cli(component_types, boot_info)

    return boot_info, handlers


def main_entry(trinity_boot: BootFn,
               get_base_db_fn: Callable[[BootInfo], LevelDB],
               app_identifier: str,
               component_types: Tuple[Type[BaseComponentAPI], ...],
               sub_configs: Sequence[Type[BaseAppConfig]]) -> None:
    boot_info, handlers = construct_boot_info(app_identifier, component_types, sub_configs)
    args = boot_info.args
    trinity_config = boot_info.trinity_config

    # Components can provide a subcommand with a `func` which does then control
    # the entire process from here.
    if hasattr(args, 'func'):
        args.func(args, trinity_config)
        return

    # This prints out the ASCII "trinity" header in the terminal
    display_launch_logs(trinity_config)

    # Setup the log listener which child processes relay their logs through
    with IPCListener(*handlers).run(trinity_config.logging_ipc_path):
        trinity_boot(boot_info)
        run(component_types, boot_info, get_base_db_fn)


def display_launch_logs(trinity_config: TrinityConfig) -> None:
    logger = logging.getLogger('trinity')
    logger.info(TRINITY_HEADER)
    logger.info("Started main process (pid=%d)", os.getpid())
    logger.info(construct_trinity_client_identifier())
    logger.info("Trinity DEBUG log file is created at %s", str(trinity_config.logfile_path))
