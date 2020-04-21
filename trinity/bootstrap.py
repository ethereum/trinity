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
    Iterable,
    Sequence,
    Tuple,
    Type,
)

from async_service import AsyncioManager
from eth_utils import ValidationError

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
    ComponentAPI,
    ComponentManager,
)
from trinity.network_configurations import (
    PRECONFIGURED_NETWORKS,
)
from trinity._utils.ipc import (
    kill_process_gracefully,
    remove_dangling_ipc_files,
)
from trinity._utils.logging import (
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


def main_entry(trinity_boot: BootFn,
               app_identifier: str,
               component_types: Tuple[Type[BaseComponentAPI], ...],
               sub_configs: Sequence[Type[BaseAppConfig]]) -> None:
    if is_prerelease():
        # this modifies the asyncio logger, but will be overridden by any custom settings below
        enable_warnings_by_default()

    for component_cls in component_types:
        component_cls.configure_parser(parser, subparser)

    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    if not args.genesis and args.network_id not in PRECONFIGURED_NETWORKS:
        parser.error(
            f"Unsupported network id: {args.network_id}. To use a network besides "
            "mainnet, ropsten or goerli, you must supply a genesis file with a flag, like "
            "`--genesis path/to/genesis.json`, also you must specify a data "
            "directory with `--data-dir path/to/data/directory`"
        )

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

    trinity_config = load_trinity_config_from_parser_args(parser,
                                                          args,
                                                          app_identifier,
                                                          sub_configs)

    ensure_data_dir_is_initialized(trinity_config)

    # +---------------+
    # | LOGGING SETUP |
    # +---------------+

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

    # This prints out the ASCII "trinity" header in the terminal
    display_launch_logs(trinity_config)

    # Setup the log listener which child processes relay their logs through
    log_listener = IPCListener(handler_stderr, handler_file)

    # Determine what logging level child processes should use.
    boot_info = BootInfo(
        args=args,
        trinity_config=trinity_config,
        min_log_level=min_log_level,
        logger_levels=logger_levels,
        profile=bool(args.profile),
    )

    # Let the components do runtime validation
    for component_cls in component_types:
        try:
            component_cls.validate_cli(boot_info)
        except ValidationError as exc:
            parser.exit(message=str(exc))

    # Components can provide a subcommand with a `func` which does then control
    # the entire process from here.
    if hasattr(args, 'func'):
        args.func(args, trinity_config)
        return

    runtime_component_types = tuple(
        component_cls
        for component_cls in component_types
        if issubclass(component_cls, ComponentAPI)
    )

    with log_listener.run(trinity_config.logging_ipc_path):

        processes = trinity_boot(boot_info)

        loop = asyncio.get_event_loop()

        def kill_trinity_with_reason(reason: str) -> None:
            kill_trinity_gracefully(
                trinity_config,
                logger,
                processes,
                reason=reason
            )

        component_manager_service = ComponentManager(
            boot_info,
            runtime_component_types,
        )
        manager = AsyncioManager(component_manager_service)

        loop.add_signal_handler(
            signal.SIGTERM,
            manager.cancel,
            'SIGTERM',
        )
        loop.add_signal_handler(
            signal.SIGINT,
            component_manager_service.shutdown,
            'CTRL+C',
        )

        try:
            loop.run_until_complete(manager.run())
        except BaseException as err:
            logger.error("Error during trinity run: %r", err)
            raise
        finally:
            kill_trinity_with_reason(component_manager_service.reason)
            if trinity_config.trinity_tmp_root_dir:
                shutil.rmtree(trinity_config.trinity_root_dir)


def display_launch_logs(trinity_config: TrinityConfig) -> None:
    logger = logging.getLogger('trinity')
    logger.info(TRINITY_HEADER)
    logger.info("Started main process (pid=%d)", os.getpid())
    logger.info(construct_trinity_client_identifier())
    logger.info("Trinity DEBUG log file is created at %s", str(trinity_config.logfile_path))


def kill_trinity_gracefully(trinity_config: TrinityConfig,
                            logger: logging.Logger,
                            processes: Iterable[multiprocessing.Process],
                            reason: str = None) -> None:
    # When a user hits Ctrl+C in the terminal, the SIGINT is sent to all processes in the
    # foreground *process group*, so both our networking and database processes will terminate
    # at the same time and not sequentially as we'd like. That shouldn't be a problem but if
    # we keep getting unhandled BrokenPipeErrors/ConnectionResetErrors like reported in
    # https://github.com/ethereum/py-evm/issues/827, we might want to change the networking
    # process' signal handler to wait until the DB process has terminated before doing its
    # thing.
    # Notice that we still need the kill_process_gracefully() calls here, for when the user
    # simply uses 'kill' to send a signal to the main process, but also because they will
    # perform a non-gracefull shutdown if the process takes too long to terminate.

    hint = f" ({reason})" if reason else f""
    logger.info('Shutting down Trinity%s', hint)

    for process in processes:
        # Our sub-processes will have received a SIGINT already (see comment above), so here we
        # wait 2s for them to finish cleanly, and if they fail we kill them for real.
        process.join(2)
        if process.is_alive():
            kill_process_gracefully(process, logger)
        logger.info('%s process (pid=%d) terminated', process.name, process.pid)

    remove_dangling_ipc_files(logger, trinity_config.ipc_dir)

    argparse.ArgumentParser().exit(message=f"Trinity shutdown complete{hint}\n")
