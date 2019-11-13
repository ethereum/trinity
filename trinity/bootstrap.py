import asyncio
import argcomplete
import logging
import os
import signal
import sys
from typing import (
    Sequence,
    Tuple,
    Type,
    TYPE_CHECKING,
)

from p2p.service import AsyncioManager

from trinity.exceptions import (
    AmbigiousFileSystem,
    MissingPath,
)
from trinity.initialization import (
    initialize_data_dir,
    is_data_dir_initialized,
)
from trinity.cli_parser import (
    parser,
    subparser,
)
from trinity.config import (
    BaseAppConfig,
    TrinityConfig,
)
from trinity.extensibility import (
    ApplicationComponentAPI,
    BaseComponentAPI,
    TrinityBootInfo,
)
from trinity.network_configurations import (
    PRECONFIGURED_NETWORKS,
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

if TYPE_CHECKING:
    from .main import TrinityMain  # noqa: F401


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


def main_entry(trinity_service_class: Type['TrinityMain'],
               app_identifier: str,
               component_types: Tuple[Type[BaseComponentAPI], ...],
               sub_configs: Sequence[Type[BaseAppConfig]]) -> None:
    for component_cls in component_types:
        component_cls.configure_parser(parser, subparser)

    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    if not args.genesis and args.network_id not in PRECONFIGURED_NETWORKS:
        raise NotImplementedError(
            f"Unsupported network id: {args.network_id}. To use a network besides "
            "mainnet or ropsten, you must supply a genesis file with a flag, like "
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

    if is_prerelease():
        # this modifies the asyncio logger, but will be overridden by any custom settings below
        enable_warnings_by_default()

    log_level_to_stderr = args.stderr_log_level or common_log_level
    handler_stderr = setup_stderr_logging(level=log_level_to_stderr)

    if args.log_levels:
        set_logger_levels(args.log_levels)

    try:
        trinity_config = TrinityConfig.from_parser_args(args, app_identifier, sub_configs)
    except AmbigiousFileSystem:
        parser.error(TRINITY_AMBIGIOUS_FILESYSTEM_INFO)

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

    log_level_to_file = args.file_log_level or common_log_level
    handler_file = setup_file_logging(
        logfile_path=trinity_config.logfile_path,
        level=log_level_to_file,
    )

    logger = logging.getLogger()
    logger.setLevel(log_level_to_stderr or logging.INFO)

    display_launch_logs(trinity_config)

    # compute the minimum configured log level across all configured loggers.
    min_configured_log_level = min(
        handler_stderr.level,
        handler_file.level,
        *(args.log_levels or {}).values()
    )

    log_listener = IPCListener(handler_stderr, handler_file)

    boot_info = TrinityBootInfo(
        args=args,
        trinity_config=trinity_config,
        log_level=min_configured_log_level,
        logger_levels=(args.log_levels if args.log_levels else {}),
    )

    # Components can provide a subcommand with a `func` which does then control
    # the entire process from here.
    if hasattr(args, 'func'):
        args.func(args, trinity_config)
        return

    # This is a hack for the eth2.0 interop component
    if hasattr(args, 'munge_func'):
        args.munge_func(args, trinity_config)

    application_component_types = tuple(
        component_cls
        for component_cls
        in component_types
        if issubclass(component_cls, ApplicationComponentAPI)
    )

    trinity_service = trinity_service_class(
        boot_info=boot_info,
        component_types=application_component_types,
    )

    manager = AsyncioManager(trinity_service)

    with log_listener.run(trinity_config.logging_ipc_path):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, manager.cancel)
        loop.add_signal_handler(signal.SIGINT, manager.cancel)

        try:
            loop.run_until_complete(manager.run())
        except KeyboardInterrupt:
            pass
        finally:
            try:
                loop.run_until_complete(manager.wait_stopped())
            except KeyboardInterrupt:
                pass

            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except (KeyboardInterrupt, GeneratorExit):
                pass

            loop.stop()
            if trinity_config.trinity_tmp_root_dir:
                import shutil
                shutil.rmtree(trinity_config.trinity_root_dir)
    sys.exit(0)


def display_launch_logs(trinity_config: TrinityConfig) -> None:
    logger = logging.getLogger('trinity')
    logger.info(TRINITY_HEADER)
    logger.info("Started main process (pid=%d)", os.getpid())
    logger.info(construct_trinity_client_identifier())
    logger.info("Trinity DEBUG log file is created at %s", str(trinity_config.logfile_path))
