from argparse import (
    ArgumentParser,
    Namespace,
    _ArgumentGroup,
    _SubParsersAction,
)
import asyncio
import logging
import subprocess
import time
from typing import (
    Union,
)

from trinity.config import (
    Eth1AppConfig,
    TrinityConfig,
)
from trinity.db.eth1.manager import (
    create_db_consumer_manager
)
from trinity.endpoint import (
    TrinityEventBusEndpoint,
)
from trinity.extensibility import (
    AsyncioIsolatedPlugin,
    BaseMainProcessPlugin,
)
from trinity._utils.shutdown import (
    exit_with_endpoint_and_services,
)
from .tracker import (
    PerformanceTracker
)


DEFAULT_TARGET_BLOCK = 100000
DEFAULT_INTERVAL = 1
DEFAULT_LABEL = f"Unnamed run ({time.time()})"
DEFAULT_REPORT_PATH = "performance_report.txt"
DEFAULT_RUN_COUNT = 3
DEFAULT_TRINITY_EXEC = "trinity"


def add_shared_cli_args(parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
        parser.add_argument(
            '--track-perf-target-block',
            type=int,
            default=DEFAULT_TARGET_BLOCK,
            help='Target block number at which a run is finished',
        )

        parser.add_argument(
            '--track-perf-label',
            help='Label that identifies the current run',
            default=DEFAULT_LABEL,
        )

        parser.add_argument(
            '--track-perf-report-path',
            help='Report file path',
            default=DEFAULT_REPORT_PATH,
        )

        parser.add_argument(
            '--track-perf-interval',
            help='Interval at which current block is looked up',
            type=int,
            default=DEFAULT_INTERVAL,
        )


class TrackerRunnerPlugin(BaseMainProcessPlugin):
    """
    Run Trinity a specified number of times to take performance measures.
    Performance stats for each run are written to a single file and Trinity's
    database is archived before every run.
    """

    @property
    def name(self) -> str:
        return "Tracker Runner"

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:

        track_perf_parser = subparser.add_parser(
            'track-perf',
            help='Repeatedly sync up to target block and track performance',
        )

        track_perf_parser.add_argument(
            '--track-perf-run-count',
            type=int,
            default=DEFAULT_RUN_COUNT,
            help='Number of times to run Trinity',
        )

        track_perf_parser.add_argument(
            '--track-perf-pre-exec-cmd',
            help='Command hook executed before every run',
        )

        track_perf_parser.add_argument(
            '--track-perf-trinity-exec',
            default=DEFAULT_TRINITY_EXEC,
            help='Command or path to Trinity executable',
        )

        track_perf_parser.add_argument(
            '--track-perf-trinity-args',
            help='Pass additional arguments to Trinity',
        )

        add_shared_cli_args(track_perf_parser)

        track_perf_parser.set_defaults(func=cls.start_tracking)

    @classmethod
    def start_tracking(cls, args: Namespace, trinity_config: TrinityConfig) -> None:
        logger = cls.get_logger()
        file_handler = logging.handlers.RotatingFileHandler(args.track_perf_report_path)
        logger.addHandler(file_handler)

        logger.info(
            "Performance Tracking (%s runs up to block #%s)",
            args.track_perf_run_count,
            args.track_perf_target_block
        )
        logger.info('=============================================')

        for run_number in range(1, args.track_perf_run_count + 1):
            db_path = trinity_config.get_app_config(Eth1AppConfig).database_dir
            db_new_name = f"{db_path.name}_{time.time()}"
            if db_path.exists():
                db_path.rename(db_path.with_name(db_new_name))

            logger.info("Run %s out of %s", run_number, args.track_perf_run_count)

            if args.track_perf_pre_exec_cmd:
                logger.info("Runing pre exec command hook: %s ", args.track_perf_pre_exec_cmd)
                pre_hook = subprocess.run(
                    args.track_perf_pre_exec_cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                # A common hook would be something like `git checkout -` to toggle between branches
                # This output allows us to identify the branch in the report
                logger.info(pre_hook.stdout.decode())
                logger.info(pre_hook.stderr.decode())

            logger.info("Archived previous database to %s", db_new_name)

            cmd = (
                f"{args.track_perf_trinity_exec} --track-perf"
                f" --track-perf-label '{args.track_perf_label} #{run_number}'"
                f" --track-perf-target-block {args.track_perf_target_block}"
                f" --track-perf-interval {args.track_perf_interval}"
                f" --track-perf-report-path '{args.track_perf_report_path}'"
            )

            # Pass any additional params on to Trinity
            # (e.g. --track-perf-trininty-args='--disable-discovery')
            if args.track_perf_trinity_args is not None:
                cmd += f" {args.track_perf_trinity_args}"

            logger.info("Running cmd=%s", cmd)
            subprocess.run(cmd, shell=True)


class PerformanceTrackerPlugin(AsyncioIsolatedPlugin):
    """
    Track the time from syncing the first block to some specified target block.
    """

    @property
    def name(self) -> str:
        return "Performance Tracker"

    def on_ready(self, manager_eventbus: TrinityEventBusEndpoint) -> None:
        if self.boot_info.args.track_perf:
            self.start()

    @classmethod
    def configure_parser(cls, arg_parser: ArgumentParser, subparser: _SubParsersAction) -> None:
        perf_parser = arg_parser.add_argument_group('Performance Tracker')

        perf_parser.add_argument(
            '--track-perf',
            action='store_true',
            help='Enable performance tracking',
        )

        add_shared_cli_args(perf_parser)

    def do_start(self) -> None:
        db_manager = create_db_consumer_manager(self.boot_info.trinity_config.database_ipc_path)
        chain_db = db_manager.get_chaindb()  # type: ignore

        perf_tracker = PerformanceTracker(
            chain_db,
            self.event_bus,
            self.boot_info.args.track_perf_label,
            self.boot_info.args.track_perf_target_block,
            self.boot_info.args.track_perf_interval,
            self.boot_info.args.track_perf_report_path,
        )
        asyncio.ensure_future(exit_with_endpoint_and_services(self.event_bus, perf_tracker))
        asyncio.ensure_future(perf_tracker.run())
