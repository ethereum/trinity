from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
import logging
import time

from trinity.config import (
    TrinityConfig,
)
from trinity.extensibility import Application
from trinity._utils.ipc import (
    kill_process_id_gracefully,
    remove_dangling_ipc_files,
)


class FixUncleanShutdownComponent(Application):
    logger = logging.getLogger('trinity.components.fix_unclean_shutdown.FixUncleanShutdown')

    @classmethod
    def configure_parser(cls,
                         arg_parser: ArgumentParser,
                         subparser: _SubParsersAction) -> None:

        attach_parser = subparser.add_parser(
            'fix-unclean-shutdown',
            help='close any dangling processes from a previous unclean shutdown',
        )

        attach_parser.set_defaults(func=cls.fix_unclean_shutdown)

    @classmethod
    def fix_unclean_shutdown(cls, args: Namespace, trinity_config: TrinityConfig) -> None:
        cls.logger.info("Cleaning up unclean shutdown...")

        cls.logger.info("Searching for process id files in %s...", trinity_config.data_dir)
        pidfiles = tuple(trinity_config.pid_dir.glob('*.pid'))
        if len(pidfiles) > 1:
            cls.logger.info('Found %d processes from a previous run. Closing...', len(pidfiles))
        elif len(pidfiles) == 1:
            cls.logger.info('Found 1 process from a previous run. Closing...')
        else:
            cls.logger.info('Found 0 processes from a previous run. No processes to kill.')

        for pidfile in pidfiles:
            process_id = int(pidfile.read_text())
            kill_process_id_gracefully(process_id, time.sleep, cls.logger)
            try:
                pidfile.unlink()
                cls.logger.info(
                    'Manually removed %s after killing process id %d', pidfile, process_id
                )
            except FileNotFoundError:
                cls.logger.debug(
                    'pidfile %s was gone after killing process id %d', pidfile, process_id
                )

        remove_dangling_ipc_files(cls.logger, trinity_config.ipc_dir)
