import tempfile
import logging
from pathlib import Path
import sys

from asyncio_run_in_process import run_in_process

import pytest

from trinity._utils.logging import QueueListener, QueueHandler


@pytest.fixture
def ipc_path():
    with tempfile.TemporaryDirectory() as dir:
        yield Path(dir) / "logging.ipc"


@pytest.mark.asyncio
async def test_queued_logging(ipc_path):
    class StoreHandler(logging.Handler):
        def __init__(self):
            self.logs = []
            super().__init__()

        def handle(self, record):
            self.logs.append(record)

    async def do_other_process_logging(ipc_path):
        queue_handler = QueueHandler.connect(ipc_path)
        queue_handler.setLevel(logging.DEBUG)
        logger = logging.getLogger(str(ipc_path))
        logger.addHandler(queue_handler)
        logger.setLevel(logging.DEBUG)

        logger.error('error log')
        logger.info('info log')
        logger.debug('debug log')

        queue_handler.close()

    logger = logging.getLogger(str(ipc_path))

    handler = StoreHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    queue_listener = QueueListener(handler)

    with queue_listener.run(ipc_path):
        assert len(handler.logs) == 0
        await run_in_process(do_other_process_logging, ipc_path)
        assert len(handler.logs) == 3

    error_log, info_log, debug_log = handler.logs

    assert 'error log' in error_log.message
    assert 'info log' in info_log.message
    assert 'debug log' in debug_log.message
