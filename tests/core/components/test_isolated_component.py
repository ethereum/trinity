from argparse import Namespace
import asyncio
import logging
import os
from pathlib import Path
import tempfile

from async_service import background_asyncio_service
import pytest

from lahja import EndpointAPI, BaseEvent

from p2p.service import BaseService, run_service

from trinity._utils.chains import (
    get_local_data_dir,
)
from trinity._utils.logging import IPCListener
from trinity.boot_info import BootInfo
from trinity.config import TrinityConfig
from trinity.extensibility import AsyncioIsolatedComponent, ComponentManager


class IsStarted(BaseEvent):
    def __init__(self, path):
        self.path = path


class AsyncioComponentService(BaseService):
    touch_path = None

    def __init__(self, event_bus) -> None:
        super().__init__()
        self.event_bus = event_bus

    async def _run(self) -> None:
        self.logger.debug('Broadcasting `IsStarted`')
        path = Path(tempfile.NamedTemporaryFile().name)
        await self.event_bus.broadcast(IsStarted(path))
        try:
            self.logger.debug('Waiting for cancellation')
            await self.cancellation()
        finally:
            self.logger.debug('Got cancellation: touching `%s`', self.touch_path)
            path.touch()
            self.logger.debug('EXITING')


class AsyncioComponentForTest(AsyncioIsolatedComponent):
    name = "component-test"
    endpoint_name = 'component-test'
    logger = logging.getLogger('trinity.testing.ComponentForTest')

    @property
    def is_enabled(self) -> bool:
        return True

    @classmethod
    async def do_run(cls, boot_info: BootInfo, event_bus: EndpointAPI) -> None:
        cls.logger.debug('Entered `do_run`')
        service = AsyncioComponentService(event_bus)
        async with run_service(service):
            cls.logger.debug('Running service')
            try:
                await service.cancellation()
            finally:
                cls.logger.debug('Got cancellation')


@pytest.fixture
def trinity_config(xdg_trinity_root):
    data_dir = get_local_data_dir('mainnet', xdg_trinity_root)
    return TrinityConfig(
        network_id=1,
        data_dir=data_dir,
    )


@pytest.fixture
def boot_info(trinity_config):
    return BootInfo(
        args=Namespace(),
        trinity_config=trinity_config,
        profile=False,
        child_process_log_level=logging.INFO,
        logger_levels={},
    )


@pytest.fixture
def log_listener(trinity_config):
    logger = logging.getLogger()
    assert logger.handlers
    listener = IPCListener(*logger.handlers)
    os.makedirs(trinity_config.ipc_dir, exist_ok=True)
    with listener.run(trinity_config.logging_ipc_path):
        yield


@pytest.mark.asyncio
async def test_asyncio_isolated_component(boot_info,
                                          log_listener):
    # Test the lifecycle management for isolated process components to be sure
    # they start and stop as expected
    component_manager = ComponentManager(boot_info, (AsyncioComponentForTest,), lambda reason: None)

    async with background_asyncio_service(component_manager):
        event_bus = await component_manager.get_event_bus()

        got_started = asyncio.Future()

        event_bus.subscribe(IsStarted, lambda ev: got_started.set_result(ev.path))

        touch_path = await asyncio.wait_for(got_started, timeout=10)
        assert not touch_path.exists()

    for _ in range(1000):
        if not touch_path.exists():
            await asyncio.sleep(0.001)
        else:
            break
    else:
        assert touch_path.exists()
