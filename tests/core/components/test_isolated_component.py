from argparse import Namespace
import asyncio
import logging
import os

from async_service import background_asyncio_service
import pytest

from trinity._utils.chains import get_local_data_dir
from trinity._utils.logging import IPCListener
from trinity.boot_info import BootInfo
from trinity.config import TrinityConfig
from trinity.extensibility import ComponentManager
from trinity.tools._component_isolation import (
    AsyncioComponentForTest,
    IsStarted,
    TrioComponentForTest,
)


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
        min_log_level=logging.DEBUG,
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


# XXX: This test only completes/passes when using AsyncioComponentForTest because
# asyncio-run-in-process sends a SIGINT followed by a SIGTERM to the subprocess running our
# component. The SIGTERM should not be needed as our components should exit cleanly after a
# SIGINT, but currently that's not the case: https://github.com/ethereum/trinity/issues/1711.
@pytest.mark.parametrize("component", (AsyncioComponentForTest, TrioComponentForTest))
@pytest.mark.asyncio
async def test_isolated_component(boot_info, log_listener, component, request):
    # Test the lifecycle management for isolated process components to be sure
    # they start and stop as expected
    component_manager = ComponentManager(boot_info, (component,))

    async with background_asyncio_service(component_manager) as cm_manager:
        event_bus = await component_manager.get_event_bus()

        got_started = asyncio.Future()

        event_bus.subscribe(IsStarted, lambda ev: got_started.set_result(ev.path))

        touch_path = await asyncio.wait_for(got_started, timeout=10)

        def delete_touch_path():
            if touch_path.exists():
                touch_path.unlink()

        request.addfinalizer(delete_touch_path)
        assert not touch_path.exists()
        component_manager.shutdown('exiting component manager')
        await cm_manager.wait_finished()

    assert touch_path.exists()
