import asyncio
import logging

import pytest

from async_service.asyncio import AsyncioManager

from trinity.config import TrinityConfig
from trinity.boot_info import BootInfo
from trinity.extensibility.component_manager import ComponentManager
from trinity.components.builtin.peer_discovery.component import EmptyComponent
from trinity._utils.chains import get_local_data_dir


@pytest.mark.asyncio
async def test_1435(xdg_trinity_root):
    data_dir = get_local_data_dir('muffin', xdg_trinity_root)
    trinity_config = TrinityConfig(1, app_identifier="eth1", data_dir=data_dir)
    trinity_config.ipc_dir.mkdir(parents=True)
    boot_info = BootInfo(
        args=None, trinity_config=trinity_config, profile=False,
        child_process_log_level=logging.DEBUG, logger_levels={})
    components = [EmptyComponent] * 30
    s = ComponentManager(boot_info, components, None)
    manager = AsyncioManager(s)

    async def cancel_manager():
        await asyncio.sleep(40)
        manager.cancel()

    asyncio.ensure_future(cancel_manager())
    await manager.run()
