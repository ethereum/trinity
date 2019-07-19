import pytest
import asyncio
from trinity._utils.async_iter import (
    contains_all
)


@pytest.mark.parametrize(
    'command',
    (
        ('trinity-beacon',),
    )
)
@pytest.mark.asyncio
async def test_plugin_boot(async_process_runner, command, tmpdir):
    testnet_path = tmpdir / "testnet"
    testnet_path.mkdir()

    proc = await asyncio.create_subprocess_shell(
        f"trinity-beacon testnet --num=1 --genesis-delay=1 --network-dir={testnet_path}"
    )
    await proc.wait()

    command = command + (f"--trinity-root-dir={testnet_path/'alice'}", )
    await async_process_runner.run(command, timeout_sec=30)
    assert await contains_all(async_process_runner.stderr, {
        "Running server",
    })
