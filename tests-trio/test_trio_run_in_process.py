import tempfile

import pytest

import trio

from p2p.trio_run_in_process import run_in_process


@pytest.mark.trio
async def test_run_in_process():
    async def touch_file(path: trio.Path):
        await path.touch()

    with tempfile.TemporaryDirectory() as base_dir:
        path = trio.Path(base_dir) / 'test.txt'
        assert not await path.exists()
        await run_in_process(touch_file, path)
        assert await path.exists()


@pytest.mark.trio
async def test_run_in_process_with_result():
    async def return7():
        return 7

    result = await run_in_process(return7)
    assert result == 7


@pytest.mark.trio
async def test_run_in_process_with_error():
    async def raise_err():
        raise ValueError("Some err")

    with pytest.raises(ValueError, match="Some err"):
        await run_in_process(raise_err)
