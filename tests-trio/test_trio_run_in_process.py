import tempfile

import pytest

import trio

from p2p.trio_run_in_process import run_in_process, open_in_process


@pytest.mark.trio
async def test_run_in_process_touch_file():
    async def touch_file(path: trio.Path):
        await path.touch()

    with trio.fail_after(2):
        with tempfile.TemporaryDirectory() as base_dir:
            path = trio.Path(base_dir) / 'test.txt'
            assert not await path.exists()
            await run_in_process(touch_file, path)
            assert await path.exists()


@pytest.mark.trio
async def test_run_in_process_with_result():
    async def return7():
        return 7

    with trio.fail_after(2):
        result = await run_in_process(return7)
    assert result == 7


@pytest.mark.trio
async def test_run_in_process_with_error():
    async def raise_err():
        raise ValueError("Some err")

    with trio.fail_after(2):
        with pytest.raises(ValueError, match="Some err"):
            await run_in_process(raise_err)


@pytest.mark.trio
async def test_open_in_proc_can_terminate():
    async def do_sleep_forever():
        import trio
        await trio.sleep_forever()

    with trio.fail_after(2):
        async with open_in_process(do_sleep_forever) as proc:
            proc.terminate()
        assert proc.returncode == 1


@pytest.mark.skip
@pytest.mark.trio
async def test_run_in_process_handles_keyboard_interrupt():
    async def monitor_for_interrupt(path):
        import trio
        try:
            await trio.sleep_forever()
        except KeyboardInterrupt:
            await path.touch()
        else:
            assert False

    with trio.fail_after(2):
        with tempfile.TemporaryDirectory() as base_dir:
            # TODO
            path = trio.Path(base_dir) / 'test.txt'
            assert not await path.exists()
            async with open_in_process(monitor_for_interrupt, path) as proc:
                print('killing')
                proc.terminate()
                print('killed')
            print('finished')
            assert await path.exists()
