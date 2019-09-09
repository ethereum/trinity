import pickle
import signal
import tempfile

import pytest

import trio

from p2p.trio_run_in_process import run_in_process, open_in_process, ProcessException


@pytest.fixture
def touch_path():
    with tempfile.TemporaryDirectory() as base_dir:
        yield trio.Path(base_dir) / 'touch.txt'


@pytest.mark.trio
async def test_run_in_process_touch_file(touch_path):
    async def touch_file(path: trio.Path):
        await path.touch()

    with trio.fail_after(2):
        assert not await touch_path.exists()
        await run_in_process(touch_file, touch_path)
        assert await touch_path.exists()


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
async def test_open_in_proc_termination_while_running():
    async def do_sleep_forever():
        import trio
        await trio.sleep_forever()

    with trio.fail_after(2):
        async with open_in_process(do_sleep_forever) as proc:
            proc.terminate()
    assert proc.returncode == 15


@pytest.mark.trio
async def test_open_in_proc_kill_while_running():
    async def do_sleep_forever():
        import trio
        await trio.sleep_forever()

    with trio.fail_after(2):
        async with open_in_process(do_sleep_forever) as proc:
            proc.kill()
    assert proc.returncode == -9
    assert isinstance(proc.error, ProcessException)


@pytest.mark.trio
async def test_open_proc_interrupt_while_running():
    async def monitor_for_interrupt():
        import trio
        await trio.sleep_forever()

    with trio.fail_after(2):
        async with open_in_process(monitor_for_interrupt) as proc:
            proc.send_signal(signal.SIGINT)
        assert proc.returncode == 2


@pytest.mark.trio
async def test_open_proc_invalid_function_call():
    async def takes_no_args():
        pass

    with trio.fail_after(2):
        async with open_in_process(takes_no_args, 1, 2, 3) as proc:
            pass
        assert proc.returncode == 1
        assert isinstance(proc.error, TypeError)


@pytest.mark.trio
async def test_open_proc_unpickleable_params(touch_path):
    async def takes_open_file(f):
        pass

    with trio.fail_after(2):
        with pytest.raises(pickle.PickleError):
            with open(touch_path, 'w') as touch_file:
                async with open_in_process(takes_open_file, touch_file):
                    # this code block shouldn't get executed
                    assert False


@pytest.mark.trio
async def test_open_proc_outer_KeyboardInterrupt():
    async def sleep_forever():
        import trio
        await trio.sleep_forever()

    with trio.fail_after(2):
        with pytest.raises(KeyboardInterrupt):
            async with open_in_process(sleep_forever) as proc:
                raise KeyboardInterrupt
        assert proc.returncode == 2
