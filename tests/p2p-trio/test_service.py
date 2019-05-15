import pytest

import trio

from p2p.trio_service import (
    Service,
)


async def do_service_lifecycle_check(service,
                                     service_run_fn,
                                     trigger_exit_condition_fn):
    async with trio.open_nursery() as nursery:
        assert service.has_started is False
        assert service.is_running is False
        assert service.is_cancelled is False
        assert service.is_stopped is False

        nursery.start_soon(service_run_fn)

        with trio.fail_after(0.1):
            await service.wait_started()

        assert service.has_started is True
        assert service.is_running is True
        assert service.is_cancelled is False
        assert service.is_stopped is False

        # trigger the service to exit
        trigger_exit_condition_fn()

        with trio.fail_after(0.1):
            await service.wait_cancelled()

        assert service.has_started is True
        assert service.is_running is True
        assert service.is_cancelled is True
        assert service.is_stopped is False

        with trio.fail_after(0.1):
            await service.wait_stopped()

        assert service.has_started is True
        assert service.is_running is False
        assert service.is_cancelled is True
        assert service.is_stopped is True


@pytest.mark.trio
async def test_trio_service_lifecycle_run_and_clean_exit():
    trigger_exit = trio.Event()

    @Service.from_function
    async def ServiceTest(service):
        await trigger_exit.wait()

    service = ServiceTest()

    await do_service_lifecycle_check(service, service.run, trigger_exit.set)


@pytest.mark.trio
async def test_trio_service_lifecycle_run_and_internal_cancellation():
    trigger_cancel = trio.Event()

    @Service.from_function
    async def ServiceTest(service):
        await trigger_cancel.wait()
        service.cancel()

    service = ServiceTest()

    await do_service_lifecycle_check(service, service.run, trigger_cancel.set)


@pytest.mark.trio
async def test_trio_service_lifecycle_run_and_external_cancellation():

    @Service.from_function
    async def ServiceTest(service):
        while True:
            await trio.sleep(1)

    service = ServiceTest()

    await do_service_lifecycle_check(service, service.run, service.cancel)


@pytest.mark.trio
async def test_trio_service_lifecycle_run_and_exception():
    trigger_error = trio.Event()

    @Service.from_function
    async def ServiceTest(service):
        await trigger_error.wait()
        raise RuntimeError("Service throwing error")

    service = ServiceTest()

    async def do_service_run():
        with pytest.raises(RuntimeError, match="Service throwing error"):
            await service.run()

    await do_service_lifecycle_check(service, do_service_run, trigger_error.set)
