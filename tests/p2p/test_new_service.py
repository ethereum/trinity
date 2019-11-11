import asyncio

import pytest

from p2p.service import (
    DaemonTaskExit,
    Service,
    as_service,
    AsyncioManager,
    background_asyncio_service,
    external_api,
    ServiceCancelled,
)


class WaitCancelledService(Service):
    async def run(self) -> None:
        await self.manager.wait_cancelled()


async def do_service_lifecycle_check(manager,
                                     manager_run_fn,
                                     trigger_exit_condition_fn,
                                     should_be_cancelled):
    assert manager.is_started is False
    assert manager.is_running is False
    assert manager.is_cancelled is False
    assert manager.is_stopped is False

    asyncio.ensure_future(manager_run_fn())

    await asyncio.wait_for(manager.wait_started(), timeout=0.1)

    assert manager.is_started is True
    assert manager.is_running is True
    assert manager.is_cancelled is False
    assert manager.is_stopped is False

    # trigger the service to exit
    trigger_exit_condition_fn()

    if should_be_cancelled:
        await asyncio.wait_for(manager.wait_cancelled(), timeout=0.01)

        assert manager.is_started is True
        # We cannot determine whether the service should be running at this
        # stage because a service is considered running until it has
        # stopped.  Since it may be cancelled but still not stopped we
        # can't know.
        assert manager.is_cancelled is True
        # We cannot determine whether a service should be stopped at this
        # stage as it could have exited cleanly and is now stopped or it
        # might be doing some cleanup after which it will register as being
        # stopped.

    await asyncio.wait_for(manager.wait_stopped(), timeout=0.1)

    assert manager.is_started is True
    assert manager.is_running is False
    assert manager.is_cancelled is should_be_cancelled
    assert manager.is_stopped is True


def test_service_manager_initial_state():
    service = WaitCancelledService()
    manager = AsyncioManager(service)

    assert manager.is_started is False
    assert manager.is_running is False
    assert manager.is_cancelled is False
    assert manager.is_stopped is False


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_clean_exit():
    trigger_exit = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        await trigger_exit.wait()

    service = ServiceTest()
    manager = AsyncioManager(service)

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=manager.run,
        trigger_exit_condition_fn=trigger_exit.set,
        should_be_cancelled=False,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_external_cancellation():

    @as_service
    async def ServiceTest(manager):
        while True:
            await asyncio.sleep(0)

    service = ServiceTest()
    manager = AsyncioManager(service)

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=manager.run,
        trigger_exit_condition_fn=manager.cancel,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_exception():
    trigger_error = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        await trigger_error.wait()
        raise RuntimeError("Service throwing error")

    service = ServiceTest()
    manager = AsyncioManager(service)

    async def do_service_run():
        with pytest.raises(RuntimeError, match="Service throwing error"):
            await manager.run()

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=do_service_run,
        trigger_exit_condition_fn=trigger_error.set,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_task_exception():
    trigger_error = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        async def task_fn():
            await trigger_error.wait()
            raise RuntimeError("Service throwing error")
        manager.run_task(task_fn)

    service = ServiceTest()
    manager = AsyncioManager(service)

    async def do_service_run():
        with pytest.raises(RuntimeError, match="Service throwing error"):
            await manager.run()

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=do_service_run,
        trigger_exit_condition_fn=trigger_error.set,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_daemon_task_exit():
    trigger_error = asyncio.Event()

    @as_service
    async def ServiceTest(manager):
        async def daemon_task_fn():
            await trigger_error.wait()
        manager.run_daemon_task(daemon_task_fn)

    service = ServiceTest()
    manager = AsyncioManager(service)

    async def do_service_run():
        with pytest.raises(DaemonTaskExit, match="Daemon task"):
            await manager.run()

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=do_service_run,
        trigger_exit_condition_fn=trigger_error.set,
        should_be_cancelled=True,
    )


@pytest.mark.asyncio
async def test_asyncio_service_background_service_context_manager():
    service = WaitCancelledService()

    async with background_asyncio_service(service) as manager:
        # ensure the manager property is set.
        assert hasattr(service, 'manager')
        assert service.manager is manager

        assert manager.is_started is True
        assert manager.is_running is True
        assert manager.is_cancelled is False
        assert manager.is_stopped is False

    assert manager.is_started is True
    assert manager.is_running is False
    assert manager.is_cancelled is True
    assert manager.is_stopped is True


@pytest.mark.asyncio
async def test_asyncio_service_manager_stop():
    service = WaitCancelledService()

    async with background_asyncio_service(service) as manager:
        assert manager.is_started is True
        assert manager.is_running is True
        assert manager.is_cancelled is False
        assert manager.is_stopped is False

        await manager.stop()

        assert manager.is_started is True
        assert manager.is_running is False
        assert manager.is_cancelled is True
        assert manager.is_stopped is True


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            task_event.set()
        manager.run_task(task_fn)
        await manager.wait_cancelled()

    async with background_asyncio_service(RunTaskService()):
        await asyncio.wait_for(task_event.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task_waits_for_task_completion():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            await asyncio.sleep(0.01)
            task_event.set()
        manager.run_task(task_fn)
        # the task is set to run in the background but then  the service exits.
        # We want to be sure that the task is allowed to continue till
        # completion unless explicitely cancelled.

    async with background_asyncio_service(RunTaskService()):
        await asyncio.wait_for(task_event.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task_can_still_cancel_after_run_finishes():
    task_event = asyncio.Event()
    service_finished = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            # this will never complete
            await task_event.wait()

        manager.run_task(task_fn)
        # the task is set to run in the background but then  the service exits.
        # We want to be sure that the task is allowed to continue till
        # completion unless explicitely cancelled.
        service_finished.set()

    async with background_asyncio_service(RunTaskService()) as manager:
        await asyncio.wait_for(service_finished.wait(), timeout=0.01)

        # show that the service hangs waiting for the task to complete.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(manager.wait_stopped(), timeout=0.01)

        # trigger cancellation and see that the service actually stops
        manager.cancel()
        await asyncio.wait_for(manager.wait_stopped(), timeout=0.01)


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_task_reraises_exceptions():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def task_fn():
            await task_event.wait()
            raise Exception("task exception in run_task")
        manager.run_task(task_fn)
        await asyncio.wait_for(asyncio.sleep(100), timeout=1)

    with pytest.raises(BaseException, match="task exception in run_task"):
        async with background_asyncio_service(RunTaskService()) as manager:
            task_event.set()
            await manager.wait_stopped()
            pass


@pytest.mark.asyncio
async def test_asyncio_service_manager_run_daemon_task_cancels_if_exits():
    task_event = asyncio.Event()

    @as_service
    async def RunTaskService(manager):
        async def daemon_task_fn():
            await task_event.wait()

        manager.run_daemon_task(daemon_task_fn, name='daemon_task_fn')
        await asyncio.wait_for(asyncio.sleep(100), timeout=1)

    with pytest.raises(DaemonTaskExit, match="Daemon task daemon_task_fn exited"):
        async with background_asyncio_service(RunTaskService()) as manager:
            task_event.set()
            await manager.wait_stopped()


@pytest.mark.asyncio
async def test_asyncio_service_manager_propogates_and_records_exceptions():
    @as_service
    async def ThrowErrorService(manager):
        raise RuntimeError('this is the error')

    service = ThrowErrorService()
    manager = AsyncioManager(service)

    assert manager.did_error is False

    with pytest.raises(RuntimeError, match='this is the error'):
        await manager.run()

    assert manager.did_error is True


@pytest.mark.asyncio
async def test_asyncio_service_lifecycle_run_and_clean_exit_with_child_service():
    trigger_exit = asyncio.Event()

    @as_service
    async def ChildServiceTest(manager):
        await trigger_exit.wait()

    @as_service
    async def ServiceTest(manager):
        child_manager = manager.run_child_service(ChildServiceTest())
        await child_manager.wait_started()

    service = ServiceTest()
    manager = AsyncioManager(service)

    await do_service_lifecycle_check(
        manager=manager,
        manager_run_fn=manager.run,
        trigger_exit_condition_fn=trigger_exit.set,
        should_be_cancelled=False,
    )


@pytest.mark.asyncio
async def test_asyncio_service_with_async_generator():
    is_within_agen = asyncio.Event()

    async def do_agen():
        while True:
            yield

    @as_service
    async def ServiceTest(manager):
        async for _ in do_agen():  # noqa: F841
            await asyncio.sleep(0)
            is_within_agen.set()

    async with background_asyncio_service(ServiceTest()) as manager:
        await is_within_agen.wait()
        manager.cancel()


@pytest.mark.asyncio
async def test_asyncio_service_external_api_raises_ServiceCancelled():
    class ServiceTest(Service):
        @external_api
        async def get_7(self):
            return 7

    service = ServiceTest()
    async with background_asyncio_service(service) as manager:
        manager.cancel()
        with pytest.raises(ServiceCancelled):
            await service.get_7()
