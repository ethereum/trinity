from abc import abstractmethod
import asyncio
import concurrent
import contextlib
import functools
import time
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    List,
    Optional,
    TypeVar,
)
from weakref import WeakSet

from async_service import Service, ServiceAPI

from cancel_token import CancelToken, OperationCancelled
from eth_utils import (
    ExtendedDebugLogger,
    ValidationError,
)

from p2p.abc import ServiceEventsAPI, AsyncioServiceAPI
from p2p.cancellable import CancellableMixin
from p2p._utils import get_logger


class ServiceEvents(ServiceEventsAPI):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.cleaned_up = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.finished = asyncio.Event()


class BaseService(CancellableMixin, AsyncioServiceAPI):
    # Use a WeakSet so that we don't have to bother updating it when tasks finish.
    _child_services: 'WeakSet[AsyncioServiceAPI]'
    _tasks: 'WeakSet[asyncio.Future[Any]]'
    _finished_callbacks: List[Callable[[AsyncioServiceAPI], None]]
    # Number of seconds cancel() will wait for run() to finish.
    _wait_until_finished_timeout = 5

    # the custom event loop to run in, or None if the default loop should be used
    _loop: asyncio.AbstractEventLoop = None

    _logger: ExtendedDebugLogger = None

    _start_time: float = None

    def __init__(self,
                 token: CancelToken = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.events = ServiceEvents()
        self._run_lock = asyncio.Lock()
        self._child_services = WeakSet()
        self._tasks = WeakSet()
        self._finished_callbacks = []

        self._loop = loop

        base_token = CancelToken(type(self).__name__, loop=loop)

        if token is None:
            self.cancel_token = base_token
        else:
            self.cancel_token = base_token.chain(token)

    @property
    def logger(self) -> ExtendedDebugLogger:
        if self._logger is None:
            self._logger = get_logger(
                self.__module__ + '.' + self.__class__.__name__
            )
        return self._logger

    @property
    def uptime(self) -> float:
        if self._start_time is None:
            return 0.0
        else:
            return time.monotonic() - self._start_time

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            return asyncio.get_event_loop()
        else:
            return self._loop

    async def run(
            self,
            finished_callback: Optional[Callable[[AsyncioServiceAPI], None]] = None) -> None:
        """Await for the service's _run() coroutine.

        Once _run() returns, triggers the cancel token, call cleanup() and
        finished_callback (if one was passed).
        """
        if self.is_running:
            raise ValidationError("Cannot start the service while it's already running: %s", self)
        elif self.is_cancelled:
            raise ValidationError(
                "Cannot restart a service that has already been cancelled: %s -> %s",
                self,
                self.cancel_token.triggered_token,
            )

        if finished_callback:
            self._finished_callbacks.append(finished_callback)

        try:
            async with self._run_lock:
                self.events.started.set()
                self._start_time = time.monotonic()
                await self._run()
        except OperationCancelled as e:
            self.logger.debug("%s finished: %s", self, e)
        except asyncio.CancelledError:
            # If a cancellation occurs we just want to re-raise it.  No need to log anything.
            raise
        except Exception:
            self.logger.exception("Unexpected error in %r, exiting", self)
        else:
            if self.is_cancelled:
                self.logger.debug("%s cancelled, cleaning up...", self)
            else:
                self.logger.debug("%s had nothing left to do, ceasing operation...", self)
        finally:
            # Trigger our cancel token to ensure all pending asyncio tasks and background
            # coroutines started by this service exit cleanly.
            self.events.cancelled.set()
            self.cancel_token.trigger()

            await self.cleanup()

            for callback in self._finished_callbacks:
                callback(self)

            self.events.finished.set()
            self.logger.debug("%s halted cleanly", self)

    def add_finished_callback(self, finished_callback: Callable[[AsyncioServiceAPI], None]) -> None:
        self._finished_callbacks.append(finished_callback)

    def run_task(self, awaitable: Awaitable[Any]) -> None:
        """Run the given awaitable in the background.

        The awaitable should return whenever this service's cancel token is triggered.

        If it raises OperationCancelled, that is caught and ignored.
        """
        @functools.wraps(awaitable)  # type: ignore
        async def _run_task_wrapper() -> None:
            self.logger.debug2("Running task %s", awaitable)
            try:
                await awaitable
            except OperationCancelled:
                pass
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.warning("Task %s finished unexpectedly: %r", awaitable, e)
                self.logger.debug("Task failure traceback", exc_info=True)
            else:
                self.logger.debug2("Task %s finished with no errors", awaitable)
        self._tasks.add(asyncio.ensure_future(_run_task_wrapper()))

    def run_daemon_task(self, awaitable: Awaitable[Any]) -> None:
        """Run the given awaitable in the background.

        Like :meth:`run_task` but if the task ends without cancelling, then this
        this service will terminate as well.
        """
        @functools.wraps(awaitable)  # type: ignore
        async def _run_daemon_task_wrapper() -> None:
            try:
                await awaitable
            finally:
                if not self.is_cancelled:
                    self.logger.debug(
                        "%s finished while %s is still running, terminating as well",
                        awaitable,
                        self,
                    )
                    self.cancel_nowait()
        self.run_task(_run_daemon_task_wrapper())

    def run_child_service(self, child_service: AsyncioServiceAPI) -> None:
        """
        Run a child service and keep a reference to it to be considered during the cleanup.
        """
        if child_service.is_running:
            raise ValidationError(
                f"Can't start service {child_service!r}, child of {self!r}: it's already running"
            )
        elif child_service.is_cancelled:
            raise ValidationError(
                f"Can't restart {child_service!r}, child of {self!r}: it's already completed"
            )

        self._child_services.add(child_service)
        self.run_task(child_service.run())

    def run_daemon(self, service: AsyncioServiceAPI) -> None:
        """
        Run a service and keep a reference to it to be considered during the cleanup.

        If the service finishes while we're still running, we'll terminate as well.
        """
        if service.is_running:
            raise ValidationError(
                f"Can't start daemon {service!r}, child of {self!r}: it's already running"
            )
        elif service.is_cancelled:
            raise ValidationError(
                f"Can't restart daemon {service!r}, child of {self!r}: it's already completed"
            )

        self._child_services.add(service)

        @functools.wraps(service.run)
        async def _run_daemon_wrapper() -> None:
            try:
                await service.run()
            except OperationCancelled:
                pass
            except Exception as e:
                self.logger.warning("Daemon Service %s finished unexpectedly: %s", service, e)
                self.logger.debug("Daemon Service failure traceback", exc_info=True)
            finally:
                if not self.is_cancelled:
                    self.logger.debug(
                        "%s finished while %s is still running, terminating as well",
                        service,
                        self,
                    )
                    self.cancel_nowait()

        self.run_task(_run_daemon_wrapper())

    def call_later(self, delay: float, callback: 'Callable[..., None]', *args: Any) -> None:

        @functools.wraps(callback)
        async def _call_later_wrapped() -> None:
            await self.sleep(delay)
            callback(*args)

        self.run_task(_call_later_wrapped())

    async def _run_in_executor(self,
                               executor: concurrent.futures.Executor,
                               callback: Callable[..., Any], *args: Any) -> Any:

        loop = self.get_event_loop()
        try:
            return await self.wait(loop.run_in_executor(executor, callback, *args))
        except concurrent.futures.process.BrokenProcessPool:
            self.logger.exception("Fatal error. Process pool died. Cancelling operations.")
            await self.cancel()

    async def cleanup(self) -> None:
        """
        Run the ``_cleanup()`` coroutine and set the ``cleaned_up`` event after the service as
        well as all child services finished their cleanup.

        The ``_cleanup()`` coroutine is invoked before the child services may have finished
        their cleanup.
        """
        if self._child_services:
            self.logger.debug("%s waiting for child services: %s", self, list(self._child_services))
            wait_for_clean_up_tasks = (
                child_service.events.cleaned_up.wait()
                for child_service in self._child_services
            )
            await asyncio.gather(*wait_for_clean_up_tasks)
            self.logger.debug("%s: All child services finished", self)
        if self._tasks:
            self._log_tasks("Waiting for tasks")
            await asyncio.gather(*self._tasks)
            self.logger.debug("All tasks finished")

        await self._cleanup()
        self.events.cleaned_up.set()

    def _log_tasks(self, message: str) -> None:
        MAX_DISPLAY_TASKS = 50
        task_list = list(self._tasks)
        if len(self._tasks) > MAX_DISPLAY_TASKS:
            task_display = ''.join(map(str, [
                task_list[:MAX_DISPLAY_TASKS // 2],
                '...',
                task_list[-1 * MAX_DISPLAY_TASKS // 2:],
            ]))
        else:
            task_display = str(task_list)
        self.logger.debug("%s: %s (%d): %s", self, message, len(self._tasks), task_display)

    def cancel_nowait(self) -> None:
        if self.is_cancelled:
            self.logger.warning("Tried to cancel %s, but it was already cancelled", self)
            self.logger.debug("Second cancellation of %s: stack trace", self, stack_info=True)
            return
        elif not self.is_running:
            raise ValidationError("Cannot cancel a service that has not been started")

        self.logger.debug("Cancelling service: %s", self)
        self.events.cancelled.set()
        self.cancel_token.trigger()

    async def cancel(self) -> None:
        """Trigger the CancelToken and wait for the cleaned_up event to be set."""
        self.cancel_nowait()

        try:
            await asyncio.wait_for(
                self.events.cleaned_up.wait(), timeout=self._wait_until_finished_timeout)
        except asyncio.TimeoutError:
            self.logger.info(
                "Timed out waiting for %s to finish its cleanup, forcibly cancelling pending "
                "tasks and exiting anyway", self)
            if self._tasks:
                self._log_tasks("Pending tasks")
            if self._child_services:
                self.logger.debug("Pending child services: %s", list(self._child_services))
            await self._forcibly_cancel_all_tasks()
            # Sleep a bit because the Future.cancel() method just schedules the callbacks, so we
            # need to give the event loop a chance to actually call them.
            await asyncio.sleep(0.01)
        else:
            self.logger.debug("%s finished cleanly", self)

    async def _forcibly_cancel_all_tasks(self) -> None:
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_token.triggered

    @property
    def is_operational(self) -> bool:
        return self.events.started.is_set() and not self.cancel_token.triggered

    @property
    def is_running(self) -> bool:
        return self._run_lock.locked()

    async def cancellation(self) -> None:
        """
        Pause until this service is cancelled
        """
        await self.wait(self.events.cancelled.wait())

    async def threadsafe_cancel(self) -> None:
        """
        Cancel service in another thread. Block until service is cleaned up.

        :param poll_period: how many seconds to wait in between each check for service cleanup
        """
        asyncio.run_coroutine_threadsafe(self.cancel(), loop=self.get_event_loop())
        await asyncio.wait_for(
            self.events.cleaned_up.wait(),
            timeout=self._wait_until_finished_timeout,
        )

    async def sleep(self, delay: float) -> None:
        """Coroutine that completes after a given time (in seconds)."""
        await self.wait(asyncio.sleep(delay))

    @abstractmethod
    async def _run(self) -> None:
        """Run the service's loop.

        Should return or raise OperationCancelled when the CancelToken is triggered.
        """
        pass

    async def _cleanup(self) -> None:
        """Clean up any resources held by this service.

        Called after the service's _run() method returns.
        """
        pass

    def as_new_service(self) -> ServiceAPI:
        return WrappedLegacyService(self)


def service_timeout(timeout: int) -> Callable[..., Any]:
    """
    Decorator to time out a method call.

    :param timeout: seconds to wait before raising a timeout exception

    :raise asyncio.TimeoutError: if the call is not complete before timeout seconds
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapped(service: AsyncioServiceAPI, *args: Any, **kwargs: Any) -> Any:
            return await service.wait(
                func(service, *args, **kwargs),
                timeout=timeout,
            )
        return wrapped
    return decorator


class EmptyService(BaseService):
    async def _run(self) -> None:
        pass

    async def _cleanup(self) -> None:
        pass


TService = TypeVar('TService', bound=AsyncioServiceAPI)


@contextlib.asynccontextmanager
async def run_service(service: TService) -> AsyncIterator[TService]:
    task = asyncio.ensure_future(service.run())
    await service.events.started.wait()
    try:
        yield service
    finally:
        try:
            if not service.is_cancelled:
                await service.cancel()
        finally:
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass


class WrappedLegacyService(Service):
    def __init__(self, service: AsyncioServiceAPI) -> None:
        self._service = service

    async def run(self) -> None:
        async with run_service(self._service):
            try:
                await self._service.cancellation()
            except OperationCancelled:
                # We must not let an OperationCancelled bubble because asyncio-service's
                # run_task() & co don't handle those like BaseService methods.
                pass
