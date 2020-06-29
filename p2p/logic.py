from __future__ import annotations
from abc import abstractmethod
import asyncio
import contextlib
from typing import (
    cast,
    AsyncIterator,
    Generic,
    Iterable,
    List,
    Sequence,
    Tuple,
    Type,
)

from trio import MultiError

from p2p.abc import (
    BehaviorAPI,
    ConnectionAPI,
    HandlerFn,
    LogicAPI,
    QualifierFn,
    TCommand,
)
from p2p.behaviors import Behavior
from p2p.qualifiers import HasCommand


class BaseLogic(LogicAPI):
    qualifier: QualifierFn = None

    def as_behavior(self, qualifier: QualifierFn = None) -> BehaviorAPI:
        if qualifier is None:
            # mypy bug: https://github.com/python/mypy/issues/708
            if self.qualifier is None:  # type: ignore
                raise TypeError("No qualifier provided or found on class")
            # mypy bug: https://github.com/python/mypy/issues/708
            qualifier = self.qualifier  # type: ignore
        return Behavior(qualifier, self)


class CommandHandler(BaseLogic, Generic[TCommand]):
    """
    Base class to reduce boilerplate for Behaviors that want to register a
    handler against a single command.
    """
    command_type: Type[TCommand]

    # This property is
    connection: ConnectionAPI

    @property
    def qualifier(self) -> QualifierFn:  # type: ignore
        return HasCommand(self.command_type)

    @contextlib.asynccontextmanager
    async def apply(self, connection: ConnectionAPI) -> AsyncIterator[asyncio.Future[None]]:
        """
        See LogicAPI.apply()

        The future returned here will never be done as a CommandHandler doesn't run a background
        task.
        """
        self.connection = connection

        with connection.add_command_handler(self.command_type, cast(HandlerFn, self.handle)):
            yield asyncio.Future()

    @abstractmethod
    async def handle(self, connection: ConnectionAPI, command: TCommand) -> None:
        ...


async def wait_first(futures: Sequence[asyncio.Future[None]]) -> None:
    """
    Wait for the first of the given futures to complete, then cancels all others.

    If the completed future raised an exception, re-raise it.

    If the task running us is cancelled, all futures will be cancelled.
    """
    for future in futures:
        if not isinstance(future, asyncio.Future):
            raise ValueError("{future} is not an asyncio.Future")

    try:
        done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        await cancel_futures(futures)
        raise
    else:
        await cancel_futures(pending)
        if len(done) != 1:
            raise Exception(
                "Invariant: asyncio.wait() returned more than one future even "
                "though we used return_when=asyncio.FIRST_COMPLETED: %s", done)
        done_future = list(done)[0]
        if done_future.exception():
            raise done_future.exception()


class Application(BaseLogic):
    """
    Wrapper arround a collection of behaviors.  Primarily used to aggregate
    multiple smaller units of functionality.

    When applied an `Application` registers itself with the `ConnectionAPI`
    under the defined `name`.
    """
    name: str
    connection: ConnectionAPI
    _behaviors: Tuple[BehaviorAPI, ...] = ()

    def add_child_behavior(self, behavior: BehaviorAPI) -> None:
        self._behaviors += (behavior,)

    @contextlib.asynccontextmanager
    async def apply(self, connection: ConnectionAPI) -> AsyncIterator[asyncio.Future[None]]:
        """
        See LogicAPI.apply()

        The future returned here will be done when the first of the futures obtained from applying
        all behaviors of this application is done.
        """
        self.connection = connection

        async with contextlib.AsyncExitStack() as stack:
            futures: List[asyncio.Future[None]] = []
            # First apply all the child behaviors
            for behavior in self._behaviors:
                if behavior.should_apply_to(connection):
                    fut = await stack.enter_async_context(behavior.apply(connection))
                    futures.append(fut)

            # If none of our behaviors were applied, use a never-ending Future so that callsites
            # can wait on it like when behaviors are applied.
            if not futures:
                futures.append(asyncio.Future())

            # Now register ourselves with the connection.
            with connection.add_logic(self.name, self):
                yield asyncio.create_task(wait_first(futures))


async def cancel_futures(futures: Iterable[asyncio.Future[None]]) -> None:
    """
    Cancel and await for the given futures, ignoring any asyncio.CancelledErrors.
    """
    for fut in futures:
        fut.cancel()

    errors: List[BaseException] = []
    # Wait for all futures in parallel so if any of them catches CancelledError and performs a
    # slow cleanup the othders don't have to wait for it. The timeout is long as our component
    # tasks can do a lot of stuff during their cleanup.
    done, pending = await asyncio.wait(futures, timeout=5)
    if pending:
        errors.append(
            asyncio.TimeoutError("Tasks never returned after being cancelled: %s", pending))
    for task in done:
        with contextlib.suppress(asyncio.CancelledError):
            if task.exception():
                errors.append(task.exception())
    if errors:
        raise MultiError(errors)
