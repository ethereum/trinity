from __future__ import annotations
from abc import abstractmethod
import asyncio
import contextlib
from typing import (
    cast,
    Any,
    AsyncIterator,
    Generic,
    List,
    Tuple,
    Type,
)

from p2p.abc import (
    BehaviorAPI,
    ConnectionAPI,
    HandlerFn,
    LogicAPI,
    QualifierFn,
    TCommand,
)
from p2p.asyncio_utils import create_task, wait_first
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

    def post_apply(self) -> None:
        pass


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
    async def apply(self, connection: ConnectionAPI) -> AsyncIterator[asyncio.Task[Any]]:
        """
        See LogicAPI.apply()

        The future returned here will never be done as a CommandHandler doesn't run a background
        task.
        """
        self.connection = connection

        with connection.add_command_handler(self.command_type, cast(HandlerFn, self.handle)):
            yield create_task(
                _never_ending_coro(),
                f'{connection.remote}/CommandHandler/{self.__class__.__name__}/apply'
            )

    @abstractmethod
    async def handle(self, connection: ConnectionAPI, command: TCommand) -> None:
        ...


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
    async def apply(self, connection: ConnectionAPI) -> AsyncIterator[asyncio.Task[Any]]:
        """
        See LogicAPI.apply()

        The future returned here will be done when the first of the futures obtained from applying
        all behaviors of this application is done.
        """
        self.connection = connection

        async with contextlib.AsyncExitStack() as stack:
            futures: List[asyncio.Task[Any]] = []
            # First apply all the child behaviors
            for behavior in self._behaviors:
                if behavior.should_apply_to(connection):
                    fut = await stack.enter_async_context(behavior.apply(connection))
                    futures.append(fut)

            # If none of our behaviors were applied, use a never-ending task so that callsites
            # can wait on it like when behaviors are applied.
            if not futures:
                futures.append(
                    create_task(_never_ending_coro(),
                                f'{connection.remote}/Application/{self.name}/no-behaviors-fut'))

            # Now register ourselves with the connection.
            with connection.add_logic(self.name, self):
                name = f'{connection.remote}/Application/{self.name}/apply'
                yield create_task(
                    wait_first(futures, max_wait_after_cancellation=2),
                    name=name,
                )


async def _never_ending_coro() -> None:
    await asyncio.Future()
