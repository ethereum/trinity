from abc import abstractmethod
from typing import (
    AsyncIterator,
    Tuple,
    Type,
)

from async_generator import asynccontextmanager
from async_exit_stack import AsyncExitStack

from p2p.abc import (
    BehaviorAPI,
    CommandAPI,
    ConnectionAPI,
    LogicAPI,
    QualifierFn,
)
from p2p.behaviors import Behavior
from p2p.qualifiers import HasCommand
from p2p.typing import Payload


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


class CommandHandler(BaseLogic):
    """
    Base class to reduce boilerplate for Behaviors that want to register a
    handler against a single command.
    """
    command_type: Type[CommandAPI]

    # This property is
    connection: ConnectionAPI

    @property
    def qualifier(self) -> QualifierFn:  # type: ignore
        return HasCommand(self.command_type)

    @asynccontextmanager
    async def __call__(self, connection: ConnectionAPI) -> AsyncIterator[None]:
        self.connection = connection

        with connection.add_command_handler(self.command_type, self.handle):
            yield

    @abstractmethod
    async def handle(self, connection: ConnectionAPI, msg: Payload) -> None:
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

    @asynccontextmanager
    async def __call__(self, connection: ConnectionAPI) -> AsyncIterator[None]:
        self.connection = connection

        async with AsyncExitStack() as stack:
            # First apply all the child behaviors
            for behavior in self._behaviors:
                if behavior.should_apply_to(connection):
                    await stack.enter_async_context(behavior.apply(connection))

            # Now register ourselves with the connection.
            with connection.add_logic(self.name, self):
                yield
