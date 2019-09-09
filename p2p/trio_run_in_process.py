import argparse
import enum
import io
import logging
import os
import signal
import struct
# import subprocess
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    BinaryIO,
    Callable,
    Optional,
    Sequence,
    TypeVar,
)

from async_generator import asynccontextmanager
import cloudpickle
from eth_utils.toolz import sliding_window
import trio
import trio_typing


TReturn = TypeVar('TReturn')


logger = logging.getLogger('trio-run-in-process')


def get_subprocess_command(child_r, child_w, parent_pid):
    return (
        sys.executable,
        '-m', 'p2p.trio_run_in_process',
        '--parent-pid', str(parent_pid),
        '--fd-read', str(child_r),
        '--fd-write', str(child_w),
    )


class State(enum.Enum):
    """
    Child process lifecycle
    """
    INITIALIZING = b'\x00'
    INITIALIZED = b'\x01'
    WAIT_EXEC_DATA = b'\x02'
    BOOTING = b'\x03'
    STARTED = b'\x04'
    EXECUTING = b'\x05'
    STOPPING = b'\x06'
    FINISHED = b'\x07'

    def as_int(self) -> int:
        return self.value[0]

    def is_next(self, other: 'State') -> bool:
        return other.as_int() == self.as_int() + 1

    def is_on_or_after(self, other: 'State') -> bool:
        return self.value[0] >= other.value[0]

    def is_before(self, other: 'State') -> bool:
        return self.value[0] < other.value[0]


class empty:
    pass


class ProcessException(Exception):
    pass


class Process(Awaitable[TReturn]):
    returncode: Optional[int] = None

    _pid: Optional[int] = None
    _returncode: Optional[int] = None
    _return_value: Optional[TReturn] = empty
    _error: Optional[BaseException] = None
    _state: State = State.INITIALIZING

    def __init__(self, async_fn: Callable[..., TReturn], args: Sequence[TReturn]) -> None:
        self._async_fn = async_fn
        self._args = args

        self._has_pid = trio.Event()
        self._has_returncode = trio.Event()
        self._has_return_value = trio.Event()
        self._has_error = trio.Event()
        self._state_changed = trio.Event()

    def __await__(self) -> TReturn:
        return self.run().__await__()

    #
    # State
    #
    @property
    def state(self) -> State:
        return self._state

    @state.setter
    def state(self, value: State) -> State:
        if not self._state.is_next(value):
            raise Exception(f"Invalid state transition: {self.state} -> {value}")
        self._state = value
        self._state_changed.set()
        self._state_changed = trio.Event()

    async def wait_for_state(self, state: State) -> None:
        """
        Block until the process as reached the
        """
        if self.state.is_on_or_after(state):
            return

        for _ in range(len(State)):
            await self._state_changed.wait()
            if self.state.is_on_or_after(state):
                break
        else:
            raise Exception(
                f"This code path should not be reachable since there are a "
                f"finite number of state transitions.  Current state is "
                f"{self.state}"
            )

    #
    # PID
    #
    @property
    def pid(self) -> int:
        if self._pid is None:
            raise AttributeError("No PID set for process")
        return self._pid

    @pid.setter
    def pid(self, value: int) -> None:
        self._pid = value
        self._has_pid.set()

    async def wait_pid(self) -> int:
        await self._has_pid.wait()
        return self.pid

    #
    # Return Value
    #
    @property
    def return_value(self) -> int:
        if self._return_value is empty:
            raise AttributeError("No return_value set")
        return self._return_value

    @return_value.setter
    def return_value(self, value: int) -> None:
        self._return_value = value
        self._has_return_value.set()

    async def wait_return_value(self) -> int:
        await self._has_return_value.wait()
        return self.return_value

    #
    # Return Code
    #
    @property
    def returncode(self) -> int:
        if self._returncode is None:
            raise AttributeError("No returncode set")
        return self._returncode

    @returncode.setter
    def returncode(self, value: int) -> None:
        self._returncode = value
        self._has_returncode.set()

    async def wait_returncode(self) -> int:
        await self._has_returncode.wait()
        return self.returncode

    #
    # Error
    #
    @property
    def error(self) -> int:
        if self._error is None:
            raise AttributeError("No error set")
        return self._error

    @error.setter
    def error(self, value: int) -> None:
        self._error = value
        self._has_error.set()

    async def wait_error(self) -> int:
        await self._has_error.wait()
        return self.error

    #
    # Result
    #
    @property
    def result(self) -> TReturn:
        if self._error is None and self._return_value is empty:
            raise AttributeError("Process not done")
        elif self._error is not None:
            raise self._error
        elif self._return_value is not empty:
            return self._return_value
        else:
            raise Exception("Code path should be unreachable")

    async def wait_result(self) -> TReturn:
        """
        Block until the process has exited, either returning the return value
        if execution was successful, or raising an exception if it failed
        """
        await self.wait_returncode()

        if self.returncode == 0:
            return await self.wait_return_value()
        else:
            raise await self.wait_error()

    #
    # Lifecycle management APIs
    #
    async def wait(self) -> None:
        """
        Block until the process has exited.
        """
        await self.wait_returncode()

        if self.returncode == 0:
            await self.wait_return_value()
        else:
            await self.wait_error()

    def poll(self) -> Optional[int]:
        """
        Check if the process has finished.  Returns `None` if the re
        """
        return self.returncode

    def kill(self) -> None:
        self.send_signal(signal.SIGKILL)
        self.status = State.FINISHED
        self.error = ProcessException("Process terminated with SIGKILL")

    def terminate(self) -> None:
        self.send_signal(signal.SIGTERM)

    def send_signal(self, sig: int) -> None:
        os.kill(self.pid, sig)


def pickle_value(value: Any) -> bytes:
    serialized_value = cloudpickle.dumps(value)
    return struct.pack('>I', len(serialized_value)) + serialized_value


async def coro_read_exactly(stream: trio.abc.ReceiveStream, num_bytes: int) -> bytes:
    buffer = io.BytesIO()
    bytes_remaining = num_bytes
    while bytes_remaining > 0:
        data = await stream.receive_some(bytes_remaining)
        if data == b'':
            raise Exception("End of stream...")
        buffer.write(data)
        bytes_remaining -= len(data)

    return buffer.getvalue()


async def coro_receive_pickled_value(stream: trio.abc.ReceiveStream) -> Any:
    logger.info('waiting for pickled length')
    len_bytes = await coro_read_exactly(stream, 4)
    serialized_len = int.from_bytes(len_bytes, 'big')
    logger.info('got pickled length: %s', serialized_len)
    logger.info('waiting for pickled payload')
    serialized_result = await coro_read_exactly(stream, serialized_len)
    logger.info('got pickled payload')
    return cloudpickle.loads(serialized_result)


async def _monitor_sub_proc(proc: Process, sub_proc: trio.Process, parent_w: int) -> None:
    logger.debug('starting subprocess to run %s', proc)
    async with sub_proc:
        # set the process ID
        proc.pid = sub_proc.pid
        logger.debug('subprocess for %s started.  pid=%d', proc, proc.pid)

        # we write the execution data immediately without waiting for the
        # `WAIT_EXEC_DATA` state to ensure that the child process doesn't have
        # to wait for that data due to the round trip times between processes.
        logger.debug('writing execution data for %s over stdin', proc)
        # pass the child process the serialized `async_fn` and `args`
        async with trio.hazmat.FdStream(parent_w) as to_child:
            await to_child.send_all(pickle_value((proc._async_fn, proc._args)))

        # this wait ensures that we
        with trio.fail_after(5):
            await proc.wait_for_state(State.WAIT_EXEC_DATA)

        with trio.fail_after(5):
            await proc.wait_for_state(State.EXECUTING)
        logger.debug('waiting for process %s finish', proc)

    proc.returncode = sub_proc.returncode
    logger.debug('process %s finished: returncode=%d', proc, proc.returncode)


async def _relay_signals(proc: Process, signal_aiter: AsyncIterator[int]) -> None:
    async for signum in signal_aiter:
        if proc.state.is_before(State.STARTED):
            # If the process has not reached the state where the child process
            # can properly handle the signal, give it a moment to reach the
            # `STARTED` stage.
            with trio.fail_after(1):
                await proc.wait_for_state(State.STARTED)
        logger.debug('relaying signal %s to child process %s', signum, proc)
        proc.send_signal(signum)


async def _monitor_state(proc: Process, from_child: trio.hazmat.FdStream) -> None:
    for current_state, next_state in sliding_window(2, State):
        if proc.state is not current_state:
            raise Exception(
                f"Invalid state.  proc in state {proc.state} but expected state {current_state}"
            )

        child_state_as_byte = await coro_read_exactly(from_child, 1)

        try:
            child_state = State(child_state_as_byte)
        except TypeError:
            raise Exception(f"Invalid state.  child sent state: {child_state_as_byte.hex()}")

        if child_state is not next_state:
            raise Exception(
                f"Invalid state.  child sent state {child_state_as_byte.hex()} "
                f"but expected state {next_state}"
            )

        proc.state = child_state

    if proc.state is not State.FINISHED:
        raise Exception(f"Invalid final state: {proc.state}")

    result = await coro_receive_pickled_value(from_child)

    # The `returncode` should already be set but we do a quick wait to ensure
    # that it will be set when we access it below.
    with trio.fail_after(5):
        await proc.wait_returncode()

    if proc.returncode == 0:
        proc.return_value = result
    else:
        proc.error = result


RELAY_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


@asynccontextmanager
@trio_typing.takes_callable_and_args
async def open_in_process(async_fn: Callable[..., TReturn], *args: Any) -> AsyncIterator[Process]:
    proc = Process(async_fn, args)

    parent_r, child_w = os.pipe()
    child_r, parent_w = os.pipe()
    parent_pid = os.getpid()

    command = get_subprocess_command(
        child_r,
        child_w,
        parent_pid,
    )

    sub_proc = await trio.open_process(
        command,
        # stdin=subprocess.PIPE,
        # stdout=subprocess.PIPE,
        # stderr=subprocess.PIPE,
        pass_fds=(child_r, child_w),
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_monitor_sub_proc, proc, sub_proc, parent_w)

        async with trio.hazmat.FdStream(parent_r) as from_child:
            with trio.open_signal_receiver(*RELAY_SIGNALS) as signal_aiter:
                # Monitor the child stream for incoming updates to the state of
                # the child process.
                nursery.start_soon(_monitor_state, proc, from_child)

                # Relay any appropriate signals to the child process.
                nursery.start_soon(_relay_signals, proc, signal_aiter)

                await proc.wait_pid()

                # Wait until the child process has reached the STARTED
                # state before yielding the context.  This ensures that any
                # calls to things like `terminate` or `kill` will be handled
                # properly in the child process.
                #
                # The timeout ensures that if something is fundamentally wrong
                # with the subprocess we don't hang indefinitely.
                with trio.fail_after(5):
                    await proc.wait_for_state(State.STARTED)

                try:
                    yield proc
                except KeyboardInterrupt as err:
                    # If a keyboard interrupt is encountered relay it to the
                    # child process and then give it a moment to cleanup before
                    # re-raising
                    try:
                        proc.send_signal(signal.SIGINT)
                        with trio.move_on_after(1):
                            await proc.wait()
                    finally:
                        raise err

                await proc.wait()

                nursery.cancel_scope.cancel()


@trio_typing.takes_callable_and_args
async def run_in_process(async_fn: Callable[..., TReturn], *args: Any) -> TReturn:
    async with open_in_process(async_fn, *args) as proc:
        await proc.wait()
    return proc.result


#
# CLI invocation for subprocesses
#
parser = argparse.ArgumentParser(description='trio-run-in-process')
parser.add_argument(
    '--parent-pid',
    type=int,
    required=True,
    help="The PID of the parent process",
)
parser.add_argument(
    '--fd-read',
    type=int,
    required=True,
    help=(
        "The file descriptor that the child process can use to read data that "
        "has been written by the parent process"
    )
)
parser.add_argument(
    '--fd-write',
    type=int,
    required=True,
    help=(
        "The file descriptor that the child process can use for writing data "
        "meant to be read by the parent process"
    ),
)


def read_exactly(stream: io.BytesIO, num_bytes: int) -> bytes:
    buffer = io.BytesIO()
    bytes_remaining = num_bytes
    while bytes_remaining > 0:
        data = stream.read(bytes_remaining)
        if data == b'':
            raise Exception("End of stream...")
        buffer.write(data)
        bytes_remaining -= len(data)

    return buffer.getvalue()


def receive_pickled_value(stream: io.BytesIO) -> Any:
    len_bytes = read_exactly(stream, 4)
    serialized_len = int.from_bytes(len_bytes, 'big')
    serialized_result = read_exactly(stream, serialized_len)
    return cloudpickle.loads(serialized_result)


def update_state(to_parent: BinaryIO, state: State) -> None:
    to_parent.write(state.value)
    to_parent.flush()


def update_state_finished(to_parent: BinaryIO, finished_payload: bytes) -> None:
    payload = State.FINISHED.value + finished_payload
    to_parent.write(payload)
    to_parent.flush()


SHUTDOWN_SIGNALS = {signal.SIGTERM}


async def _do_monitor_signals(signal_aiter: AsyncIterator[int]):
    async for signum in signal_aiter:
        raise SystemExit(signum)


@trio_typing.takes_callable_and_args
async def _do_async_fn(async_fn: Callable[..., TReturn],
                       args: Sequence[Any],
                       to_parent: trio.hazmat.FdStream) -> TReturn:
    with trio.open_signal_receiver(*SHUTDOWN_SIGNALS) as signal_aiter:
        # state: STARTED
        update_state(to_parent, State.STARTED)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(_do_monitor_signals, signal_aiter)

            # state: EXECUTING
            update_state(to_parent, State.EXECUTING)

            result = await async_fn(*args)

            # state: STOPPING
            update_state(to_parent, State.STOPPING)

            nursery.cancel_scope.cancel()
        return result


def _run_process(parent_pid: int,
                 fd_read: int,
                 fd_write: int) -> None:
    """
    Run the child process
    """
    # state: INITIALIZING
    with os.fdopen(fd_write, 'wb', closefd=True) as to_parent:
        # state: INITIALIZED
        update_state(to_parent, State.INITIALIZED)
        with os.fdopen(fd_read, 'rb', closefd=True) as from_parent:
            # state: WAIT_EXEC_DATA
            update_state(to_parent, State.WAIT_EXEC_DATA)
            async_fn, args = receive_pickled_value(from_parent)

        # state: BOOTING
        update_state(to_parent, State.BOOTING)

        try:
            try:
                result = trio.run(
                    _do_async_fn,
                    async_fn,
                    args,
                    to_parent,
                )
            except BaseException as err:
                # state: STOPPING
                update_state(to_parent, State.STOPPING)
                finished_payload = pickle_value(err)
                raise
        except KeyboardInterrupt:
            code = 2
        except SystemExit as err:
            code = err.args[0]
        except BaseException:
            code = 1
        else:
            # state: STOPPING (set from within _do_async_fn)
            finished_payload = pickle_value(result)
            code = 0
        finally:
            # state: FINISHED
            update_state_finished(to_parent, finished_payload)
            sys.exit(code)


if __name__ == "__main__":
    args = parser.parse_args()
    _run_process(
        parent_pid=args.parent_pid,
        fd_read=args.fd_read,
        fd_write=args.fd_write,
    )
