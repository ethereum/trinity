import argparse
import io
import logging
import os
import signal
import struct
import sys

from async_generator import asynccontextmanager
import trio_typing
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Optional,
    Sequence,
    TypeVar,
)

import cloudpickle
import trio


TReturn = TypeVar('TReturn')


logger = logging.getLogger('trio.multiprocessing')


def get_subprocess_command(child_r, child_w, parent_pid):
    return (
        sys.executable,
        '-m', 'p2p.trio_run_in_process',
        '--parent-pid', str(parent_pid),
        '--fd-read', str(child_r),
        '--fd-write', str(child_w),
    )


def pickle_value(value: Any) -> bytes:
    serialized_value = cloudpickle.dumps(value)
    return struct.pack('>I', len(serialized_value)) + serialized_value


async def coro_read_exactly(stream: trio.abc.ReceiveStream, num_bytes: int) -> bytes:
    buffer = io.BytesIO()
    bytes_remaining = num_bytes
    while bytes_remaining > 0:
        data = await stream.read(bytes_remaining)
        if data == b'':
            raise Exception("End of stream...")
        buffer.write(data)
        bytes_remaining -= len(data)

    return buffer.getvalue()


async def coro_receive_pickled_value(stream: trio.abc.ReceiveStream) -> Any:
    len_bytes = await coro_read_exactly(stream, 4)
    serialized_len = int.from_bytes(len_bytes, 'big')
    serialized_result = await coro_read_exactly(stream, serialized_len)
    return cloudpickle.loads(serialized_result)


class empty:
    pass


class Process(Awaitable[TReturn]):
    returncode: Optional[int] = None

    _pid: Optional[int] = None
    _result: Optional[TReturn] = empty
    _returncode: Optional[int] = None
    _error: Optional[BaseException] = None

    def __init__(self, async_fn: Callable[..., TReturn], args: Sequence[TReturn]) -> None:
        self._async_fn = async_fn
        self._args = args

        self._has_pid = trio.Event()
        self._has_returncode = trio.Event()
        self._has_result = trio.Event()
        self._has_error = trio.Event()

    def __await__(self) -> TReturn:
        return self.run().__await__()

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
    # Result
    #
    @property
    def result(self) -> int:
        if self._result is empty:
            raise AttributeError("No result set")
        return self._result

    @result.setter
    def result(self, value: int) -> None:
        self._result = value
        self._has_result.set()

    async def wait_result(self) -> int:
        await self._has_result.wait()
        return self.result

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

    async def wait(self) -> None:
        """
        Block until the process has exited.
        """
        await self.wait_returncode()

        if self.returncode == 0:
            await self.wait_result()
        else:
            raise await self.wait_error()

    def poll(self) -> Optional[int]:
        """
        Check if the process has finished.  Returns `None` if the re
        """
        return self.returncode

    def kill(self) -> None:
        self.send_signal(signal.SIGKILL)

    def terminate(self) -> None:
        self.send_signal(signal.SIGTERM)

    def send_signal(self, sig: int) -> None:
        os.kill(self.pid, sig)


async def _monitor_sub_proc(proc: Process) -> None:
    parent_r, child_w = os.pipe()
    child_r, parent_w = os.pipe()
    parent_pid = os.getpid()

    command = get_subprocess_command(
        child_r,
        child_w,
        parent_pid,
    )

    async with await trio.open_file(child_w, 'wb', closefd=False) as to_parent:
        async with await trio.open_file(child_r, 'rb', closefd=False) as from_parent:
            sub_proc = await trio.open_process(command, stdin=from_parent, stdout=to_parent)
            logger.debug('starting subprocess to run %s', proc)
            async with sub_proc:
                # set the process ID
                proc.pid = sub_proc.pid
                logger.debug('subprocess for %s started.  pid=%d', proc, proc.pid)

                logger.debug('writing execution data for %s over stdin', proc)
                # pass the child process the serialized `async_fn`
                # and `args` over stdin.
                async with await trio.open_file(parent_w, 'wb', closefd=True) as to_child:
                    await to_child.write(pickle_value((proc._async_fn, proc._args)))
                    await to_child.flush()
                logger.debug('waiting for process %s finish', proc)

    proc.returncode = sub_proc.returncode
    logger.debug('process %s finished: returncode=%d', proc, proc.returncode)

    async with await trio.open_file(parent_r, 'rb', closefd=True) as from_child:
        if proc.returncode == 0:
            logger.debug('setting result for process %s', proc)
            proc.result = await coro_receive_pickled_value(from_child)
        else:
            with trio.move_on_after(2) as scope:
                logger.debug('setting error for process %s', proc)
                proc.error = await coro_receive_pickled_value(from_child)
            if scope.cancelled_caught:
                logger.debug('process %s exited due unknown reason.', proc)
                proc.error = SystemExit(proc.returncode)


async def _monitory_signals(proc: Process, signal_aiter: AsyncIterator[int]) -> None:
    async for signum in signal_aiter:
        logger.info('GOT SIGNAL: %s', signum)
        proc.send_signal(signum)


@asynccontextmanager
@trio_typing.takes_callable_and_args
async def open_in_process(async_fn: Callable[..., TReturn], *args: Any) -> AsyncIterator[Process]:
    proc = Process(async_fn, args)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_monitor_sub_proc, proc)

        await proc.wait_pid()

        with trio.open_signal_receiver(signal.SIGTERM) as signal_aiter:
            nursery.start_soon(_monitory_signals, proc, signal_aiter)

            yield proc
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


def _run_process(parent_pid: int,
                 fd_read: int,
                 fd_write: int) -> None:
    with os.fdopen(sys.stdin.fileno(), 'rb', closefd=True) as stdin_binary:
        async_fn, args = receive_pickled_value(stdin_binary)

    # TODO: signal handling
    try:
        result = trio.run(async_fn, *args)
    except BaseException as err:
        with os.fdopen(sys.stdout.fileno(), 'wb', closefd=True) as stdout_binary:
            stdout_binary.write(pickle_value(err))
        sys.exit(1)
    else:
        logger.debug("Ran successfully: %r", result)
        with os.fdopen(sys.stdout.fileno(), 'wb', closefd=True) as stdout_binary:
            stdout_binary.write(pickle_value(result))
        sys.exit(0)


if __name__ == "__main__":
    args = parser.parse_args()
    _run_process(
        parent_pid=args.parent_pid,
        fd_read=args.fd_read,
        fd_write=args.fd_write,
    )
