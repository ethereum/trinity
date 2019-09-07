import argparse
import io
import logging
import os
import struct
import sys
from typing import (
    Any,
    Callable,
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


def pickle_value(value: Any) -> bytes:
    serialized_value = cloudpickle.dumps(value)
    return struct.pack('>I', len(serialized_value)) + serialized_value


class Process:
    def __init__(self,
                 async_fn: Callable[..., Any],
                 args: Sequence[Any]) -> None:
        self._async_fn = async_fn
        self._args = args

    async def run_process(self):
        parent_r, child_w = os.pipe()
        child_r, parent_w = os.pipe()
        parent_pid = os.getpid()

        command = get_subprocess_command(
            child_r,
            child_w,
            parent_pid,
        )

        async with await trio.open_file(parent_w, 'wb', closefd=True) as to_child:
            async with await trio.open_file(parent_r, 'rb', closefd=True) as from_child:
                async with await trio.open_file(child_w, 'wb', closefd=False) as to_parent:
                    async with await trio.open_file(child_r, 'rb', closefd=False) as from_parent:
                        proc = await trio.open_process(command, stdin=from_parent, stdout=to_parent)
                        async with proc:
                            await to_child.write(pickle_value((self._async_fn, self._args)))
                            await to_child.flush()

                        if proc.returncode == 0:
                            result = await coro_receive_pickled_value(from_child)
                            return result
                        else:
                            error = await coro_receive_pickled_value(from_child)
                            raise error


async def run_in_process(async_fn: Callable[..., TReturn], *args) -> TReturn:
    proc = Process(async_fn, args)
    # TODO: signal handling
    return await proc.run_process()


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
