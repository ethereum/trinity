import os
import sys
from typing import (
    Callable,
    TypeVar,
)

import trio


TReturn = TypeVar('TReturn')


class Process:
    def __init__(self,
                 target: Callable[..., Any],
                 args: Sequence[Any]) -> None:
        self._target = target
        self._args = args

    def _run(self, child_r, child_w, parent_pid) -> None:
        self._target(*self._args)

    def run_process(self) -> None:
        self._spawn()

    def _spawn(self):
        parent_r, child_w = os.pipe()
        child_r, parent_w = os.pipe()
        original_pid = os.getpid()
        fork_pid = os.fork()

        if fork_pid == 0:
            parent_pid = original_pid
            os.close(parent_r)
            os.close(parent_w)
            code = self._run(child_r, child_w, parent_pid)
            sys.exit(code)
        else:
            child_pid = fork_pid
            os.close(child_r)
            os.close(child_w)
            handle_parent(parent_r, parent_w, child_pid)


async def run_in_process(async_fn: Callable[..., TReturn], *args) -> TReturn:
    proc = Process(async_fn, args)
    proc.start()
    await proc.run_process()


def test_spawning_process():
    parent_r, child_w = os.pipe()
    child_r, parent_w = os.pipe()
    original_pid = os.getpid()
    fork_pid = os.fork()

    if fork_pid == 0:
        parent_pid = original_pid
        os.close(parent_r)
        os.close(parent_w)
        code = handle_child(child_r, child_w, parent_pid)
        sys.exit(code)
    else:
        child_pid = fork_pid
        os.close(child_r)
        os.close(child_w)
        handle_parent(parent_r, parent_w, child_pid)


def handle_parent(parent_r, parent_w, child_pid):
    print('Parent', os.getpid(), parent_r, parent_w, "Child pid: ", child_pid)


def handle_child(child_r, child_w, parent_pid):
    print('Child:', os.getpid(), child_r, child_w, "Parent pid: ", parent_pid)
    return 0


if __name__ == '__main__':
    test_spawning_process()
