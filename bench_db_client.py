from trinity.db_manager import (
    DBManager,
    AsyncDBClient,
    DBClient,
    _wait_for_path,
)
from eth.db.atomic import AtomicDB
from eth.db.backends.level import LevelDB

import multiprocessing

import trio
import os
import signal
import pathlib
import time
import random
from trinity._utils.profiling import profiler

IPC_PATH = trio.Path("./foo.ipc")


def random_bytes(num):
    return random.getrandbits(8 * num).to_bytes(num, 'little')


key_values = {
    random_bytes(32): random_bytes(256)
    for i in range(10000)
}


def run_server(ipc_path):
    db = LevelDB("./bar")
    manager = DBManager(db)
    try:
        trio.run(manager.serve, ipc_path)
        print("Exit run server")
    except KeyboardInterrupt:
        pathlib.Path(ipc_path).unlink()


async def run_async_client(ipc_path):
    # with profiler("client.prof"):
    await _wait_for_path(ipc_path)
    db_client = DBClient.connect(ipc_path)

    for _ in range(3):
        start = time.perf_counter()
        for key, value in key_values.items():
            db_client.set(key, value)
            db_client.get(key)
        end = time.perf_counter()
        duration = end - start

        num_keys = len(key_values)
        print(f"The new: Takes {duration} seconds to do {num_keys} times of get-set.")
        print(f"{num_keys/duration} get-set per second")


def outer(ipc_path):
    trio.run(run_async_client, ipc_path)


if __name__ == '__main__':

    server = multiprocessing.Process(target=run_server, args=[IPC_PATH])
    client = multiprocessing.Process(target=outer, args=[IPC_PATH])
    server.start()
    client.start()
    client.join(600)

    os.kill(server.pid, signal.SIGINT)
    server.join(1)
