from trinity.db_manager import (
    DBManager,
    AsyncDBClient,
    DBClient,
    _wait_for_path,
)
from eth.db.atomic import AtomicDB
import multiprocessing

import trio
import os
import signal
import pathlib
import time
import random

IPC_PATH = trio.Path("./foo.ipc")


def random_bytes(num):
    return random.getrandbits(8 * num).to_bytes(num, 'little')


key_values = {
    random_bytes(32): random_bytes(256)
    for i in range(10000)
}


def run_server(ipc_path):
    db = AtomicDB()
    manager = DBManager(db)
    try:
        trio.run(manager.serve, ipc_path)
        print("Exit run server")
    except KeyboardInterrupt:
        pathlib.Path(ipc_path).unlink()


async def run_async_client(ipc_path):
    db_client = await AsyncDBClient.connect(ipc_path)
    start = time.perf_counter()

    for key, value in key_values.items():
        await db_client.set(key, value)
        await db_client.get(key)
    end = time.perf_counter()
    duration = end - start

    num_keys = len(key_values)
    print(
        f"Takes {duration} seconds to do {num_keys} times of get-set.",
        f"avg {duration/num_keys *1000000} µs per get-set."
    )


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
