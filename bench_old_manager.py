
import asyncio
from eth.db.atomic import AtomicDB
import multiprocessing
from trinity.db.base import AsyncDBProxy
from multiprocessing.managers import (
    BaseManager,
)
from trinity.db.beacon.manager import (
    create_db_consumer_manager,
)

import trio
import os
import signal
import pathlib
import time
import random

IPC_PATH = pathlib.Path("./foo.ipc")


def random_bytes(num):
    return random.getrandbits(8 * num).to_bytes(num, 'little')


key_values = {
    random_bytes(32): random_bytes(256)
    for i in range(10000)
}


def run_server(ipc_path):
    db = AtomicDB()

    class DBManager(BaseManager):
        pass

    if ipc_path.exists():
        ipc_path.unlink()
    DBManager.register(
        'get_db', callable=lambda: db, proxytype=AsyncDBProxy)
    manager = DBManager(address=str(ipc_path))
    server = manager.get_server()

    try:
        server.serve_forever()
        print("Exit run server")
    except KeyboardInterrupt:
        pathlib.Path(ipc_path).unlink()


async def run_async_client(ipc_path):
    db_manager = create_db_consumer_manager(ipc_path)
    db_client = db_manager.get_db()

    for _ in range(3):
        start = time.perf_counter()

        for key, value in key_values.items():
            await db_client.coro_set(key, value)
            await db_client.coro_get(key)
        end = time.perf_counter()
        duration = end - start

        num_keys = len(key_values)
        print(f"The old: Takes {duration} seconds to do {num_keys} times of get-set.")
        print(f"{num_keys/duration} get-set per second")


def outer(ipc_path):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_async_client(ipc_path))


if __name__ == '__main__':

    server = multiprocessing.Process(target=run_server, args=[IPC_PATH])
    client = multiprocessing.Process(target=outer, args=[IPC_PATH])
    server.start()
    client.start()
    client.join(600)

    os.kill(server.pid, signal.SIGINT)
    server.join(1)
