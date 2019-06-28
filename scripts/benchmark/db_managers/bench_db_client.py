import pathlib
from trinity.db_manager import (
    DBManager,
    DBClient,
)
from eth.db.backends.level import LevelDB

import multiprocessing

import os
import signal
import time
import random

IPC_PATH = pathlib.Path("./foo.ipc")
DB_PATH = pathlib.Path("./tmp-db/")


def random_bytes(num):
    return random.getrandbits(8 * num).to_bytes(num, 'little')


def run_server(ipc_path):
    db = LevelDB(db_path=DB_PATH)
    manager = DBManager(db)

    with manager.run(ipc_path):
        try:
            manager.wait_stopped()
        except KeyboardInterrupt:
            pass

    ipc_path.unlink()


def run_client(ipc_path, client_id):

    key_values = {
        random_bytes(32): random_bytes(256)
        for i in range(10000)
    }

    db_client = DBClient.connect(ipc_path)

    for _ in range(3):
        start = time.perf_counter()
        for key, value in key_values.items():
            db_client.set(key, value)
            db_client.get(key)
        end = time.perf_counter()
        duration = end - start

        num_keys = len(key_values)
        print(f"Client {client_id}: {num_keys/duration} get-set per second")


if __name__ == '__main__':
    if IPC_PATH.exists():
        IPC_PATH.unlink()

    server = multiprocessing.Process(target=run_server, args=[IPC_PATH])

    clients = [
        multiprocessing.Process(target=run_client, args=[IPC_PATH, i])
        for i in range(3)
    ]
    server.start()
    for client in clients:
        client.start()
    for client in clients:
        client.join(600)

    os.kill(server.pid, signal.SIGINT)
    server.join(1)
