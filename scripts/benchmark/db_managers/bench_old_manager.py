from eth.db.backends.level import LevelDB
import multiprocessing
from trinity.db.base import AsyncDBProxy
from multiprocessing.managers import (
    BaseManager,
)
from trinity.db.beacon.manager import (
    create_db_consumer_manager,
)

import os
import signal
import pathlib
import time
import random

IPC_PATH = pathlib.Path("./tmp.ipc")
DB_PATH = pathlib.Path("./tmp-db")

DB = LevelDB(db_path=DB_PATH)


def random_bytes(num):
    return random.getrandbits(8 * num).to_bytes(num, 'little')


def run_server(ipc_path, db):

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


def run_client(ipc_path, client_id):
    key_values = {
        random_bytes(32): random_bytes(256)
        for i in range(10000)
    }

    db_manager = create_db_consumer_manager(ipc_path)
    db_client = db_manager.get_db()

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
    server = multiprocessing.Process(target=run_server, args=[IPC_PATH, DB])
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
