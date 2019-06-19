from eth.db.atomic import AtomicDB
import logging
import threading
import socket
import trio
import pathlib
import pytest
import tempfile
import pytest_trio
from async_generator import asynccontextmanager


class DBManager:
    logger = logging.getLogger('manager')

    def __init__(self, db):
        self._server_running = trio.Event()
        self.db = db

    async def serve(self, ipc_path) -> None:
        async with trio.open_nursery() as nursery:
            # Store nursery on self so that we can access it for cancellation
            self._server_nursery = nursery

            self.logger.debug("%s: server starting", self)
            s = trio.socket.socket(trio.socket.AF_UNIX, trio.socket.SOCK_STREAM)
            await s.bind(ipc_path.__fspath__())
            s.listen(1)
            listener = trio.SocketListener(s)

            self._server_running.set()

            try:
                await trio.serve_listeners(
                    handler=self._accept_conn,
                    listeners=(listener,),
                    handler_nursery=nursery,
                )
            finally:
                self.logger.debug("%s: server stopping", self)

    async def _accept_conn(self, s: trio.SocketStream) -> None:
        self.logger.debug("%s: starting client handler for %s", self, s)
        buffer = bytearray()

        async def read_exactly(num_bytes):
            nonlocal buffer
            while len(buffer) < num_bytes:

                data = await s.receive_some(4096)

                if data == b"":
                    self.logger.debug("Connection closed")
                    return

                buffer.extend(data)
            payload = buffer[:num_bytes]
            buffer = buffer[num_bytes:]
            return bytes(payload)

        while True:
            operation = await read_exactly(1)
            if operation == b'\x00':
                self.logger.debug("GET")
                key_length_data = await read_exactly(4)
                key_length = int.from_bytes(key_length_data, 'little')
                key = await read_exactly(key_length)
                value = self.db[key]
                value_length = len(value)
                await s.send_all(value_length.to_bytes(4, 'little') + value)
            else:
                raise Exception(f"Got unknown operation {operation}")

        assert False

    @asynccontextmanager
    async def run(self, ipc_path):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.serve, ipc_path)
            await self._server_running.wait()
            await _wait_for_path(trio.Path(ipc_path))
            try:
                yield self
            finally:
                nursery.cancel_scope.cancel()


class DBClient:
    logger = logging.getLogger('client')

    def __init__(self, s):
        self._socket = s
        self._lock = trio.Lock()
        self._buffer = bytearray()

    async def read_exactly(self, num_bytes):
        if not self._lock.locked():
            raise Exception("Not locked")

        while len(self._buffer) < num_bytes:

            data = await self._socket.receive_some(4096)

            if data == b"":
                raise Exception("socket closed")

            self._buffer.extend(data)
        payload = self._buffer[:num_bytes]
        self._buffer = self._buffer[num_bytes:]
        return bytes(payload)

    async def get(self, key):
        async with self._lock:
            key_length = len(key)
            await self._socket.send_all(b'\x00' + key_length.to_bytes(4, 'little') + key)
            value_length_data = await self.read_exactly(4)
            value_length = int.from_bytes(value_length_data, 'little')
            value = await self.read_exactly(value_length)
            return value


    @classmethod
    async def connect(cls, path: pathlib.Path) -> "TrioConnection":
        s = await trio.open_unix_socket(str(path))
        cls.logger.debug("Opened connection to %s: %s", path, s)
        return cls(s)


@pytest.fixture
def ipc_path():
    with tempfile.TemporaryDirectory() as dir:
        yield pathlib.Path(dir) / "foo.ipc"


async def _wait_for_path(path):
    while not await path.exists():
        await trio.sleep(0.005)


@pytest.fixture
def db():
    return AtomicDB()


@pytest_trio.trio_fixture
async def manager(db, ipc_path):
    m = DBManager(db)
    async with m.run(ipc_path):
        yield m


@pytest.mark.trio
async def test_the_thing(ipc_path, db, manager):
    db[b'key'] = b'value'

    client_db = await DBClient.connect(ipc_path)
    assert await client_db.get(b'key') == b'value'
