import logging
import socket
from async_generator import asynccontextmanager
import trio
import pathlib
import io


async def _wait_for_path(path):
    while not await path.exists():
        await trio.sleep(0.005)


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
                    raise Exception("Connection closed")

                buffer.extend(data)
            payload = buffer[:num_bytes]
            buffer = buffer[num_bytes:]
            return bytes(payload)

        while True:
            try:
                operation = await read_exactly(1)
            except Exception as error:
                self.logger.debug("closing connection, no operation %s", error)
                return
            if operation == b'\x00':
                # self.logger.debug("GET")
                key_length_data = await read_exactly(4)
                key_length = int.from_bytes(key_length_data, 'little')
                key = await read_exactly(key_length)
                try:
                    value = self.db[key]
                    value_length = len(value)
                    await s.send_all(value_length.to_bytes(4, 'little') + value)
                except KeyError:
                    self.logger.debug("Key %s doesn't exist", key)
                    value_length = 0
                    await s.send_all(value_length.to_bytes(4, 'little'))
            elif operation == b'\x01':
                # self.logger.debug("SET")
                length_data = await read_exactly(8)
                key_length = int.from_bytes(length_data[:4], 'little')
                value_length = int.from_bytes(length_data[4:], 'little')
                payload = await read_exactly(key_length + value_length)
                key, value = payload[:key_length], payload[key_length:value_length]
                self.db[key] = value
                await s.send_all((1).to_bytes(1, 'little'))
            elif operation == b'\x02':
                # self.logger.debug("DEL")
                key_length_data = await read_exactly(4)
                key_length = int.from_bytes(key_length_data, 'little')
                key = await read_exactly(key_length)
                del self.db[key]
                await s.send_all(b'\x00')
            elif operation == b'\x03':
                # self.logger.debug("EXIST")
                key_length_data = await read_exactly(4)
                key_length = int.from_bytes(key_length_data, 'little')
                key = await read_exactly(key_length)
                result = key in self.db
                # self.logger.debug("Existance of %s, %s", key, result)
                await s.send_all(result.to_bytes(1, 'little'))
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
        self._buffer = bytearray()

    def read_exactly(self, num_bytes):

        while len(self._buffer) < num_bytes:

            data = self._socket.recv(4096)

            if data == b"":
                raise Exception("socket closed")

            self._buffer.extend(data)
        payload = self._buffer[:num_bytes]
        self._buffer = self._buffer[num_bytes:]
        return bytes(payload)

    def get(self, key):
        key_length = len(key)
        self._socket.sendall(b'\x00' + key_length.to_bytes(4, 'little') + key)
        value_length_data = self.read_exactly(4)
        value_length = int.from_bytes(value_length_data, 'little')
        value = self.read_exactly(value_length)
        return value

    @classmethod
    def connect(cls, path: pathlib.Path) -> "TrioConnection":
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        cls.logger.debug("Opened connection to %s: %s", path, s)
        s.connect(str(path))
        return cls(s)


class AsyncDBClient:
    logger = logging.getLogger('async_client')

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
            payload = io.BytesIO()
            payload.write(b'\x00')
            payload.write(key_length.to_bytes(4, 'little'))
            payload.write(key)
            d = payload.getvalue()
            await self._socket.send_all(d)
            value_length_data = await self.read_exactly(4)
            value_length = int.from_bytes(value_length_data, 'little')
            if value_length == 0:
                raise KeyError(f"Key {key} doesn't exist")
            value = await self.read_exactly(value_length)
            return value

    async def set(self, key, value):
        async with self._lock:
            key_length = len(key)
            value_length = len(value)
            payload = io.BytesIO()
            payload.write(b'\x01')
            payload.write(key_length.to_bytes(4, 'little'))
            payload.write(value_length.to_bytes(4, 'little'))
            payload.write(key)
            payload.write(value)
            d = payload.getvalue()
            await self._socket.send_all(d)
            result = await self.read_exactly(1)
            if int.from_bytes(result, 'little') != 1:
                raise Exception(f"Fail to Write {key}:{value}")

    async def delete(self, key):
        async with self._lock:
            key_length = len(key)
            await self._socket.send_all(b'\x02' + key_length.to_bytes(4, 'little') + key)
            await self.read_exactly(1)

    async def exists(self, key):
        async with self._lock:
            key_length = len(key)
            await self._socket.send_all(b'\x03' + key_length.to_bytes(4, 'little') + key)
            result_data = await self.read_exactly(1)
            if int.from_bytes(result_data, "little") == 1:
                return True
            else:
                return False

    @classmethod
    async def connect(cls, path: pathlib.Path) -> "TrioConnection":
        await _wait_for_path(path)
        s = await trio.open_unix_socket(str(path))
        cls.logger.debug("Opened connection to %s: %s", path, s)
        return cls(s)
