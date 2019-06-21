import logging
import socket
import pathlib
import io
import trio


def _block_for_path(path):
    while not path.exists():
        time.sleep(0.001)


async def _wait_for_path(path: trio.Path):
    while not await path.exists():
        await trio.sleep(0.001)


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

    def get(self, key: bytes) -> bytes:
        key_length = len(key)
        self._socket.sendall(b'\x00' + key_length.to_bytes(4, 'little') + key)

        value_exists = self.read_exactly(1)
        if value_exists == b'\x00':
            raise KeyError(f"Key does not exist: {key}")

        value_length_data = self.read_exactly(4)
        value_length = int.from_bytes(value_length_data, 'little')
        value = self.read_exactly(value_length)
        return value

    def set(self, key: bytes, value: bytes) -> None:
        key_length = len(key)
        value_length = len(value)
        self._socket.sendall(
            b'\x01' +
            key_length.to_bytes(4, 'little') +
            value_length.to_bytes(4, 'little') +
            key + value
        )
        self.read_exactly(1)

    @classmethod
    def connect(cls, path: pathlib.Path) -> "TrioConnection":
        _block_for_path(path)
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
