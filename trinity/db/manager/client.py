import logging
import pathlib
import socket

import trio

from trinity._utils.ipc import (
    wait_for_ipc,
)

from .schema import (
    DELETE,
    EXIST,
    GET,
    SET,
)
from .exceptions import (
    OperationError,
)


async def _wait_for_path(path: trio.Path):
    path = trio.Path(path)
    while not await path.exists():
        await trio.sleep(0.001)


class SyncDBClient:
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
        message = GET.client_request_message(key)
        self._socket.sendall(message)
        try:
            value = GET.client_reads_server_response_sync(self.read_exactly)
            return value
        except OperationError as e:
            raise KeyError(f"Key does not exist: {key}")

    def set(self, key: bytes, value: bytes) -> None:
        message = SET.client_request_message(key, value)
        self._socket.sendall(message)
        try:
            SET.client_reads_server_response_sync(self.read_exactly)
        except OperationError:
            raise Exception(f"Fail to Write {key}:{value}")

    def delete(self, key):
        message = DELETE.client_request_message(key)
        self._socket.sendall(message)
        try:
            DELETE.client_reads_server_response_sync(self.read_exactly)
        except OperationError:
            raise Exception(f"Fail to delete {key}")

    def exists(self, key):
        message = EXIST.client_request_message(key)
        self._socket.sendall(message)
        try:
            exist = EXIST.client_reads_server_response_sync(self.read_exactly)
            return exist
        except OperationError:
            raise Exception(f"Fail to query existence of key: {key}")

    @classmethod
    def connect(cls, path: pathlib.Path) -> "TrioConnection":
        wait_for_ipc(path)
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
            message = GET.client_request_message(key)
            await self._socket.send_all(message)
            try:
                value = await GET.client_reads_server_response_sync(self.read_exactly)
                return value
            except OperationError as e:
                raise KeyError(f"Key does not exist: {key}")

    async def set(self, key, value):
        async with self._lock:
            message = SET.client_request_message(key, value)
            await self._socket.send_all(message)
            try:
                await SET.client_reads_server_response_sync(self.read_exactly)
            except OperationError:
                raise Exception(f"Fail to Write {key}:{value}")

    async def delete(self, key):
        async with self._lock:
            message = DELETE.client_request_message(key)
            await self._socket.send_all(message)
            try:
                await DELETE.client_reads_server_response_sync(self.read_exactly)
            except OperationError:
                raise Exception(f"Fail to delete {key}")

    async def exists(self, key):
        async with self._lock:
            message = EXIST.client_request_message(key)
            await self._socket.send_all(message)
            try:
                exist = await EXIST.client_reads_server_response_sync(self.read_exactly)
                return exist
            except OperationError:
                raise Exception(f"Fail to query existence of key: {key}")

    @classmethod
    async def connect(cls, path: pathlib.Path) -> "TrioConnection":
        await _wait_for_path(path)
        s = await trio.open_unix_socket(str(path))
        cls.logger.debug("Opened connection to %s: %s", path, s)
        return cls(s)
