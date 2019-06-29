from abc import (
    ABC,
)
from .constants import (
    SUCCESS_BYTE,
    FAIL_BYTE,
    LEN_BYTES,
)

from .utils import (
    encode_length,
    decode_length,
)
from typing import (
    Tuple,
    Callable,
    Awaitable,
)
from .exceptions import (
    OperationError,
)


def read_key(read_exactly: Callable[[int], bytes]) -> bytes:
    key_length_data = read_exactly(LEN_BYTES)
    key = read_exactly(decode_length(key_length_data))
    return key


class Operation(ABC):
    code: bytes

    @staticmethod
    def server_responds_fail_message() -> bytes:
        return FAIL_BYTE


class GET(Operation):
    code = b'\x00'

    @classmethod
    def client_request_message(cls, key: bytes) -> bytes:
        return cls.code + encode_length(key) + key

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> bytes:
        return read_key(read_exactly)

    @staticmethod
    def server_responds_success_message(value: bytes) -> bytes:
        return SUCCESS_BYTE + encode_length(value) + value

    @staticmethod
    def client_reads_server_response_sync(read_exactly: Callable[[int], bytes]) -> bytes:
        success = read_exactly(1)
        if success == FAIL_BYTE:
            raise OperationError()
        value_length_data = read_exactly(LEN_BYTES)
        value = read_exactly(decode_length(value_length_data))
        return value

    @staticmethod
    async def client_reads_server_response_async(
            read_exactly: Callable[[int], Awaitable[bytes]]) -> bytes:
        success = await read_exactly(1)
        if success == FAIL_BYTE:
            raise OperationError()
        value_length_data = await read_exactly(LEN_BYTES)
        value = await read_exactly(decode_length(value_length_data))
        return value


class SET(Operation):
    code = b'\x01'

    @classmethod
    def client_request_message(cls, key: bytes, value: bytes) -> bytes:
        return cls.code + encode_length(key) + encode_length(value) + key + value

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> Tuple[bytes, bytes]:
        length_data = read_exactly(LEN_BYTES + LEN_BYTES)
        key_length = decode_length(length_data[:LEN_BYTES])
        value_length = decode_length(length_data[LEN_BYTES:])
        payload = read_exactly(key_length + value_length)
        key, value = payload[:key_length], payload[key_length:]
        return key, value

    @staticmethod
    def server_responds_success_message() -> bytes:
        return SUCCESS_BYTE

    @staticmethod
    def client_reads_server_response_sync(read_exactly: Callable[[int], bytes]) -> None:
        success = read_exactly(1)
        if success == FAIL_BYTE:
            raise OperationError()

    @staticmethod
    async def client_reads_server_response_async(
            read_exactly: Callable[[int], Awaitable[bytes]]) -> None:
        success = await read_exactly(1)
        if success == FAIL_BYTE:
            raise OperationError()


class DELETE(Operation):
    code = b'\x02'

    @classmethod
    def client_request_message(cls, key: bytes) -> bytes:
        return cls.code + encode_length(key) + key

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> bytes:
        return read_key(read_exactly)

    @staticmethod
    def server_responds_success_message() -> bytes:
        return SUCCESS_BYTE

    @staticmethod
    def client_reads_server_response_sync(read_exactly: Callable[[int], bytes]) -> None:
        success = read_exactly(1)
        if success == FAIL_BYTE:
            raise OperationError()

    @staticmethod
    async def client_reads_server_response_async(
            read_exactly: Callable[[int], Awaitable[bytes]]) -> None:
        success = await read_exactly(1)
        if success == FAIL_BYTE:
            raise OperationError()


class EXIST(Operation):
    code = b'\x03'

    @classmethod
    def client_request_message(cls, key: bytes) -> bytes:
        return cls.code + encode_length(key) + key

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> bytes:
        return read_key(read_exactly)

    @staticmethod
    def server_responds_success_message(result: bool) -> bytes:
        return SUCCESS_BYTE + result.to_bytes(1, 'little')

    @staticmethod
    def client_reads_server_response_sync(read_exactly: Callable[[int], bytes]) -> bool:
        data = read_exactly(2)
        success, exist = data[:1], bool(data[1])
        if success == FAIL_BYTE:
            raise OperationError()
        return exist

    @staticmethod
    async def client_reads_server_response_async(
            read_exactly: Callable[[int], Awaitable[bytes]]) -> bool:
        data = await read_exactly(2)
        success, exist = data[:1], bool(data[1])
        if success == FAIL_BYTE:
            raise OperationError()
        return exist
