from abc import (
    ABC,
)
from .constants import (
    SUCCESS_BYTE,
    FAIL_BYTE,
    LEN_BYTES,
)

from .utils import (
    len_bytes,
    bytes_to_int,
)
from typing import (
    Tuple,
    Callable,
)


def read_key(read_exactly: Callable[[int], bytes]) -> bytes:
    key_length_data = read_exactly(LEN_BYTES)
    key = read_exactly(bytes_to_int(key_length_data))
    return key


class Operation(ABC):
    code: bytes

    @staticmethod
    def server_responds_fail_message() -> bytes:
        return FAIL_BYTE


class GET(Operation):
    code = b'\x00'

    @classmethod
    def client_request_message(cls, key) -> bytes:
        return cls.code + len_bytes(key) + key

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> bytes:
        return read_key(read_exactly)

    @staticmethod
    def server_responds_success_message(value) -> bytes:
        return SUCCESS_BYTE + len_bytes(value) + value


class SET(Operation):
    code = b'\x01'

    @classmethod
    def client_request_message(cls, key, value) -> bytes:
        return cls.code + len_bytes(key) + len_bytes(value) + key + value

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> Tuple[bytes, bytes]:
        length_data = read_exactly(LEN_BYTES + LEN_BYTES)
        key_length = bytes_to_int(length_data[:LEN_BYTES])
        value_length = bytes_to_int(length_data[LEN_BYTES:])
        payload = read_exactly(key_length + value_length)
        key, value = payload[:key_length], payload[key_length:]
        return key, value

    @staticmethod
    def server_responds_success_message() -> bytes:
        return SUCCESS_BYTE


class DELETE(Operation):
    code = b'\x02'

    @classmethod
    def client_request_message(cls, key) -> bytes:
        return cls.code + len_bytes(key) + key

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> bytes:
        return read_key(read_exactly)

    @staticmethod
    def server_responds_success_message() -> bytes:
        return SUCCESS_BYTE


class EXIST(Operation):
    code = b'\x03'

    @classmethod
    def client_request_message(cls, key) -> bytes:
        return cls.code + len_bytes(key) + key

    @staticmethod
    def server_reads_client_request(read_exactly: Callable[[int], bytes]) -> bytes:
        return read_key(read_exactly)

    @staticmethod
    def server_responds_success_message(result: bool) -> bytes:
        return SUCCESS_BYTE + result.to_bytes(1, 'little')
