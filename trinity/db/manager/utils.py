from .constants import LEN_BYTES


def len_bytes(data: bytes) -> bytes:
    return len(data).to_bytes(LEN_BYTES, 'little')


def bytes_to_int(data: bytes) -> int:
    return int.from_bytes(data, 'little')
