from .constants import LEN_BYTES


def encode_length(data: bytes) -> bytes:
    return len(data).to_bytes(LEN_BYTES, 'little')


def decode_length(data: bytes) -> int:
    return int.from_bytes(data, 'little')
