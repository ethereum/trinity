from .constants import LEN_BYTES


def len_bytes(data):
    return len(data).to_bytes(LEN_BYTES, 'little')


def bytes_to_int(data):
    return int.from_bytes(data, 'little')
