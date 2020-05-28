import collections
from typing import Hashable, Sequence, Tuple, TypeVar, AsyncGenerator, Any

import rlp

import eth_utils


# Try to import a snappy exception from a few places
try:
    # This is largely a cargo-cult incantation, inspired by:
    # https://github.com/andrix/python-snappy/blob/602e9c10d743f71bef0bac5e4c4dffa17340d7b3/snappy/snappy.py#L47-L52  # noqa: E501
    from snappy._snappy import CompressedLengthError as snappy_CompressedLengthError
except (ImportError, ModuleNotFoundError):
    try:
        # According to the python-snappy structure, one these should be capable of
        #   importing the exception. But since it digs into the internals,
        #   it could be yanked away from us at any moment, so wrap it in another try:
        from snappy.snappy_cffi import CompressedLengthError as snappy_CompressedLengthError
    except (ImportError, ModuleNotFoundError):
        # As a last resort, we can just treat it as some unknown exception. A "try/catch" will
        #   miss the exception, but that's an acceptable way to find out that the API
        #   has changed.
        class _NewException(Exception):
            pass

        # Trying to name the class snappy_CompressedLengthError confuses mypy, hence the rename
        snappy_CompressedLengthError = _NewException


def get_logger(name: str) -> eth_utils.ExtendedDebugLogger:
    # Just a local wrapper around trinity's get_logger() as we need to delay the import to avoid
    # cyclical imports ar parse time.
    from trinity._utils.logging import get_logger as trinity_get_logger
    return trinity_get_logger(name)


def sxor(s1: bytes, s2: bytes) -> bytes:
    if len(s1) != len(s2):
        raise ValueError("Cannot sxor strings of different length")
    return bytes(x ^ y for x, y in zip(s1, s2))


def roundup_16(x: int) -> int:
    """Rounds up the given value to the next multiple of 16."""
    remainder = x % 16
    if remainder != 0:
        x += 16 - remainder
    return x


def get_devp2p_cmd_id(msg: bytes) -> int:
    """Return the cmd_id for the given devp2p msg.

    The cmd_id, also known as the payload type, is always the first entry of the RLP, interpreted
    as an integer.
    """
    return rlp.decode(msg[:1], sedes=rlp.sedes.big_endian_int)


def trim_middle(arbitrary_string: str, max_length: int) -> str:
    """
    Trim down strings to max_length by cutting out the middle.
    This assumes that the most "interesting" bits are toward
    the beginning and the end.

    Adds a highly unusual '✂✂✂' in the middle where characters
    were stripped out, to avoid not realizing about the stripped
    info.
    """
    # candidate for moving to eth-utils, if we like it
    size = len(arbitrary_string)
    if size <= max_length:
        return arbitrary_string
    else:
        half_len, is_odd = divmod(max_length, 2)
        first_half = arbitrary_string[:half_len - 1]
        last_half_len = half_len - 2 + is_odd
        if last_half_len > 0:
            last_half = arbitrary_string[last_half_len * -1:]
        else:
            last_half = ''
        return f"{first_half}✂✂✂{last_half}"


TValue = TypeVar('TValue', bound=Hashable)


def duplicates(elements: Sequence[TValue]) -> Tuple[TValue, ...]:
    return tuple(
        value for
        value, count in
        collections.Counter(elements).items()
        if count > 1
    )


TCo = TypeVar('TCo')
TContra = TypeVar('TContra')


class aclosing:
    def __init__(self, aiter: AsyncGenerator[TCo, TContra]) -> None:
        self._aiter = aiter

    async def __aenter__(self) -> AsyncGenerator[TCo, TContra]:
        return self._aiter

    async def __aexit__(self, *args: Any) -> None:
        await self._aiter.aclose()
