from mypy_extensions import (
    TypedDict,
)
from typing import NamedTuple

from eth_typing import (
    BlockNumber,
)


class SyncProgress(TypedDict):
    startingBlock: BlockNumber
    currentBlock: BlockNumber
    highestBlock: BlockNumber


class Response(NamedTuple):
    content_type: str
    body: str
