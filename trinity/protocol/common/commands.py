from abc import ABC, abstractmethod
from typing import Tuple

from eth.rlp.headers import BlockHeader

from p2p.protocol import (
    Command,
    _DecodedMsgType,
)


# `Command` must come before `ABC`, since `Command` is derived from `ABC`
class BaseBlockHeaders(Command, ABC):

    @abstractmethod
    def extract_headers(self, msg: _DecodedMsgType) -> Tuple[BlockHeader, ...]:
        raise NotImplementedError("Must be implemented by subclasses")
