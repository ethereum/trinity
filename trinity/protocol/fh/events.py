from dataclasses import dataclass
from typing import Tuple

from lahja import BaseEvent

from eth.rlp.blocks import BaseBlock
from eth_typing import Hash32


@dataclass
class CreatedNewBlockWitnessHashes(BaseEvent):
    """
    Event to announce that some new block metadata was generated locally. Specifically,
    a list of trie node hashes that form the witness of all data read when executing a block.
    """
    block: BaseBlock
    witness_hashes: Tuple[Hash32, ...]
