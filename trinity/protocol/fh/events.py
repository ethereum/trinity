from dataclasses import (
    dataclass,
)
from typing import (
    Sequence,
    Tuple,
    Type,
)

from eth.abc import (
    BlockAPI,
    BlockHeaderAPI,
    ReceiptAPI,
    SignedTransactionAPI,
)
from eth.rlp.blocks import BaseBlock

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from eth_typing import (
    BlockIdentifier,
    Hash32,
)

from p2p.abc import SessionAPI

from trinity.protocol.common.events import (
    PeerPoolMessageEvent,
)
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)


@dataclass
class CreatedNewBlockWitnessHashes(BaseEvent):
    """
    Event to announce that some new block metadata was generated locally. Specifically,
    a list of trie node hashes that form the witness of all data read when executing a block.
    """
    block: BaseBlock
    witness_hashes: Tuple[Hash32, ...]
