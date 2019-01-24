from typing import (
    List,
    Tuple,
)

from eth.rlp.blocks import BaseBlock
from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt
from lahja import (
    BaseEvent,
)
from p2p.peer import (
    IdentifiablePeer,
)


class SendBlockHeadersEvent(BaseEvent):
    """
    An event to carry a *send block header* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, dto_peer: IdentifiablePeer, headers: Tuple[BlockHeader, ...]) -> None:
        self.dto_peer = dto_peer
        self.headers = headers


class SendBlockBodiesEvent(BaseEvent):
    """
    An event to carry a *send block bodies* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, dto_peer: IdentifiablePeer, blocks: List[BaseBlock]) -> None:
        self.dto_peer = dto_peer
        self.blocks = blocks


class SendNodeDataEvent(BaseEvent):
    """
    An event to carry a *send node data* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, dto_peer: IdentifiablePeer, nodes: Tuple[bytes, ...]) -> None:
        self.dto_peer = dto_peer
        self.nodes = nodes


class SendReceiptsEvent(BaseEvent):
    """
    An event to carry a *send receipts* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, dto_peer: IdentifiablePeer, receipts: List[List[Receipt]]) -> None:
        self.dto_peer = dto_peer
        self.receipts = receipts
