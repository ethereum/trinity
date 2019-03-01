from typing import (
    List,
    Tuple,
    Type,
)

from eth.rlp.blocks import BaseBlock
from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt
from eth_typing import (
    BlockIdentifier,
    Hash32,
)
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)
from p2p.peer import (
    IdentifiablePeer,
)

from trinity.protocol.common.types import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)


# RAW PROTOCOL EVENTS

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


# EXCHANGE HANDLER REQUEST / RESPONSE PAIRS

class GetBlockHeadersResponse(BaseEvent):

    def __init__(self,
                 headers: Tuple[BlockHeader, ...]) -> None:
        self.headers = headers


class GetBlockHeadersRequest(BaseRequestResponseEvent[GetBlockHeadersResponse]):

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 block_number_or_hash: BlockIdentifier,
                 max_headers: int,
                 skip: int,
                 reverse: bool,
                 timeout: float) -> None:
        self.dto_peer = dto_peer
        self.block_number_or_hash = block_number_or_hash
        self.max_headers = max_headers
        self.skip = skip
        self.reverse = reverse
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetBlockHeadersResponse]:
        return GetBlockHeadersResponse


class GetBlockBodiesResponse(BaseEvent):

    def __init__(self,
                 bundles: BlockBodyBundles) -> None:
        self.bundles = bundles


class GetBlockBodiesRequest(BaseRequestResponseEvent[GetBlockBodiesResponse]):

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 headers: Tuple[BlockHeader, ...],
                 timeout: float) -> None:
        self.dto_peer = dto_peer
        self.headers = headers
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetBlockBodiesResponse]:
        return GetBlockBodiesResponse


class GetNodeDataResponse(BaseEvent):

    def __init__(self,
                 bundles: NodeDataBundles) -> None:
        self.bundles = bundles


class GetNodeDataRequest(BaseRequestResponseEvent[GetNodeDataResponse]):

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 node_hashes: Tuple[Hash32, ...],
                 timeout: float) -> None:
        self.dto_peer = dto_peer
        self.node_hashes = node_hashes
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetNodeDataResponse]:
        return GetNodeDataResponse


class GetReceiptsResponse(BaseEvent):

    def __init__(self,
                 bundles: ReceiptsBundles) -> None:
        self.bundles = bundles


class GetReceiptsRequest(BaseRequestResponseEvent[GetReceiptsResponse]):

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 headers: Tuple[BlockHeader, ...],
                 timeout: float) -> None:
        self.dto_peer = dto_peer
        self.headers = headers
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetReceiptsResponse]:
        return GetReceiptsResponse

# Other PeerPool events


class GetHighestTDPeerResponse(BaseEvent):

    def __init__(self,
                 dto_peer: IdentifiablePeer) -> None:
        self.dto_peer = dto_peer


class GetHighestTDPeerRequest(BaseRequestResponseEvent[GetHighestTDPeerResponse]):

    def __init__(self,
                 timeout: float) -> None:
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetHighestTDPeerResponse]:
        return GetHighestTDPeerResponse


class GetConnectedPeersResponse(BaseEvent):

    def __init__(self,
                 dto_peers: Tuple[IdentifiablePeer, ...]) -> None:
        self.dto_peers = dto_peers


class GetConnectedPeersRequest(BaseRequestResponseEvent[GetConnectedPeersResponse]):

    def __init__(self, min_td: int = 0) -> None:
        self.min_td = min_td

    @staticmethod
    def expected_response_type() -> Type[GetConnectedPeersResponse]:
        return GetConnectedPeersResponse
