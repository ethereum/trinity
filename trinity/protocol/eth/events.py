from dataclasses import (
    dataclass,
)
from typing import (
    Sequence,
    Type,
)

from eth.abc import (
    BlockHeaderAPI,
    SignedTransactionAPI,
)

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

from .commands import (
    BlockBodiesV65,
    BlockHeadersV65,
    GetBlockBodiesV65,
    GetBlockHeadersV65,
    GetNodeDataV65,
    GetReceiptsV65,
    NewBlock,
    NewBlockHashes,
    NodeDataV65,
    ReceiptsV65,
    Transactions,
    NewPooledTransactionHashes,
    GetPooledTransactionsV65,
    PooledTransactionsV65,
)


# Events flowing from PeerPool to Proxy

class GetBlockHeadersEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``GetBlockHeaders`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: GetBlockHeadersV65


class GetBlockBodiesEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``GetBlockBodies`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: GetBlockBodiesV65


class GetReceiptsEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``GetReceipts`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: GetReceiptsV65


class GetNodeDataEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``GetNodeData`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: GetNodeDataV65


class TransactionsEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``Transactions`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: Transactions


class NewBlockEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``NewBlock`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: NewBlock


class NewBlockHashesEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``NewBlockHashes`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: NewBlockHashes


class NewPooledTransactionHashesEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``NewPooledTransactionHashes`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: NewPooledTransactionHashes


class GetPooledTransactionsEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``GetPooledTransactions`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: GetPooledTransactionsV65


class PooledTransactionsEvent(PeerPoolMessageEvent):
    """
    Event to carry a ``PooledTransactions`` command from the peer pool to any process that
    subscribes the event through the event bus.
    """
    command: PooledTransactionsV65


# Events flowing from Proxy to PeerPool


@dataclass
class SendBlockHeadersEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHPeer.sub_proto.send_block_headers`` call from a proxy peer to the actual
    peer that sits in the peer pool.
    """
    session: SessionAPI
    command: BlockHeadersV65


@dataclass
class SendBlockBodiesEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHPeer.sub_proto.send_block_bodies`` call from a proxy peer to the actual
    peer that sits in the peer pool.
    """
    session: SessionAPI
    command: BlockBodiesV65


@dataclass
class SendNewBlockHashesEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHPeer.sub_proto.send_new_block_hashes`` call from a proxy peer to the
    actual peer that sits in the peer pool.
    """
    session: SessionAPI
    command: NewBlockHashes


@dataclass
class SendNewBlockEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHProxyPeer.send_new_block`` call to the actual peer that sits in the peer
    pool.
    """
    session: SessionAPI
    command: NewBlock


@dataclass
class SendNodeDataEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHPeer.sub_proto.send_node_data`` call from a proxy peer to the actual
    peer that sits in the peer pool.
    """
    session: SessionAPI
    command: NodeDataV65


@dataclass
class SendReceiptsEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHPeer.sub_proto.send_receipts`` call from a proxy peer to the actual
    peer that sits in the peer pool.
    """
    session: SessionAPI
    command: ReceiptsV65


@dataclass
class SendTransactionsEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHPeer.sub_proto.send_transactions`` call from a proxy peer to the actual
    peer that sits in the peer pool.
    """
    session: SessionAPI
    command: Transactions


@dataclass
class SendPooledTransactionsEvent(PeerPoolMessageEvent):
    """
    Event to proxy a ``ETHPeer.sub_proto.send_pooled_transactions`` call from a proxy peer to
    the actual peer that sits in the peer pool.
    """
    session: SessionAPI
    command: PooledTransactionsV65

# EXCHANGE HANDLER REQUEST / RESPONSE PAIRS


@dataclass
class GetBlockHeadersResponse(BaseEvent):
    """
    The response class to answer a ``GetBlockHeadersRequest``.
    """

    headers: Sequence[BlockHeaderAPI]
    error: Exception = None


@dataclass
class GetBlockHeadersRequest(BaseRequestResponseEvent[GetBlockHeadersResponse]):
    """
    A request class to delegate a :class:`trinity.protocol.proxy.eth.events.GetBlockHeaders` command
    from any process to another process that can perform the actual
    :class:`trinity.protocol.proxy.eth.events.GetBlockHeaders` command, wrap the result and send it
    back to the origin process via a
    :class:`trinity.protocol.proxy.eth.events.GetBlockHeadersResponse`.

    This is a low-level event class used by :class:`trinity.protocol.proxy.ProxyETHAPI` to allow
    any Trinity process to interact with peers through the event bus.
    """

    session: SessionAPI
    block_number_or_hash: BlockIdentifier
    max_headers: int
    skip: int
    reverse: bool
    timeout: float

    @staticmethod
    def expected_response_type() -> Type[GetBlockHeadersResponse]:
        return GetBlockHeadersResponse


@dataclass
class GetBlockBodiesResponse(BaseEvent):
    """
    The response class to answer a :class:`trinity.protocol.proxy.eth.events.GetBlockBodiesRequest`
    """
    bundles: BlockBodyBundles
    error: Exception = None


@dataclass
class GetBlockBodiesRequest(BaseRequestResponseEvent[GetBlockBodiesResponse]):
    """
    A request class to delegate a :class:`trinity.protocol.proxy.eth.events.GetBlockBodies` command
    from any process to another process that can perform the actual
    :class:`trinity.protocol.proxy.eth.events.GetBlockBodies` command, wrap the result and send it
    back to the origin process via a
    :class:`trinity.protocol.proxy.eth.events.GetBlockBodiesResponse`.

    This is a low-level event class used by :class:`trinity.protocol.proxy.ProxyETHAPI` to allow
    any Trinity process to interact with peers through the event bus.
    """

    session: SessionAPI
    headers: Sequence[BlockHeaderAPI]
    timeout: float

    @staticmethod
    def expected_response_type() -> Type[GetBlockBodiesResponse]:
        return GetBlockBodiesResponse


@dataclass
class GetNodeDataResponse(BaseEvent):
    """
    The response class to answer a :class:`trinity.protocol.proxy.eth.events.GetNodeDataRequest`.
    """
    bundles: NodeDataBundles
    error: Exception = None


@dataclass
class GetNodeDataRequest(BaseRequestResponseEvent[GetNodeDataResponse]):
    """
    A request class to delegate a :class:`trinity.protocol.proxy.eth.events.GetNodeData` command
    from any process to another process that can perform the actual
    :class:`trinity.protocol.proxy.eth.events.GetNodeData` command, wrap the result and send it back
    to the origin process via a :class:`trinity.protocol.proxy.eth.events.GetNodeDataResponse`.

    This is a low-level event class used by :class:`trinity.protocol.proxy.ProxyETHAPI` to allow
    any Trinity process to interact with peers through the event bus.
    """

    session: SessionAPI
    node_hashes: Sequence[Hash32]
    timeout: float

    @staticmethod
    def expected_response_type() -> Type[GetNodeDataResponse]:
        return GetNodeDataResponse


@dataclass
class GetReceiptsResponse(BaseEvent):
    """
    The response class to answer a :class:`trinity.protocol.proxy.eth.events.GetReceiptsRequest`.
    """
    bundles: ReceiptsBundles
    error: Exception = None


@dataclass
class GetReceiptsRequest(BaseRequestResponseEvent[GetReceiptsResponse]):
    """
    A request class to delegate a :class:`trinity.protocol.proxy.eth.events.GetReceipts` command
    from any process to another process that can perform the actual
    :class:`trinity.protocol.proxy.eth.events.GetReceipts` command, wrap the result and send it back
    to the origin process via a :class:`trinity.protocol.proxy.eth.events.GetNodeDataResponse`.

    This is a low-level event class used by :class:`trinity.protocol.proxy.ProxyETHAPI` to allow
    any Trinity process to interact with peers through the event bus.
    """

    session: SessionAPI
    headers: Sequence[BlockHeaderAPI]
    timeout: float

    @staticmethod
    def expected_response_type() -> Type[GetReceiptsResponse]:
        return GetReceiptsResponse


@dataclass
class GetPooledTransactionsResponse(BaseEvent):
    """
    The response class to answer a ``GetPooledTransactionsRequest``.
    """
    transactions: Sequence[SignedTransactionAPI]
    error: Exception = None


@dataclass
class GetPooledTransactionsRequest(BaseRequestResponseEvent[GetPooledTransactionsResponse]):
    """
    A request class to delegate a ``GetPooledTransactions`` command from any process to another
    process that can perform the actual ``GetPooledTransactions`` command, wrap the result and
    send it back to the origin process via a ``GetPooledTransactionsResponse``.

    This is a low-level event class used by :class:`trinity.protocol.proxy.ProxyETHAPI` to allow
    any Trinity process to interact with peers through the event bus.
    """

    session: SessionAPI
    transaction_hashes: Sequence[Hash32]
    timeout: float

    @staticmethod
    def expected_response_type() -> Type[GetPooledTransactionsResponse]:
        return GetPooledTransactionsResponse
