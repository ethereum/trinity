from abc import abstractmethod
from typing import Any, Sequence, Tuple, Union, Generic, Type, TypeVar, Optional

from cached_property import cached_property

from eth_typing import BlockNumber, Hash32

from eth.abc import (
    BlockAPI,
    BlockHeaderAPI,
    ReceiptAPI,
    SignedTransactionAPI,
)

from p2p.abc import ConnectionAPI, ProtocolAPI
from p2p.exchange import ExchangeAPI, ExchangeLogic
from p2p.logic import Application, CommandHandler
from p2p.qualifiers import HasProtocol

from trinity._utils.requests import gen_request_id
from trinity.protocol.common.abc import HeadInfoAPI
from trinity.protocol.common.payloads import (
    BlockHeadersQuery,
    BlockHeadersQueryPayload,
    BlockHeadersResultPayload,
    BlocksResultPayload,
    BytesTupleResultPayload,
    Hash32TupleQueryPayload,
    ReceiptBundleResultPayload,
)
from trinity.protocol.eth.commands import (
    BlockBodiesV65,
    BlockHeadersV65,
    GetBlockBodiesV65,
    GetBlockHeadersV65,
    GetNodeDataV65,
    GetReceiptsV65,
    GetBlockHeadersV66,
    NewBlock,
    NewBlockHashes,
    NodeDataV65,
    ReceiptsV65,
    StatusV63,
    Transactions,
    Status,
    GetPooledTransactionsV65,
    GetBlockBodiesV66, GetNodeDataV66, GetReceiptsV66, GetPooledTransactionsV66, NodeDataV66,
    BlockHeadersV66, BlockBodiesV66, ReceiptsV66)
from trinity.rlp.block_body import BlockBody

from .exchanges import (
    GetBlockBodiesV65Exchange,
    GetBlockBodiesV66Exchange,
    GetBlockHeadersV65Exchange,
    GetBlockHeadersV66Exchange,
    GetNodeDataV65Exchange,
    GetNodeDataV66Exchange,
    GetReceiptsV65Exchange,
    GetReceiptsV66Exchange,
    GetPooledTransactionsV65Exchange,
    GetPooledTransactionsV66Exchange,
)
from .handshaker import ETHV63HandshakeReceipt, ETHHandshakeReceipt, BaseETHHandshakeReceipt
from .payloads import (
    BlockFields,
    NewBlockHash,
    NewBlockPayload,
    StatusV63Payload,
    StatusPayload,
)
from .proto import ETHProtocolV63, ETHProtocolV64, ETHProtocolV65, ETHProtocolV66

THandshakeReceipt = TypeVar("THandshakeReceipt", bound=BaseETHHandshakeReceipt[Any])


class BaseHeadInfoTracker(CommandHandler[NewBlock], HeadInfoAPI, Generic[THandshakeReceipt]):
    command_type = NewBlock

    _head_td: int = None
    _head_hash: Hash32 = None
    _head_number: BlockNumber = None

    _receipt_type: Type[THandshakeReceipt]

    async def handle(self, connection: ConnectionAPI, cmd: NewBlock) -> None:
        header = cmd.payload.block.header
        actual_td = cmd.payload.total_difficulty - header.difficulty

        if actual_td > self.head_td:
            self._head_hash = header.parent_hash
            self._head_td = actual_td
            self._head_number = BlockNumber(header.block_number - 1)

    #
    # HeadInfoAPI
    #
    @cached_property
    def _eth_receipt(self) -> THandshakeReceipt:
        return self.connection.get_receipt_by_type(self._receipt_type)

    @property
    def head_td(self) -> int:
        if self._head_td is None:
            self._head_td = self._eth_receipt.total_difficulty
        return self._head_td

    @property
    def head_hash(self) -> Hash32:
        if self._head_hash is None:
            self._head_hash = self._eth_receipt.head_hash
        return self._head_hash

    @property
    def head_number(self) -> BlockNumber:
        if self._head_number is None:
            # TODO: fetch on demand using request/response API
            raise AttributeError("Head block number is not currently known")
        return self._head_number


class ETHV63HeadInfoTracker(BaseHeadInfoTracker[ETHV63HandshakeReceipt]):

    _receipt_type = ETHV63HandshakeReceipt


class ETHHeadInfoTracker(BaseHeadInfoTracker[ETHHandshakeReceipt]):

    _receipt_type = ETHHandshakeReceipt


class BaseETHAPI(Application):
    name = 'eth'
    head_info_tracker_cls = BaseHeadInfoTracker[THandshakeReceipt]

    get_block_bodies: Union[GetBlockBodiesV65Exchange, GetBlockBodiesV66Exchange]
    get_block_headers: Union[GetBlockHeadersV65Exchange, GetBlockHeadersV66Exchange]
    get_node_data: Union[GetNodeDataV65Exchange, GetNodeDataV66Exchange]
    get_receipts: Union[GetReceiptsV65Exchange, GetReceiptsV66Exchange]
    get_pooled_transactions: Union[
        GetPooledTransactionsV65Exchange, GetPooledTransactionsV66Exchange
    ]

    def __init__(self) -> None:
        self.head_info = self.head_info_tracker_cls()
        self.add_child_behavior(self.head_info.as_behavior())

        # Request/Response API
        self.get_block_bodies = GetBlockBodiesV65Exchange()
        self.get_block_headers = GetBlockHeadersV65Exchange()
        self.get_node_data = GetNodeDataV65Exchange()
        self.get_receipts = GetReceiptsV65Exchange()

        self.add_child_behavior(ExchangeLogic(self.get_block_bodies).as_behavior())
        self.add_child_behavior(ExchangeLogic(self.get_block_headers).as_behavior())
        self.add_child_behavior(ExchangeLogic(self.get_node_data).as_behavior())
        self.add_child_behavior(ExchangeLogic(self.get_receipts).as_behavior())

    @property
    @abstractmethod
    def protocol(self) -> ProtocolAPI:
        ...

    @property
    @abstractmethod
    def receipt(self) -> BaseETHHandshakeReceipt[Any]:
        ...

    @cached_property
    def exchanges(self) -> Tuple[ExchangeAPI[Any, Any, Any], ...]:
        return (
            self.get_block_bodies,
            self.get_block_headers,
            self.get_node_data,
            self.get_receipts,
        )

    def get_extra_stats(self) -> Tuple[str, ...]:
        return tuple(
            f"{exchange.get_response_cmd_type()}: {exchange.tracker.get_stats()}"
            for exchange in self.exchanges
        )

    @cached_property
    def network_id(self) -> int:
        return self.receipt.network_id

    @cached_property
    def genesis_hash(self) -> Hash32:
        return self.receipt.genesis_hash

    def send_get_node_data(self, node_hashes: Sequence[Hash32]) -> Optional[int]:
        return self.protocol.send(GetNodeDataV65(tuple(node_hashes)))

    def send_node_data(self, nodes: Sequence[bytes], request_id: int = None) -> None:
        if request_id is not None:
            raise ValueError("request_id not expected for eth/63 API")

        self.protocol.send(NodeDataV65(tuple(nodes)))

    def send_get_block_headers(
            self,
            block_number_or_hash: Union[BlockNumber, Hash32],
            max_headers: int,
            skip: int,
            reverse: bool) -> Optional[int]:
        payload = BlockHeadersQuery(
            block_number_or_hash=block_number_or_hash,
            max_headers=max_headers,
            skip=skip,
            reverse=reverse
        )
        return self.protocol.send(GetBlockHeadersV65(payload))

    def send_block_headers(self, headers: Sequence[BlockHeaderAPI]) -> None:
        self.protocol.send(BlockHeadersV65(tuple(headers)))

    def send_get_block_bodies(self, block_hashes: Sequence[Hash32]) -> Optional[int]:
        return self.protocol.send(GetBlockBodiesV65(tuple(block_hashes)))

    def send_block_bodies(self, blocks: Sequence[BlockAPI], request_id: int = None) -> None:
        if request_id is not None:
            raise ValueError("request_id not expected for eth/63 API")

        block_bodies = tuple(
            BlockBody(block.transactions, block.uncles)
            for block in blocks
        )
        self.protocol.send(BlockBodiesV65(block_bodies))

    def send_get_receipts(self, block_hashes: Sequence[Hash32]) -> Optional[int]:
        return self.protocol.send(GetReceiptsV65(tuple(block_hashes)))

    def send_receipts(self,
                      receipts: Sequence[Sequence[ReceiptAPI]],
                      request_id: Optional[int] = None) -> None:
        if request_id is not None:
            raise ValueError("request_id not expected for eth/63 API")

        self.protocol.send(ReceiptsV65(tuple(map(tuple, receipts))))

    def send_transactions(self, transactions: Sequence[SignedTransactionAPI]) -> None:
        self.protocol.send(Transactions(tuple(transactions)))

    def send_new_block_hashes(self, *new_block_hashes: NewBlockHash) -> None:
        self.protocol.send(NewBlockHashes(new_block_hashes))

    def send_new_block(self,
                       block: BlockAPI,
                       total_difficulty: int) -> None:
        block_fields = BlockFields(block.header, block.transactions, block.uncles)
        payload = NewBlockPayload(block_fields, total_difficulty)
        self.protocol.send(NewBlock(payload))


class ETHV63API(BaseETHAPI):
    qualifier = HasProtocol(ETHProtocolV63)
    head_info_tracker_cls = ETHV63HeadInfoTracker

    @cached_property
    def protocol(self) -> ProtocolAPI:
        return self.connection.get_protocol_by_type(ETHProtocolV63)

    @cached_property
    def receipt(self) -> ETHV63HandshakeReceipt:
        return self.connection.get_receipt_by_type(ETHV63HandshakeReceipt)

    def send_status(self, payload: StatusV63Payload) -> None:
        self.protocol.send(StatusV63(payload))


class ETHV64API(BaseETHAPI):
    qualifier = HasProtocol(ETHProtocolV64)
    head_info_tracker_cls = ETHHeadInfoTracker

    @cached_property
    def protocol(self) -> ProtocolAPI:
        return self.connection.get_protocol_by_type(ETHProtocolV64)

    @cached_property
    def receipt(self) -> ETHHandshakeReceipt:
        return self.connection.get_receipt_by_type(ETHHandshakeReceipt)

    def send_status(self, payload: StatusPayload) -> None:
        self.protocol.send(Status(payload))


class ETHV65API(ETHV64API):
    qualifier = HasProtocol(ETHProtocolV65)

    @cached_property
    def protocol(self) -> ProtocolAPI:
        return self.connection.get_protocol_by_type(ETHProtocolV65)

    def __init__(self) -> None:
        super().__init__()

        # Request/Response API
        self.get_pooled_transactions = GetPooledTransactionsV65Exchange()
        self.add_child_behavior(ExchangeLogic(self.get_pooled_transactions).as_behavior())

    def send_get_pooled_transactions(self, transaction_hashes: Sequence[Hash32]) -> Optional[int]:
        return self.protocol.send(GetPooledTransactionsV65(tuple(transaction_hashes)))


class ETHV66API(ETHV65API):
    qualifier = HasProtocol(ETHProtocolV66)

    @cached_property
    def protocol(self) -> ProtocolAPI:
        return self.connection.get_protocol_by_type(ETHProtocolV66)

    def __init__(self) -> None:
        super().__init__()
        self.get_block_headers = GetBlockHeadersV66Exchange()
        self.get_block_bodies = GetBlockBodiesV66Exchange()
        self.get_node_data = GetNodeDataV66Exchange()
        self.get_receipts = GetReceiptsV66Exchange()
        self.get_pooled_transactions = GetPooledTransactionsV66Exchange()
        self.add_child_behavior(ExchangeLogic(self.get_block_headers).as_behavior())
        self.add_child_behavior(ExchangeLogic(self.get_block_bodies).as_behavior())
        self.add_child_behavior(ExchangeLogic(self.get_node_data).as_behavior())
        self.add_child_behavior(ExchangeLogic(self.get_receipts).as_behavior())
        self.add_child_behavior(ExchangeLogic(self.get_pooled_transactions).as_behavior())

    def send_get_block_headers(self,
                               block_number_or_hash: Union[BlockNumber, Hash32],
                               max_headers: int,
                               skip: int,
                               reverse: bool) -> int:
        query = BlockHeadersQuery(
            block_number_or_hash=block_number_or_hash,
            max_headers=max_headers,
            skip=skip,
            reverse=reverse,
        )
        payload = BlockHeadersQueryPayload(
            request_id=gen_request_id(),
            query=query,
        )
        self.protocol.send(GetBlockHeadersV66(payload))
        return payload.request_id

    def send_get_block_bodies(self, block_hashes: Sequence[Hash32]) -> Optional[int]:
        payload = Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=tuple(block_hashes)
        )
        self.protocol.send(GetBlockBodiesV66(payload))
        return payload.request_id

    def send_get_node_data(self, node_hashes: Sequence[Hash32]) -> Optional[int]:
        payload = Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=tuple(node_hashes)
        )
        self.protocol.send(GetNodeDataV66(payload))
        return payload.request_id

    def send_get_receipts(self, block_hashes: Sequence[Hash32]) -> Optional[int]:
        payload = Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=tuple(block_hashes)
        )
        self.protocol.send(GetReceiptsV66(payload))
        return payload.request_id

    def send_get_pooled_transactions(self, transaction_hashes: Sequence[Hash32]) -> Optional[int]:
        payload = Hash32TupleQueryPayload(
            request_id=gen_request_id(),
            query=tuple(transaction_hashes)
        )
        self.protocol.send(GetPooledTransactionsV66(payload))
        return payload.request_id

    def send_node_data(self, nodes: Sequence[bytes], request_id: int = None) -> None:
        payload = BytesTupleResultPayload(
            request_id=request_id,
            result=tuple(nodes)
        )
        self.protocol.send(NodeDataV66(payload))

    def send_block_headers(self, headers: Sequence[BlockHeaderAPI], request_id: int = None) -> None:
        payload = BlockHeadersResultPayload(
            request_id=request_id,
            result=tuple(headers)
        )
        self.protocol.send(BlockHeadersV66(payload))

    def send_block_bodies(self, blocks: Sequence[BlockAPI], request_id: int = None) -> None:
        block_bodies = tuple(
            BlockBody(block.transactions, block.uncles)
            for block in blocks
        )
        payload = BlocksResultPayload(
            request_id=request_id,
            result=block_bodies
        )
        self.protocol.send(BlockBodiesV66(payload))

    def send_receipts(self,
                      receipts: Sequence[Sequence[ReceiptAPI]],
                      request_id: int = None) -> None:
        payload = ReceiptBundleResultPayload(
            request_id=request_id,
            result=tuple(map(tuple, receipts))
        )
        self.protocol.send(ReceiptsV66(payload))


AnyETHAPI = Union[ETHV63API, ETHV64API, ETHV65API, ETHV66API]
