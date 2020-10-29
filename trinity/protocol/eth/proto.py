from typing import (
    TYPE_CHECKING,
    Union,
    Type,
)

from eth_utils import get_extended_debug_logger

from p2p.protocol import BaseProtocol

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
    StatusV63,
    Status,
    NewPooledTransactionHashes,
    GetPooledTransactionsV65,
    PooledTransactionsV65,
)

if TYPE_CHECKING:
    from .peer import ETHPeer  # noqa: F401


class BaseETHProtocol(BaseProtocol):
    name = 'eth'
    status_command_type: Union[Type[StatusV63], Type[Status]]


class ETHProtocolV63(BaseETHProtocol):
    version = 63
    commands = (
        StatusV63,
        NewBlockHashes,
        Transactions,
        GetBlockHeadersV65, BlockHeadersV65,
        GetBlockBodiesV65, BlockBodiesV65,
        NewBlock,
        GetNodeDataV65, NodeDataV65,
        GetReceiptsV65, ReceiptsV65,
    )
    command_length = 17

    logger = get_extended_debug_logger('trinity.protocol.eth.proto.ETHProtocolV63')
    status_command_type = StatusV63


class ETHProtocolV64(BaseETHProtocol):
    version = 64
    commands = (
        Status,
        NewBlockHashes,
        Transactions,
        GetBlockHeadersV65, BlockHeadersV65,
        GetBlockBodiesV65, BlockBodiesV65,
        NewBlock,
        GetNodeDataV65, NodeDataV65,
        GetReceiptsV65, ReceiptsV65,
    )
    command_length = 17

    logger = get_extended_debug_logger('trinity.protocol.eth.proto.ETHProtocolV64')
    status_command_type = Status


class ETHProtocolV65(BaseETHProtocol):
    version = 65
    commands = (
        Status,
        NewBlockHashes,
        Transactions,
        GetBlockHeadersV65, BlockHeadersV65,
        GetBlockBodiesV65, BlockBodiesV65,
        NewBlock,
        NewPooledTransactionHashes,
        GetPooledTransactionsV65, PooledTransactionsV65,
        GetNodeDataV65, NodeDataV65,
        GetReceiptsV65, ReceiptsV65,
    )
    command_length = 17

    logger = get_extended_debug_logger('trinity.protocol.eth.proto.ETHProtocolV65')

    status_command_type = Status
