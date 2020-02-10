from typing import (
    TYPE_CHECKING,
    Union,
    Type,
)

from eth_utils import (
    get_extended_debug_logger,
)

from p2p.protocol import BaseProtocol
from .commands import (
    BlockBodies,
    BlockHeaders,
    GetBlockBodies,
    GetBlockHeaders,
    GetNodeData,
    GetReceipts,
    NewBlock,
    NewBlockHashes,
    NodeData,
    Receipts,
    Transactions,
    StatusV63,
    Status,
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
        GetBlockHeaders, BlockHeaders,
        GetBlockBodies, BlockBodies,
        NewBlock,
        GetNodeData, NodeData,
        GetReceipts, Receipts,
    )
    command_length = 17

    logger = get_extended_debug_logger('trinity.protocol.eth.proto.ETHProtocolV63')
    status_command_type = StatusV63


class ETHProtocol(BaseETHProtocol):
    version = 64
    commands = (
        Status,
        NewBlockHashes,
        Transactions,
        GetBlockHeaders, BlockHeaders,
        GetBlockBodies, BlockBodies,
        NewBlock,
        GetNodeData, NodeData,
        GetReceipts, Receipts,
    )
    command_length = 17

    logger = get_extended_debug_logger('trinity.protocol.eth.proto.ETHProtocol')
    status_command_type = Status
