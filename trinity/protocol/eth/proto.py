from abc import (
    abstractmethod,
)
from typing import (
    List,
    Tuple,
    TYPE_CHECKING,
    Union,
)
import typing_extensions

from eth_typing import (
    Hash32,
    BlockNumber,
)

from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt
from eth.rlp.transactions import BaseTransactionFields
from lahja import (
    Endpoint,
    BroadcastConfig,
)
from p2p.peer import (
    IdentifiablePeer,
)
from p2p.protocol import (
    Protocol,
)

from trinity.protocol.common.peer import ChainInfo
from trinity.rlp.block_body import BlockBody

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
    Status,
    Transactions,
)
from .events import (
    SendBlockBodiesEvent,
    SendBlockHeadersEvent,
    SendNodeDataEvent,
    SendReceiptsEvent,
)

from trinity._utils.logging import HasExtendedDebugLogger

if TYPE_CHECKING:
    from .peer import ETHPeer  # noqa: F401


class ETHProtocolLike(typing_extensions.Protocol):
    """
    Define the ETH protocol used to communicate between ETH peers.
    """

    @abstractmethod
    def send_get_node_data(self, node_hashes: Tuple[Hash32, ...]) -> None:
        pass

    @abstractmethod
    def send_node_data(self, nodes: Tuple[bytes, ...]) -> None:
        pass

    @abstractmethod
    def send_get_block_headers(
            self,
            block_number_or_hash: Union[BlockNumber, Hash32],
            max_headers: int,
            skip: int,
            reverse: bool) -> None:
        pass

    @abstractmethod
    def send_block_headers(self, headers: Tuple[BlockHeader, ...]) -> None:
        pass

    @abstractmethod
    def send_get_block_bodies(self, block_hashes: Tuple[Hash32, ...]) -> None:
        pass

    @abstractmethod
    def send_block_bodies(self, blocks: List[BlockBody]) -> None:
        pass

    @abstractmethod
    def send_get_receipts(self, block_hashes: Tuple[Hash32, ...]) -> None:
        pass

    @abstractmethod
    def send_receipts(self, receipts: List[List[Receipt]]) -> None:
        pass

    @abstractmethod
    def send_transactions(self, transactions: List[BaseTransactionFields]) -> None:
        pass


# HasExtendedDebugLogger must come before Protocol so there's self.logger.debug2()
class ETHProtocol(HasExtendedDebugLogger, Protocol):
    name = 'eth'
    version = 63
    _commands = [
        Status, NewBlockHashes, Transactions, GetBlockHeaders, BlockHeaders,
        GetBlockBodies, BlockBodies, NewBlock, GetNodeData, NodeData,
        GetReceipts, Receipts]
    cmd_length = 17

    peer: 'ETHPeer'

    def send_handshake(self, chain_info: ChainInfo) -> None:
        resp = {
            'protocol_version': self.version,
            'network_id': self.peer.network_id,
            'td': chain_info.total_difficulty,
            'best_hash': chain_info.block_hash,
            'genesis_hash': chain_info.genesis_hash,
        }
        cmd = Status(self.cmd_id_offset, self.snappy_support)
        self.logger.debug2("Sending ETH/Status msg: %s", resp)
        self.send(*cmd.encode(resp))

    #
    # Node Data
    #
    def send_get_node_data(self, node_hashes: Tuple[Hash32, ...]) -> None:
        cmd = GetNodeData(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(node_hashes)
        self.send(header, body)

    def send_node_data(self, nodes: Tuple[bytes, ...]) -> None:
        cmd = NodeData(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(nodes)
        self.send(header, body)

    #
    # Block Headers
    #
    def send_get_block_headers(
            self,
            block_number_or_hash: Union[BlockNumber, Hash32],
            max_headers: int,
            skip: int,
            reverse: bool) -> None:
        """Send a GetBlockHeaders msg to the remote.

        This requests that the remote send us up to max_headers, starting from
        block_number_or_hash if reverse is False or ending at block_number_or_hash if reverse is
        True.
        """
        cmd = GetBlockHeaders(self.cmd_id_offset, self.snappy_support)
        data = {
            'block_number_or_hash': block_number_or_hash,
            'max_headers': max_headers,
            'skip': skip,
            'reverse': reverse
        }
        header, body = cmd.encode(data)
        self.send(header, body)

    def send_block_headers(self, headers: Tuple[BlockHeader, ...]) -> None:
        cmd = BlockHeaders(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(headers)
        self.send(header, body)

    #
    # Block Bodies
    #
    def send_get_block_bodies(self, block_hashes: Tuple[Hash32, ...]) -> None:
        cmd = GetBlockBodies(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(block_hashes)
        self.send(header, body)

    def send_block_bodies(self, blocks: List[BlockBody]) -> None:
        cmd = BlockBodies(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(blocks)
        self.send(header, body)

    #
    # Receipts
    #
    def send_get_receipts(self, block_hashes: Tuple[Hash32, ...]) -> None:
        cmd = GetReceipts(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(block_hashes)
        self.send(header, body)

    def send_receipts(self, receipts: List[List[Receipt]]) -> None:
        cmd = Receipts(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(receipts)
        self.send(header, body)

    #
    # Transactions
    #
    def send_transactions(self, transactions: List[BaseTransactionFields]) -> None:
        cmd = Transactions(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(transactions)
        self.send(header, body)


class ProxyETHProtocol:
    """
    An ``ETHProtocol`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 event_bus: Endpoint,
                 broadcast_config: BroadcastConfig):
        self._dto_peer = dto_peer
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config

    #
    # Node Data
    #
    def send_get_node_data(self, node_hashes: Tuple[Hash32, ...]) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_node_data(self, nodes: Tuple[bytes, ...]) -> None:
        self._event_bus.broadcast(
            SendNodeDataEvent(self._dto_peer, nodes),
            self._broadcast_config,
        )

    #
    # Block Headers
    #
    def send_get_block_headers(
            self,
            block_number_or_hash: Union[BlockNumber, Hash32],
            max_headers: int,
            skip: int,
            reverse: bool) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_block_headers(self, headers: Tuple[BlockHeader, ...]) -> None:
        self._event_bus.broadcast(
            SendBlockHeadersEvent(self._dto_peer, headers),
            self._broadcast_config,
        )

    #
    # Block Bodies
    #
    def send_get_block_bodies(self, block_hashes: Tuple[Hash32, ...]) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_block_bodies(self, blocks: List[BlockBody]) -> None:
        self._event_bus.broadcast(
            SendBlockBodiesEvent(self._dto_peer, blocks),
            self._broadcast_config,
        )

    #
    # Receipts
    #
    def send_get_receipts(self, block_hashes: Tuple[Hash32, ...]) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_receipts(self, receipts: List[List[Receipt]]) -> None:
        self._event_bus.broadcast(
            SendReceiptsEvent(self._dto_peer, receipts),
            self._broadcast_config,
        )

    #
    # Transactions
    #
    def send_transactions(self, transactions: List[BaseTransactionFields]) -> None:
        raise NotImplementedError("Not yet implemented")
