from typing import (
    NamedTuple,
    Sequence,
    TYPE_CHECKING,
    Union,
)

from eth_typing import (
    Hash32,
    BlockNumber,
)

from eth_utils import (
    ValidationError,
    get_extended_debug_logger,
)

from lahja import EndpointAPI

from eth.abc import (
    BlockHeaderAPI,
    ReceiptAPI,
    SignedTransactionAPI,
)

from lahja import (
    BroadcastConfig,
)

from p2p.abc import SessionAPI
from p2p.protocol import Protocol

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
    SendTransactionsEvent,
)

if TYPE_CHECKING:
    from .peer import ETHPeer  # noqa: F401


class ETHHandshakeParams(NamedTuple):
    head_hash: Hash32
    genesis_hash: Hash32
    network_id: int
    total_difficulty: int
    version: int


class ETHProtocol(Protocol):
    name = 'eth'
    version = 63
    _commands = (
        Status,
        NewBlockHashes,
        Transactions,
        GetBlockHeaders, BlockHeaders,
        GetBlockBodies, BlockBodies,
        NewBlock,
        GetNodeData, NodeData,
        GetReceipts, Receipts,
    )
    cmd_length = 17

    peer: 'ETHPeer'

    logger = get_extended_debug_logger('trinity.protocol.eth.proto.ETHProtocol')

    def send_handshake(self, handshake_params: ETHHandshakeParams) -> None:
        if handshake_params.version != self.version:
            raise ValidationError(
                f"ETH protocol version mismatch: "
                f"params:{handshake_params.version} != proto:{self.version}"
            )
        resp = {
            'protocol_version': handshake_params.version,
            'network_id': handshake_params.network_id,
            'td': handshake_params.total_difficulty,
            'best_hash': handshake_params.head_hash,
            'genesis_hash': handshake_params.genesis_hash,
        }
        cmd = Status(self.cmd_id_offset, self.snappy_support)
        self.logger.debug2("Sending ETH/Status msg: %s", resp)
        self.transport.send(*cmd.encode(resp))

    #
    # Node Data
    #
    def send_get_node_data(self, node_hashes: Sequence[Hash32]) -> None:
        cmd = GetNodeData(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(node_hashes)
        self.transport.send(header, body)

    def send_node_data(self, nodes: Sequence[bytes]) -> None:
        cmd = NodeData(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(nodes)
        self.transport.send(header, body)

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
        self.transport.send(header, body)

    def send_block_headers(self, headers: Sequence[BlockHeaderAPI]) -> None:
        cmd = BlockHeaders(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(headers)
        self.transport.send(header, body)

    #
    # Block Bodies
    #
    def send_get_block_bodies(self, block_hashes: Sequence[Hash32]) -> None:
        cmd = GetBlockBodies(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(block_hashes)
        self.transport.send(header, body)

    def send_block_bodies(self, blocks: Sequence[BlockBody]) -> None:
        cmd = BlockBodies(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(blocks)
        self.transport.send(header, body)

    #
    # Receipts
    #
    def send_get_receipts(self, block_hashes: Sequence[Hash32]) -> None:
        cmd = GetReceipts(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(block_hashes)
        self.transport.send(header, body)

    def send_receipts(self, receipts: Sequence[Sequence[ReceiptAPI]]) -> None:
        cmd = Receipts(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(receipts)
        self.transport.send(header, body)

    #
    # Transactions
    #
    def send_transactions(self, transactions: Sequence[SignedTransactionAPI]) -> None:
        cmd = Transactions(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode(transactions)
        self.transport.send(header, body)

    #
    # NewBlock
    #
    def send_new_block(self,
                       block_header: BlockHeaderAPI,
                       transactions: Sequence[SignedTransactionAPI],
                       uncles: Sequence[BlockHeaderAPI],
                       total_difficulty: int) -> None:
        cmd = NewBlock(self.cmd_id_offset, self.snappy_support)
        # mypy doesn't recognize the following as a valid `Payload` type
        header, body = cmd.encode({  # type: ignore
            'block': (block_header, transactions, uncles),
            'total_difficulty': total_difficulty,
        })
        self.transport.send(header, body)


class ProxyETHProtocol:
    """
    An ``ETHProtocol`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """

    def __init__(self,
                 session: SessionAPI,
                 event_bus: EndpointAPI,
                 broadcast_config: BroadcastConfig):
        self.session = session
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config

    def send_handshake(self, handshake_params: ETHHandshakeParams) -> None:
        raise NotImplementedError("`ProxyETHProtocol.send_handshake` Not yet implemented")

    #
    # Node Data
    #
    def send_get_node_data(self, node_hashes: Sequence[Hash32]) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_node_data(self, nodes: Sequence[bytes]) -> None:
        self._event_bus.broadcast_nowait(
            SendNodeDataEvent(self.session, nodes),
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

    def send_block_headers(self, headers: Sequence[BlockHeaderAPI]) -> None:
        self._event_bus.broadcast_nowait(
            SendBlockHeadersEvent(self.session, headers),
            self._broadcast_config,
        )

    #
    # Block Bodies
    #
    def send_get_block_bodies(self, block_hashes: Sequence[Hash32]) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_block_bodies(self, blocks: Sequence[BlockBody]) -> None:
        self._event_bus.broadcast_nowait(
            SendBlockBodiesEvent(self.session, blocks),
            self._broadcast_config,
        )

    #
    # Receipts
    #
    def send_get_receipts(self, block_hashes: Sequence[Hash32]) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_receipts(self, receipts: Sequence[Sequence[ReceiptAPI]]) -> None:
        self._event_bus.broadcast_nowait(
            SendReceiptsEvent(self.session, receipts),
            self._broadcast_config,
        )

    #
    # Transactions
    #
    def send_transactions(self, transactions: Sequence[SignedTransactionAPI]) -> None:
        self._event_bus.broadcast_nowait(
            SendTransactionsEvent(self.session, transactions),
            self._broadcast_config,
        )
