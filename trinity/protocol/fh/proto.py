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
    from .peer import FirehosePeer  # noqa: F401


class FirehoseHandshakeParams(NamedTuple):
    head_hash: Hash32
    # TODO: replace genesis_hash with fork_id
    genesis_hash: Hash32
    network_id: int
    version: int


class FirehoseProtocol(Protocol):
    name = 'fh'
    version = 1
    _commands = (
        Status,
        NewBlockWitnessHashes,
    )
    cmd_length = 24  # twelve more identified possibilities for new sync, plus wiggle room

    peer: 'FirehosePeer'

    logger = get_extended_debug_logger('trinity.protocol.fh.proto.FirehoseProtocol')

    def send_handshake(self, handshake_params: FirehoseHandshakeParams) -> None:
        if handshake_params.version != self.version:
            raise ValidationError(
                f"Firehose protocol version mismatch: "
                f"params:{handshake_params.version} != proto:{self.version}"
            )
        resp = {
            'protocol_version': handshake_params.version,
            'network_id': handshake_params.network_id,
            'best_hash': handshake_params.head_hash,
            'genesis_hash': handshake_params.genesis_hash,
        }
        cmd = Status(self.cmd_id_offset)
        self.logger.debug2("Sending Firehose/Status msg: %s", resp)
        self.transport.send(*cmd.encode(resp))

    #
    # NewBlockWitnessHashes
    #
    def send_new_block_witness_hashes(self, node_hashes: Sequence[Hash32]) -> None:
        cmd = NewBlockWitnessHashes(self.cmd_id_offset)
        header, body = cmd.encode(node_hashes)
        self.transport.send(header, body)


class ProxyFirehoseProtocol:
    """
    An ``FirehoseProtocol`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """

    def __init__(self,
                 session: SessionAPI,
                 event_bus: EndpointAPI,
                 broadcast_config: BroadcastConfig):
        self.session = session
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config

    def send_handshake(self, handshake_params: FirehoseHandshakeParams) -> None:
        raise NotImplementedError("`ProxyFirehoseProtocol.send_handshake` Not yet implemented")

    #
    # Node Data
    #
    def send_new_block_witness_hashes(self, node_hashes: Sequence[Hash32]) -> None:
        raise NotImplementedError("Not yet implemented")
