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
    NewBlockWitnessHashes,
    Status,
)

if TYPE_CHECKING:
    from .peer import FirehosePeer  # noqa: F401


class FirehoseHandshakeParams(NamedTuple):
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
            'genesis_hash': handshake_params.genesis_hash,
            'network_id': handshake_params.network_id,
            'protocol_version': handshake_params.version,
        }
        cmd = Status(self.cmd_id_offset)
        self.logger.warning("Sending Firehose/Status msg: %s", resp)
        self.transport.send(*cmd.encode(resp))

    #
    # NewBlockWitnessHashes
    #
    def send_new_block_witness_hashes(self, node_hashes: Sequence[Hash32]) -> None:
        cmd = NewBlockWitnessHashes(self.cmd_id_offset)
        header, body = cmd.encode(node_hashes)
        self.transport.send(header, body)
