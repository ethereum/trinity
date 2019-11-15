from typing import NamedTuple, TYPE_CHECKING

from eth_utils import get_extended_debug_logger

from eth_typing import Hash32
from p2p.protocol import BaseProtocol

from .commands import NewBlockWitnessHashes, Status

if TYPE_CHECKING:
    from .peer import FirehosePeer  # noqa: F401


class FirehoseHandshakeParams(NamedTuple):
    # TODO: replace genesis_hash with fork_id
    version: int
    network_id: int
    genesis_hash: Hash32


class FirehoseProtocol(BaseProtocol):
    name = 'fh'
    version = 1
    _commands = (
        Status,
        NewBlockWitnessHashes,
    )
    cmd_length = 24  # twelve more identified possibilities for new sync, plus wiggle room

    peer: 'FirehosePeer'

    logger = get_extended_debug_logger('trinity.protocol.fh.proto.FirehoseProtocol')
