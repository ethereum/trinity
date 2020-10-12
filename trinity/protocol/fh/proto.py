from typing import TYPE_CHECKING

from eth_utils import get_extended_debug_logger

from p2p.protocol import BaseProtocol

from .commands import NewBlockWitnessHashes, Status

if TYPE_CHECKING:
    from .peer import FirehosePeer  # noqa: F401


class FirehoseProtocol(BaseProtocol):
    name = 'fh'
    version = 1
    commands = (
        Status,
        NewBlockWitnessHashes,
    )
    command_length = 24  # twelve more identified possibilities for new sync, plus wiggle room

    logger = get_extended_debug_logger('trinity.protocol.fh.proto.FirehoseProtocol')
