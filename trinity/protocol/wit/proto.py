from p2p.protocol import BaseProtocol

from .commands import BlockWitnessHashes, GetBlockWitnessHashes


class WitnessProtocol(BaseProtocol):
    name = 'wit'
    version = 0
    commands = (
        GetBlockWitnessHashes,
        BlockWitnessHashes,
    )
    command_length = 24  # twelve more identified possibilities for new sync, plus wiggle room
