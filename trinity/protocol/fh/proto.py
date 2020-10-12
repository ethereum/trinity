from p2p.protocol import BaseProtocol

from .commands import NewBlockWitnessHashes, Status


class FirehoseProtocol(BaseProtocol):
    name = 'fh'
    version = 1
    commands = (
        Status,
        NewBlockWitnessHashes,
    )
    command_length = 24  # twelve more identified possibilities for new sync, plus wiggle room
