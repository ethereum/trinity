from p2p.protocol import BaseProtocol
from p2p._utils import get_logger

from .commands import (
    BroadcastData,
    GetSum,
    Sum,
)


class ParagonProtocol(BaseProtocol):
    name = 'paragon'
    version = 1
    commands = (
        BroadcastData,
        GetSum, Sum,
    )
    command_length = 3
    logger = get_logger("p2p.tools.paragon.proto.ParagonProtocol")
