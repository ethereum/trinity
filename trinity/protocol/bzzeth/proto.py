import logging

from p2p.protocol import (
    Protocol,
)

from .commands import (
    Handshake,
    NewBlockHeaders,
    GetBlockHeaders,
    BlockHeaders,
)


class BZZETHProtocol(Protocol):
    name = 'bzzeth'
    version = 1
    _commands = (
        Handshake,
        NewBlockHeaders,
        GetBlockHeaders,
        BlockHeaders,
    )
    cmd_length = 4
    logger = logging.getLogger("trinity.protocol.bzzeth.proto.BZZETHProtocol")

    def send_handshake(self, serves_headers: bool) -> None:
        cmd = Handshake(self.cmd_id_offset, self.snappy_support)
        self.logger.debug2("Sending BZZETH Handshake message")
        self.transport.send(*cmd.encode((int(serves_headers),)))
