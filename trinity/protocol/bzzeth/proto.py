import logging
from typing import Sequence

from eth_typing import Hash32

from eth.rlp.headers import BlockHeader

from p2p.protocol import (
    Protocol,
)

from trinity._utils.les import gen_request_id

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

    def send_new_headers(self, headers: Sequence[BlockHeader]) -> None:
        payload = tuple(
            (header.hash, header.block_number)
            for header in headers
        )
        cmd = NewBlockHeaders(self.cmd_id_offset, self.snappy_support)
        self.logger.debug2("Sending BZZETH NewBlockHeaders message")
        self.transport.send(*cmd.encode(payload))

    def send_get_block_headers(self,
                               hashes: Sequence[Hash32]) -> None:
        cmd = GetBlockHeaders(self.cmd_id_offset, self.snappy_support)
        self.logger.debug2("Sending BZZETH GetBlockHeaders message")
        self.transport.send(*cmd.encode({'request_id': gen_request_id(), 'hashes': hashes}))

    def send_block_headers(self,
                           headers: Sequence[BlockHeader],
                           request_id: int) -> None:
        cmd = BlockHeaders(self.cmd_id_offset, self.snappy_support)
        self.logger.debug2("Sending BZZETH BlockHeaders message")
        self.transport.send(*cmd.encode({'request_id': request_id, 'headers': headers}))
