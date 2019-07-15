from cancel_token import CancelToken

from eth.db.header import HeaderDB

from p2p.exceptions import (
    HandshakeFailure,
)
from p2p.p2p_proto import (
    DisconnectReason,
)
from p2p.peer import (
    BasePeer,
    BasePeerContext,
    BasePeerFactory,
    PeerSubscriber,
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)
from p2p.service import BaseService

from trinity._utils.humanize import humanize_integer_sequence

from .commands import (
    Handshake,
    NewBlockHeaders,
    GetBlockHeaders,
    BlockHeaders,
)
from .proto import (
    BZZETHProtocol,
)


class BZZETHPeer(BasePeer):
    supported_sub_protocols = (BZZETHProtocol,)
    sub_proto: BZZETHProtocol = None
    serves_headers: bool = None

    async def send_sub_proto_handshake(self) -> None:
        self.sub_proto.send_handshake(serves_headers=True)
        self.logger.info('Sent BZZETH Handshake message')

    async def process_sub_proto_handshake(
            self, cmd: Command, msg: _DecodedMsgType) -> None:
        self.logger.info('Got BZZETH command: %s  message: %s', cmd, msg)
        if not isinstance(cmd, Handshake):
            await self.disconnect(DisconnectReason.subprotocol_error)
            raise HandshakeFailure(f"Expected a ETH Status msg, got {cmd}, disconnecting")
        self.serves_headers = msg['serves_headers']


class BZZETHContext(BasePeerContext):
    pass


class BZZETHPeerFactory(BasePeerFactory):
    peer_class = BZZETHPeer
    context: BZZETHContext


class BZZETHPeerPool(BasePeerPool):
    peer_factory_class = BZZETHPeerFactory
    context: BZZETHContext


class BZZETHHeaderServer(PeerSubscriber, BaseService):
    subscription_msg_types = {GetBlockHeaders, BlockHeaders, NewBlockHeaders}
    msg_queue_maxsize = 1000

    def __init__(self, token: CancelToken, headerdb: HeaderDB):
        self.headerdb = headerdb
        super().__init__(token=token)

    def register_peer(self, peer: BZZETHPeer):
        head = self.headerdb.get_canonical_head()
        self.logger.info('Announcing head %s to %s via `NewBlockHeaders` message', head, peer)
        peer.sub_proto.send_new_headers([head])

    async def _run(self) -> None:
        head = self.headerdb.get_canonical_head()
        self.logger.info('Starting BZZETH request/response server: HEAD %s', head.block_number)
        while self.is_operational:
            peer, cmd, msg = await self.msg_queue.get()

            if isinstance(cmd, GetBlockHeaders):
                self.logger.info('BZZETH: GetBlockHeaders')
                request_id = msg['request_id']
                hashes = msg['hashes']
                headers = tuple(
                    self.headerdb.get_block_header_by_hash(hash)
                    for hash in hashes
                )
                peer.sub_proto.send_block_headers(headers=headers, request_id=request_id)
            elif isinstance(cmd, BlockHeaders):
                headers = msg['headers']
                block_numbers = tuple(sorted(header.block_number for header in headers))
                self.logger.info(
                    'BZZETH: BlockHeaders.  request_id: %s, headers: %s',
                    msg['request_id'],
                    humanize_integer_sequence(block_numbers),
                )
            elif isinstance(cmd, NewBlockHeaders):
                block_numbers = tuple(sorted(block_number for _, block_number in msg))
                self.logger.info(
                    'BZZETH: NewBlockHeaders. %s',
                    humanize_integer_sequence(block_numbers),
                )
                self.logger.info('BZZETH: NewBlockHeaders')
            else:
                self.logger.error('Unexpected command: %s', cmd)
