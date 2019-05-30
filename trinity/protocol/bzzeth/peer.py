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
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)

from .commands import (
    Handshake,
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
