from p2p.abc import MultiplexerAPI
from p2p.handshake import Handshaker
from p2p.receipt import HandshakeReceipt

from .proto import WitnessProtocol


class WitnessHandshakeReceipt(HandshakeReceipt):
    pass


class WitnessHandshaker(Handshaker[WitnessProtocol]):
    protocol_class = WitnessProtocol

    async def do_handshake(self,
                           multiplexer: MultiplexerAPI,
                           protocol: WitnessProtocol) -> WitnessHandshakeReceipt:
        self.logger.debug("Performing %s handshake with %s", protocol, multiplexer.remote)
        return WitnessHandshakeReceipt(protocol)
