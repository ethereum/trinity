from typing import Type

from p2p.abc import (
    HandshakeCheckAPI,
    HandshakeReceiptAPI,
    ProtocolAPI,
)


class HandshakeReceipt(HandshakeReceiptAPI):
    """
    Data storage object for ephemeral data exchanged during protocol
    handshakes.
    """
    protocol: ProtocolAPI

    def __init__(self, protocol: ProtocolAPI) -> None:
        self.protocol = protocol

    def was_check_performed(self, check_type: Type[HandshakeCheckAPI]) -> bool:
        return False
