import logging

from typing import (
    Any,
    Dict,
)

from lahja import (
    BroadcastConfig,
    Endpoint,
)
from p2p.peer import (
    IdentifiablePeer,
)
from p2p.protocol import (
    Protocol,
)

from .commands import (
    BroadcastData,
    GetSum,
    Sum,
)
from .events import (
    GetSumRequest,
)


class ParagonProtocol(Protocol):
    name = 'paragon'
    version = 1
    _commands = [
        BroadcastData,
        GetSum, Sum,
    ]
    cmd_length = 3
    logger = logging.getLogger("p2p.tools.paragon.proto.ParagonProtocol")

    #
    # Broadcast
    #
    def send_broadcast_data(self, data: bytes) -> None:
        cmd = BroadcastData(self.cmd_id_offset, self.snappy_support)
        msg: Dict[str, Any] = {'data': data}
        header, body = cmd.encode(msg)
        self.send(header, body)

    #
    # Sum
    #
    def send_get_sum(self, value_a: int, value_b: int) -> None:
        cmd = GetSum(self.cmd_id_offset, self.snappy_support)
        msg: Dict[str, Any] = {'a': value_a, 'b': value_b}
        header, body = cmd.encode(msg)
        self.send(header, body)

    def send_sum(self, result: int) -> None:
        cmd = GetSum(self.cmd_id_offset, self.snappy_support)
        msg: Dict[str, Any] = {'result': result}
        header, body = cmd.encode(msg)
        self.send(header, body)


class ProxyParagonProtocol:
    """
    A ``PargonProtocol`` that can be used outside of the process that runs the peer pool. Any
    action performed on this class is delegated to the process that runs the peer pool.
    """

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 event_bus: Endpoint,
                 broadcast_config: BroadcastConfig):
        self._dto_peer = dto_peer
        self._event_bus = event_bus
        self._broadcast_config = broadcast_config

    def send_broadcast_data(self, data: bytes) -> None:
        raise NotImplementedError("Not yet implemented")

    def send_get_sum(self, value_a: int, value_b: int) -> None:
        self._event_bus.broadcast(
            GetSumRequest(self._dto_peer, value_a, value_b),
            self._broadcast_config,
        )

    def send_sum(self, result: int) -> None:
        raise NotImplementedError("Not yet implemented")
