from lahja import Endpoint

from p2p.events import ConnectToNodeCommand
from p2p.kademlia import Node
from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.rpc.modules import BaseRPCModule


class Admin(BaseRPCModule):

    def __init__(self, event_bus: Endpoint) -> None:
        self.event_bus = event_bus

    @property
    def name(self) -> str:
        return 'admin'

    async def addPeer(self, node: str) -> None:

        # Throw an exception to show the user if {node} has the wrong format
        enode = Node.from_uri(node)  # noqa: F841

        self.event_bus.broadcast(
            ConnectToNodeCommand(node),
            TO_NETWORKING_BROADCAST_CONFIG
        )
