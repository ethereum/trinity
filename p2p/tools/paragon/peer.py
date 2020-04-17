from typing import (
    Iterable,
    Tuple,
    Any,
)

from cached_property import cached_property

from p2p.abc import HandshakerAPI, MultiplexerAPI
from p2p.handshake import Handshaker
from p2p.receipt import HandshakeReceipt

from p2p.abc import BehaviorAPI
from p2p.constants import DEVP2P_V5
from p2p.peer import (
    BasePeer,
    BasePeerContext,
    BasePeerFactory,
)
from p2p.peer_pool import BasePeerPool

from .proto import ParagonProtocol
from .api import ParagonAPI


class ParagonPeer(BasePeer):
    supported_sub_protocols = (ParagonProtocol,)
    sub_proto: ParagonProtocol = None

    @cached_property
    def paragon_api(self) -> ParagonAPI:
        return self.connection.get_logic(ParagonAPI.name, ParagonAPI)

    def get_behaviors(self) -> Tuple[BehaviorAPI, ...]:
        return super().get_behaviors() + (ParagonAPI().as_behavior(),)


class ParagonContext(BasePeerContext):
    # nothing magic here.  Simply an example of how the context class can be
    # used to store data specific to a certain peer class.
    paragon: str = "paragon"

    def __init__(self,
                 client_version_string: str = 'paragon-test',
                 listen_port: int = 30303,
                 p2p_version: int = DEVP2P_V5) -> None:
        super().__init__(client_version_string, listen_port, p2p_version)


class ParagonHandshaker(Handshaker[ParagonProtocol]):
    protocol_class = ParagonProtocol

    async def do_handshake(self,
                           multiplexer: MultiplexerAPI,
                           protocol: ParagonProtocol) -> HandshakeReceipt:
        return HandshakeReceipt(protocol)


class ParagonPeerFactory(BasePeerFactory):
    peer_class = ParagonPeer
    context: ParagonContext

    async def get_handshakers(self) -> Tuple[HandshakerAPI[Any], ...]:
        return (ParagonHandshaker(),)


class ParagonPeerPool(BasePeerPool):
    peer_factory_class = ParagonPeerFactory
    context: ParagonContext

    async def maybe_connect_more_peers(self) -> None:
        await self.manager.wait_finished()


class ParagonMockPeerPoolWithConnectedPeers(ParagonPeerPool):
    def __init__(self, peers: Iterable[ParagonPeer]) -> None:
        super().__init__(privkey=None, context=None)
        for peer in peers:
            self.connected_nodes[peer.session] = peer

    async def run(self) -> None:
        raise NotImplementedError("This is a mock PeerPool implementation, you must not _run() it")
