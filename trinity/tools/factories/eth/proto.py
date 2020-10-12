from p2p.abc import HandshakerAPI
from trinity.protocol.eth.proto import ETHProtocolV63, ETHProtocolV64, ETHProtocolV65
from trinity.protocol.wit.proto import WitnessProtocol

try:
    import factory
except ImportError:
    raise ImportError("The p2p.tools.factories module requires the `factory_boy` library.")

from typing import (
    cast,
    AsyncContextManager,
    Tuple,
    Any,
    Type,
)

from lahja import EndpointAPI

from eth_typing import BlockNumber

from eth_keys import keys
from eth.constants import GENESIS_BLOCK_NUMBER

from p2p import kademlia
from p2p.tools.factories import PeerPairFactory

from trinity.protocol.common.context import ChainContext

from trinity.protocol.eth.handshaker import ETHHandshaker
from trinity.protocol.eth.peer import ETHPeer, ETHPeerFactory

from trinity.tools.factories.chain_context import ChainContextFactory

from .payloads import StatusPayloadFactory


class ETHHandshakerFactory(factory.Factory):
    class Meta:
        model = ETHHandshaker

    handshake_params = factory.SubFactory(StatusPayloadFactory)


class ETHV63Peer(ETHPeer):
    supported_sub_protocols = (ETHProtocolV63,)


class ETHV63PeerFactory(ETHPeerFactory):
    peer_class = ETHV63Peer

    async def get_handshakers(self) -> Tuple[HandshakerAPI[Any], ...]:
        return tuple(
            shaker for shaker in await super().get_handshakers()
            # mypy doesn't know these have a `handshake_params` property
            if (shaker.protocol_class == WitnessProtocol or
                shaker.handshake_params.version == ETHProtocolV63.version)  # type: ignore
        )


class ETHV64Peer(ETHPeer):
    supported_sub_protocols = (ETHProtocolV64,)


class ETHV64PeerFactory(ETHPeerFactory):
    peer_class = ETHV64Peer


class ETHV65Peer(ETHPeer):
    supported_sub_protocols = (ETHProtocolV65,)


class ETHV65PeerFactory(ETHPeerFactory):
    peer_class = ETHV65Peer


def LatestETHPeerPairFactory(alice_peer_context: ChainContext = None,
                             alice_remote: kademlia.Node = None,
                             alice_private_key: keys.PrivateKey = None,
                             alice_client_version: str = 'alice',
                             bob_peer_context: ChainContext = None,
                             bob_remote: kademlia.Node = None,
                             bob_private_key: keys.PrivateKey = None,
                             bob_client_version: str = 'bob',
                             event_bus: EndpointAPI = None,
                             peer_factory_class: Type[ETHPeerFactory] = ETHPeerFactory,
                             ) -> AsyncContextManager[Tuple[ETHPeer, ETHPeer]]:
    if alice_peer_context is None:
        alice_peer_context = ChainContextFactory()

    if bob_peer_context is None:
        alice_genesis = alice_peer_context.headerdb.get_canonical_block_header_by_number(
            BlockNumber(GENESIS_BLOCK_NUMBER),
        )
        bob_peer_context = ChainContextFactory(
            headerdb__genesis_params={'timestamp': alice_genesis.timestamp},
        )

    return cast(AsyncContextManager[Tuple[ETHPeer, ETHPeer]], PeerPairFactory(
        alice_peer_context=alice_peer_context,
        alice_peer_factory_class=peer_factory_class,
        bob_peer_context=bob_peer_context,
        bob_peer_factory_class=peer_factory_class,
        alice_remote=alice_remote,
        alice_private_key=alice_private_key,
        alice_client_version=alice_client_version,
        bob_remote=bob_remote,
        bob_private_key=bob_private_key,
        bob_client_version=bob_client_version,
        event_bus=event_bus,
    ))


def ETHV63PeerPairFactory(*,
                          alice_peer_context: ChainContext = None,
                          alice_remote: kademlia.Node = None,
                          alice_private_key: keys.PrivateKey = None,
                          alice_client_version: str = 'alice',
                          bob_peer_context: ChainContext = None,
                          bob_remote: kademlia.Node = None,
                          bob_private_key: keys.PrivateKey = None,
                          bob_client_version: str = 'bob',
                          event_bus: EndpointAPI = None,
                          peer_factory_class: Type[ETHPeerFactory] = ETHV63PeerFactory,
                          ) -> AsyncContextManager[Tuple[ETHPeer, ETHPeer]]:
    return LatestETHPeerPairFactory(
        alice_peer_context=alice_peer_context,
        alice_remote=alice_remote,
        alice_private_key=alice_private_key,
        alice_client_version=alice_client_version,
        bob_peer_context=bob_peer_context,
        bob_remote=bob_remote,
        bob_private_key=bob_private_key,
        bob_client_version=bob_client_version,
        event_bus=event_bus,
        peer_factory_class=peer_factory_class
    )


def ETHV64PeerPairFactory(*,
                          alice_peer_context: ChainContext = None,
                          alice_remote: kademlia.Node = None,
                          alice_private_key: keys.PrivateKey = None,
                          alice_client_version: str = 'alice',
                          bob_peer_context: ChainContext = None,
                          bob_remote: kademlia.Node = None,
                          bob_private_key: keys.PrivateKey = None,
                          bob_client_version: str = 'bob',
                          event_bus: EndpointAPI = None,
                          peer_factory_class: Type[ETHPeerFactory] = ETHV64PeerFactory,
                          ) -> AsyncContextManager[Tuple[ETHPeer, ETHPeer]]:
    return LatestETHPeerPairFactory(
        alice_peer_context=alice_peer_context,
        alice_remote=alice_remote,
        alice_private_key=alice_private_key,
        alice_client_version=alice_client_version,
        bob_peer_context=bob_peer_context,
        bob_remote=bob_remote,
        bob_private_key=bob_private_key,
        bob_client_version=bob_client_version,
        event_bus=event_bus,
        peer_factory_class=peer_factory_class
    )


def ETHV65PeerPairFactory(*,
                          alice_peer_context: ChainContext = None,
                          alice_remote: kademlia.Node = None,
                          alice_private_key: keys.PrivateKey = None,
                          alice_client_version: str = 'alice',
                          bob_peer_context: ChainContext = None,
                          bob_remote: kademlia.Node = None,
                          bob_private_key: keys.PrivateKey = None,
                          bob_client_version: str = 'bob',
                          event_bus: EndpointAPI = None,
                          peer_factory_class: Type[ETHPeerFactory] = ETHV65PeerFactory,
                          ) -> AsyncContextManager[Tuple[ETHPeer, ETHPeer]]:
    return LatestETHPeerPairFactory(
        alice_peer_context=alice_peer_context,
        alice_remote=alice_remote,
        alice_private_key=alice_private_key,
        alice_client_version=alice_client_version,
        bob_peer_context=bob_peer_context,
        bob_remote=bob_remote,
        bob_private_key=bob_private_key,
        bob_client_version=bob_client_version,
        event_bus=event_bus,
        peer_factory_class=peer_factory_class
    )


ALL_PEER_PAIR_FACTORIES = (
    ETHV63PeerPairFactory,
    ETHV64PeerPairFactory,
    ETHV65PeerPairFactory,
)
