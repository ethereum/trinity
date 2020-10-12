import pytest

from p2p.tools.factories.peer import PeerPairFactory

from trinity.protocol.eth.peer import ETHPeerFactory
from trinity.protocol.wit.api import WitnessAPI
from trinity.tools.factories import (
    ChainContextFactory,
    LatestETHPeerPairFactory,
)


@pytest.mark.asyncio
async def test_wit_api_property():
    async with LatestETHPeerPairFactory() as (alice, bob):
        assert alice.connection.has_logic(WitnessAPI.name)
        wit_api = alice.connection.get_logic(WitnessAPI.name, WitnessAPI)

        assert wit_api is alice.wit_api


@pytest.mark.asyncio
async def test_no_wit_api_property_when_witness_not_supported():
    class ETHPeerFactoryWithoutWitness(ETHPeerFactory):
        def _get_wit_handshakers(self):
            return tuple()

    peer_context = ChainContextFactory()
    peer_pair_factory = PeerPairFactory(
        alice_peer_context=peer_context,
        alice_peer_factory_class=ETHPeerFactory,
        bob_peer_context=peer_context,
        bob_peer_factory_class=ETHPeerFactoryWithoutWitness,
    )
    async with peer_pair_factory as (alice, bob):
        assert not hasattr(bob, 'wit_api')
        assert not hasattr(alice, 'wit_api')
