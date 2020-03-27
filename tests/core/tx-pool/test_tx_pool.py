import asyncio
import pytest
import uuid

from async_exit_stack import AsyncExitStack
from async_service import background_asyncio_service
from eth._utils.address import (
    force_bytes_to_address
)

from p2p.service import run_service

from trinity.components.builtin.tx_pool.pool import (
    TxPool,
)
from trinity.components.builtin.tx_pool.validators import (
    DefaultTransactionValidator
)
from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.protocol.eth.events import (
    TransactionsEvent,
)
from trinity.protocol.eth.peer import (
    ETHProxyPeerPool,
    ETHPeerPoolEventServer
)
from trinity.tools.factories import ETHPeerPairFactory, ChainContextFactory

from tests.core.integration_test_helpers import run_peer_pool_event_server
from tests.core.peer_helpers import MockPeerPoolWithConnectedPeers


def observe_incoming_transactions(event_bus):
    incoming_tx = []
    got_txns = asyncio.Event()

    async def _txn_handler(event):
        got_txns.clear()

        incoming_tx.append(event.command.payload[0])
        got_txns.set()

    event_bus.subscribe(TransactionsEvent, _txn_handler)

    return incoming_tx, got_txns


@pytest.fixture
def tx_validator(chain_with_block_validation):
    return DefaultTransactionValidator(chain_with_block_validation, 0)


@pytest.fixture
async def client_and_server():
    peer_pair = ETHPeerPairFactory(
        alice_peer_context=ChainContextFactory(),
        bob_peer_context=ChainContextFactory(),
    )
    async with peer_pair as (client_peer, server_peer):
        yield client_peer, server_peer


@pytest.fixture
async def two_connected_tx_pools(event_bus,
                                 other_event_bus,
                                 event_loop,
                                 funded_address_private_key,
                                 chain_with_block_validation,
                                 tx_validator,
                                 client_and_server):

    peer1_event_bus = event_bus
    peer2_event_bus = other_event_bus
    peer2, peer1 = client_and_server

    peer2_peer_pool = MockPeerPoolWithConnectedPeers([peer2], event_bus=peer2_event_bus)
    peer1_peer_pool = MockPeerPoolWithConnectedPeers([peer1], event_bus=peer1_event_bus)

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            peer2_event_bus, peer2_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        await stack.enter_async_context(run_peer_pool_event_server(
            peer1_event_bus, peer1_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        peer1_tx_pool = TxPool(
            peer1_event_bus,
            ETHProxyPeerPool(peer1_event_bus, TO_NETWORKING_BROADCAST_CONFIG),
            tx_validator,
        )
        await stack.enter_async_context(background_asyncio_service(peer1_tx_pool))

        peer2_tx_pool = TxPool(
            peer2_event_bus,
            ETHProxyPeerPool(peer2_event_bus, TO_NETWORKING_BROADCAST_CONFIG),
            tx_validator,
        )
        await stack.enter_async_context(background_asyncio_service(peer2_tx_pool))

        peer2_proxy_peer_pool = ETHProxyPeerPool(peer2_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(run_service(peer2_proxy_peer_pool))

        peer1_proxy_peer_pool = ETHProxyPeerPool(peer1_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(run_service(peer1_proxy_peer_pool))

        yield (peer1, peer1_event_bus, peer1_tx_pool, ), (peer2, peer2_event_bus, peer2_tx_pool)


@pytest.mark.asyncio
async def test_tx_propagation(two_connected_tx_pools,
                              chain_with_block_validation,
                              funded_address_private_key):

    (
        (peer1, peer1_event_bus, peer1_tx_pool),
        (peer2, peer2_event_bus, peer2_tx_pool)
    ) = two_connected_tx_pools

    peer1_incoming_tx, peer1_got_tx = observe_incoming_transactions(peer1_event_bus)
    peer2_incoming_tx, peer2_got_tx = observe_incoming_transactions(peer2_event_bus)

    txs_broadcasted_by_peer1 = [
        create_random_tx(chain_with_block_validation, funded_address_private_key)
    ]

    # Peer1 sends some txs (Important we let the TxPool send them to feed the bloom)
    await peer1_tx_pool._handle_tx(peer2.session, txs_broadcasted_by_peer1)

    await asyncio.wait_for(peer2_got_tx.wait(), timeout=0.01)
    assert len(peer2_incoming_tx) == 1

    assert peer2_incoming_tx[0].as_dict() == txs_broadcasted_by_peer1[0].as_dict()

    # Clear the recording, we asserted all we want and would like to have a fresh start
    peer2_incoming_tx.clear()
    peer2_got_tx.clear()

    # Peer1 sends same txs again (Important we let the TxPool send them to feed the bloom)
    await peer1_tx_pool._handle_tx(peer2.session, txs_broadcasted_by_peer1)

    # Check that Peer2 doesn't receive them again
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(peer2_got_tx.wait(), timeout=0.01)
    assert len(peer2_incoming_tx) == 0

    # Peer2 sends exact same txs back (Important we let the TxPool send them to feed the bloom)
    await peer1_tx_pool._handle_tx(peer1.session, txs_broadcasted_by_peer1)

    # Check that Peer1 won't get them as that is where they originally came from
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(peer1_got_tx.wait(), timeout=0.01)
    assert len(peer1_incoming_tx) == 0

    txs_broadcasted_by_peer2 = [
        create_random_tx(chain_with_block_validation, funded_address_private_key),
        txs_broadcasted_by_peer1[0]
    ]

    # Peer2 sends old + new tx
    await peer2_tx_pool._handle_tx(peer1.session, txs_broadcasted_by_peer2)

    await asyncio.wait_for(peer1_got_tx.wait(), timeout=0.01)

    # Check that Peer1 receives only the one tx that it didn't know about
    assert peer1_incoming_tx[0].as_dict() == txs_broadcasted_by_peer2[0].as_dict()
    assert len(peer1_incoming_tx) == 1


@pytest.mark.asyncio
async def test_does_not_propagate_invalid_tx(two_connected_tx_pools,
                                             funded_address_private_key,
                                             chain_with_block_validation):
    (
        (peer1, peer1_event_bus, peer1_tx_pool),
        (peer2, peer2_event_bus, peer2_tx_pool)
    ) = two_connected_tx_pools

    peer1_incoming_tx, peer1_got_tx = observe_incoming_transactions(peer1_event_bus)
    peer2_incoming_tx, peer2_got_tx = observe_incoming_transactions(peer2_event_bus)

    chain = chain_with_block_validation

    txs_broadcasted_by_peer1 = [
        create_random_tx(chain, funded_address_private_key, is_valid=False),
        create_random_tx(chain, funded_address_private_key)
    ]

    # Peer1 sends some txs (Important we let the TxPool send them to feed the bloom)
    await peer1_tx_pool._handle_tx(peer2.session, txs_broadcasted_by_peer1)

    # Check that Peer2 received only the second tx which is valid
    await asyncio.wait_for(peer2_got_tx.wait(), timeout=0.01)
    assert len(peer2_incoming_tx) == 1
    assert peer2_incoming_tx[0].as_dict() == txs_broadcasted_by_peer1[1].as_dict()


def create_random_tx(chain, private_key, is_valid=True):
    return chain.create_unsigned_transaction(
        nonce=0,
        gas_price=1,
        gas=2100000000000 if is_valid else 0,
        # For simplicity, both peers create tx with the same private key.
        # We rely on unique data to create truly unique txs
        data=uuid.uuid4().bytes,
        to=force_bytes_to_address(b'\x10\x10'),
        value=1,
    ).as_signed_transaction(private_key)
