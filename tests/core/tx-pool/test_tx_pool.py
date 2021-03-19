import asyncio
import contextlib
import pytest
import uuid

from async_service import background_asyncio_service
from eth._utils.address import (
    force_bytes_to_address
)
import rlp

from trinity._utils.transactions import DefaultTransactionValidator
from trinity.components.builtin.tx_pool.pool import (
    TxPool,
)

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.protocol.eth.events import (
    TransactionsEvent,
)
from trinity.protocol.eth.peer import (
    ETHProxyPeerPool,
    ETHPeerPoolEventServer
)
from trinity.sync.common.events import SendLocalTransaction
from trinity.tools.factories import LatestETHPeerPairFactory, ChainContextFactory

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
    peer_pair = LatestETHPeerPairFactory(
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

    alice_event_bus = event_bus
    bob_event_bus = other_event_bus
    bob, alice = client_and_server

    bob_peer_pool = MockPeerPoolWithConnectedPeers([bob], event_bus=bob_event_bus)
    alice_peer_pool = MockPeerPoolWithConnectedPeers([alice], event_bus=alice_event_bus)

    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(run_peer_pool_event_server(
            bob_event_bus, bob_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        await stack.enter_async_context(run_peer_pool_event_server(
            alice_event_bus, alice_peer_pool, handler_type=ETHPeerPoolEventServer
        ))

        bob_proxy_peer_pool = ETHProxyPeerPool(bob_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(bob_proxy_peer_pool))

        alice_proxy_peer_pool = ETHProxyPeerPool(alice_event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(alice_proxy_peer_pool))

        alice_tx_pool = TxPool(
            alice_event_bus,
            alice_proxy_peer_pool,
            tx_validator,
        )
        await stack.enter_async_context(background_asyncio_service(alice_tx_pool))

        bob_tx_pool = TxPool(
            bob_event_bus,
            bob_proxy_peer_pool,
            tx_validator,
        )
        await stack.enter_async_context(background_asyncio_service(bob_tx_pool))

        yield (alice, alice_event_bus, alice_tx_pool, ), (bob, bob_event_bus, bob_tx_pool)


@pytest.mark.asyncio
async def test_tx_propagation(two_connected_tx_pools,
                              chain_with_block_validation,
                              funded_address_private_key):

    (
        (alice, alice_event_bus, alice_tx_pool),
        (bob, bob_event_bus, bob_tx_pool)
    ) = two_connected_tx_pools

    alice_incoming_tx, alice_got_tx = observe_incoming_transactions(alice_event_bus)
    bob_incoming_tx, bob_got_tx = observe_incoming_transactions(bob_event_bus)

    txs_broadcasted_by_alice = [
        create_random_tx(chain_with_block_validation, funded_address_private_key)
    ]

    # Alice sends some txs (Important we let the TxPool send them to feed the bloom)
    await alice_tx_pool._handle_tx(bob.session, txs_broadcasted_by_alice)

    await asyncio.wait_for(bob_got_tx.wait(), timeout=0.2)
    assert len(bob_incoming_tx) == 1

    assert bob_incoming_tx[0] == txs_broadcasted_by_alice[0]

    # Clear the recording, we asserted all we want and would like to have a fresh start
    bob_incoming_tx.clear()
    bob_got_tx.clear()

    # Alice sends same txs again (Important we let the TxPool send them to feed the bloom)
    await alice_tx_pool._handle_tx(bob.session, txs_broadcasted_by_alice)

    # Check that Bob doesn't receive them again
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(bob_got_tx.wait(), timeout=0.2)
    assert len(bob_incoming_tx) == 0

    # Bob sends exact same txs back (Important we let the TxPool send them to feed the bloom)
    await alice_tx_pool._handle_tx(alice.session, txs_broadcasted_by_alice)

    # Check that Alice won't get them as that is where they originally came from
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(alice_got_tx.wait(), timeout=0.2)
    assert len(alice_incoming_tx) == 0

    txs_broadcasted_by_bob = [
        create_random_tx(chain_with_block_validation, funded_address_private_key),
        txs_broadcasted_by_alice[0]
    ]

    # Bob sends old + new tx
    await bob_tx_pool._handle_tx(alice.session, txs_broadcasted_by_bob)

    await asyncio.wait_for(alice_got_tx.wait(), timeout=0.2)

    # Check that Alice receives only the one tx that it didn't know about
    assert alice_incoming_tx[0] == txs_broadcasted_by_bob[0]
    assert len(alice_incoming_tx) == 1


@pytest.mark.asyncio
async def test_does_not_propagate_invalid_tx(two_connected_tx_pools,
                                             funded_address_private_key,
                                             chain_with_block_validation):
    (
        (alice, alice_event_bus, alice_tx_pool),
        (bob, bob_event_bus, bob_tx_pool)
    ) = two_connected_tx_pools

    alice_incoming_tx, alice_got_tx = observe_incoming_transactions(alice_event_bus)
    bob_incoming_tx, bob_got_tx = observe_incoming_transactions(bob_event_bus)

    chain = chain_with_block_validation

    txs_broadcasted_by_alice = [
        create_random_tx(chain, funded_address_private_key, is_valid=False),
        create_random_tx(chain, funded_address_private_key)
    ]

    # Alice sends some txs (Important we let the TxPool send them to feed the bloom)
    await alice_tx_pool._handle_tx(bob.session, txs_broadcasted_by_alice)

    # Check that Bob received only the second tx which is valid
    await asyncio.wait_for(bob_got_tx.wait(), timeout=0.2)
    assert len(bob_incoming_tx) == 1
    assert bob_incoming_tx[0] == txs_broadcasted_by_alice[1]


@pytest.mark.asyncio
async def test_local_transaction_propagation(two_connected_tx_pools,
                                             chain_with_block_validation,
                                             funded_address_private_key):

    (
        (alice, alice_event_bus, alice_tx_pool),
        (bob, bob_event_bus, bob_tx_pool)
    ) = two_connected_tx_pools

    alice_incoming_tx, alice_got_tx = observe_incoming_transactions(alice_event_bus)
    bob_incoming_tx, bob_got_tx = observe_incoming_transactions(bob_event_bus)

    local_alice_tx = create_random_tx(chain_with_block_validation, funded_address_private_key)

    await alice_event_bus.broadcast(SendLocalTransaction(local_alice_tx))

    await asyncio.wait_for(bob_got_tx.wait(), timeout=0.2)
    assert len(bob_incoming_tx) == 1

    assert bob_incoming_tx[0] == local_alice_tx


def create_random_tx(chain, private_key, is_valid=True):
    transaction = chain.create_unsigned_transaction(
        nonce=0,
        gas_price=1,
        gas=2100000000000 if is_valid else 0,
        # For simplicity, both peers create tx with the same private key.
        # We rely on unique data to create truly unique txs
        data=uuid.uuid4().bytes,
        to=force_bytes_to_address(b'\x10\x10'),
        value=1,
    ).as_signed_transaction(private_key, chain_id=chain.chain_id)
    return rlp.decode(rlp.encode(transaction))
