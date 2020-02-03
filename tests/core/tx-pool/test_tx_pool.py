import asyncio
import pytest
import uuid

from async_exit_stack import AsyncExitStack
from async_service import background_asyncio_service
from eth._utils.address import (
    force_bytes_to_address
)

from p2p.tools.factories import SessionFactory

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.components.builtin.tx_pool.pool import (
    TxPool,
)
from trinity.components.builtin.tx_pool.validators import (
    DefaultTransactionValidator
)
from trinity.protocol.common.events import (
    GetConnectedPeersRequest,
    GetConnectedPeersResponse,
)
from trinity.protocol.eth.commands import (
    Transactions
)
from trinity.protocol.eth.events import (
    TransactionsEvent,
    SendTransactionsEvent,
)
from trinity.tools.event_bus import mock_request_response

from trinity.protocol.eth.peer import (
    ETHProxyPeerPool,
)


TEST_NODES = tuple(SessionFactory.create_batch(2))


@pytest.fixture
def tx_validator(chain_with_block_validation):
    return DefaultTransactionValidator(chain_with_block_validation, 0)


def observe_outgoing_transactions(event_bus):
    outgoing_tx = []
    got_txns = asyncio.Event()

    async def _txn_handler(event):
        got_txns.clear()
        outgoing_tx.append((event.session, event.command.payload))
        got_txns.set()

    event_bus.subscribe(SendTransactionsEvent, _txn_handler)

    return outgoing_tx, got_txns


@pytest.mark.asyncio
async def test_tx_propagation(event_bus,
                              funded_address_private_key,
                              chain_with_block_validation,
                              tx_validator):

    initial_two_peers = TEST_NODES[:2]
    node_one = initial_two_peers[0]
    node_two = initial_two_peers[1]

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(mock_request_response(
            GetConnectedPeersRequest,
            GetConnectedPeersResponse(initial_two_peers),
            event_bus,
        ))

        peer_pool = ETHProxyPeerPool(event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(peer_pool))

        tx_pool = TxPool(event_bus, peer_pool, tx_validator)
        await stack.enter_async_context(background_asyncio_service(tx_pool))

        await asyncio.sleep(0.01)

        txs_broadcasted_by_peer1 = [
            create_random_tx(chain_with_block_validation, funded_address_private_key)
        ]

        # this needs to go here to ensure that the subscription is *after*
        # the one installed by the transaction pool so that the got_txns
        # event will get set after the other handlers have been called.
        outgoing_tx, got_txns = observe_outgoing_transactions(event_bus)

        # Peer1 sends some txs
        await event_bus.broadcast(
            TransactionsEvent(session=node_one, command=Transactions(txs_broadcasted_by_peer1))
        )

        await asyncio.wait_for(got_txns.wait(), timeout=0.1)

        assert outgoing_tx == [
            (node_two, tuple(txs_broadcasted_by_peer1)),
        ]
        # Clear the recording, we asserted all we want and would like to have a fresh start
        outgoing_tx.clear()

        # Peer1 sends same txs again
        await event_bus.broadcast(
            TransactionsEvent(session=node_one, command=Transactions(txs_broadcasted_by_peer1))
        )
        await asyncio.wait_for(got_txns.wait(), timeout=0.1)
        # Check that Peer2 doesn't receive them again
        assert len(outgoing_tx) == 0

        # Peer2 sends exact same txs back
        await event_bus.broadcast(
            TransactionsEvent(session=node_two, command=Transactions(txs_broadcasted_by_peer1))
        )
        await asyncio.wait_for(got_txns.wait(), timeout=0.1)

        # Check that Peer1 won't get them as that is where they originally came from
        assert len(outgoing_tx) == 0

        txs_broadcasted_by_peer2 = [
            create_random_tx(chain_with_block_validation, funded_address_private_key),
            txs_broadcasted_by_peer1[0]
        ]

        # Peer2 sends old + new tx
        await event_bus.broadcast(
            TransactionsEvent(session=node_two, command=Transactions(txs_broadcasted_by_peer2))
        )
        await asyncio.wait_for(got_txns.wait(), timeout=0.1)
        # Not sure why this sleep is needed....
        await asyncio.sleep(0.01)

        # Check that Peer1 receives only the one tx that it didn't know about
        assert outgoing_tx == [
            (node_one, (txs_broadcasted_by_peer2[0],)),
        ]


@pytest.mark.asyncio
async def test_does_not_propagate_invalid_tx(event_bus,
                                             funded_address_private_key,
                                             chain_with_block_validation,
                                             tx_validator):
    chain = chain_with_block_validation

    initial_two_peers = TEST_NODES[:2]
    node_one = initial_two_peers[0]
    node_two = initial_two_peers[1]

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(mock_request_response(
            GetConnectedPeersRequest,
            GetConnectedPeersResponse(initial_two_peers),
            event_bus,
        ))

        peer_pool = ETHProxyPeerPool(event_bus, TO_NETWORKING_BROADCAST_CONFIG)
        await stack.enter_async_context(background_asyncio_service(peer_pool))

        tx_pool = TxPool(event_bus, peer_pool, tx_validator)
        await stack.enter_async_context(background_asyncio_service(tx_pool))

        await asyncio.sleep(0.01)

        txs_broadcasted_by_peer1 = [
            create_random_tx(chain, funded_address_private_key, is_valid=False),
            create_random_tx(chain, funded_address_private_key)
        ]

        outgoing_tx, got_txns = observe_outgoing_transactions(event_bus)

        # Peer1 sends some txs
        await event_bus.broadcast(
            TransactionsEvent(session=node_one, command=Transactions(txs_broadcasted_by_peer1))
        )
        await asyncio.wait_for(got_txns.wait(), timeout=0.1)

        # Check that Peer2 received only the second tx which is valid
        assert outgoing_tx == [
            (node_two, (txs_broadcasted_by_peer1[1],)),
        ]


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
