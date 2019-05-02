import asyncio

from cancel_token import CancelToken

from eth_utils import (
    to_tuple,
)
from eth_utils.toolz import (
    merge,
)

from eth.constants import (
    ZERO_HASH32,
)
from eth.db.atomic import AtomicDB

from eth2.beacon.db.chain import BeaconChainDB
from trinity.db.beacon.chain import BaseAsyncBeaconChainDB
from eth2.beacon.types.blocks import (
    BeaconBlock,
    BeaconBlockBody,
)

from trinity.protocol.bcc.context import BeaconContext
from trinity.protocol.bcc.peer import (
    BCCPeerFactory,
    BCCPeerPool,
)

from p2p import ecies
from p2p.tools.paragon.helpers import (
    get_directly_linked_peers_without_handshake as _get_directly_linked_peers_without_handshake,
    get_directly_linked_peers as _get_directly_linked_peers,
)
from eth2.beacon.constants import (
    EMPTY_SIGNATURE,
)
from tests.core.integration_test_helpers import (
    async_passthrough,
)
from eth.db.backends.base import (
    BaseAtomicDB,
)
from eth2.beacon.state_machines.forks.serenity import SERENITY_CONFIG


class FakeAsyncBeaconChainDB(BaseAsyncBeaconChainDB, BeaconChainDB):

    def __init__(self, db: BaseAtomicDB) -> None:
        self.db = db

    coro_persist_block = async_passthrough('persist_block')
    coro_get_canonical_block_root = async_passthrough('get_canonical_block_root')
    coro_get_canonical_block_by_slot = async_passthrough('get_canonical_block_by_slot')
    coro_get_canonical_block_root_by_slot = async_passthrough('get_canonical_block_root_by_slot')
    coro_get_canonical_head = async_passthrough('get_canonical_head')
    coro_get_canonical_head_root = async_passthrough('get_canonical_head_root')
    coro_get_finalized_head = async_passthrough('get_finalized_head')
    coro_get_block_by_root = async_passthrough('get_block_by_root')
    coro_get_score = async_passthrough('get_score')
    coro_block_exists = async_passthrough('block_exists')
    coro_persist_block_chain = async_passthrough('persist_block_chain')
    coro_get_state_by_root = async_passthrough('get_state_by_root')
    coro_persist_state = async_passthrough('persist_state')
    coro_exists = async_passthrough('exists')
    coro_get = async_passthrough('get')


def create_test_block(parent=None, **kwargs):
    defaults = {
        "slot": SERENITY_CONFIG.GENESIS_SLOT,
        "previous_block_root": ZERO_HASH32,
        "state_root": ZERO_HASH32,  # note: not the actual genesis state root
        "signature": EMPTY_SIGNATURE,
        "body": BeaconBlockBody.create_empty_body()
    }

    if parent is not None:
        kwargs["previous_block_root"] = parent.signing_root
        kwargs["slot"] = parent.slot + 1

    return BeaconBlock(**merge(defaults, kwargs))


@to_tuple
def create_branch(length, root=None, **start_kwargs):
    if length == 0:
        return

    if root is None:
        root = create_test_block()

    parent = create_test_block(parent=root, **start_kwargs)
    yield parent

    for _ in range(root.slot + 2, root.slot + length + 1):
        child = create_test_block(parent)
        yield child
        parent = child


async def get_chain_db(blocks=()):
    db = AtomicDB()
    chain_db = FakeAsyncBeaconChainDB(db)
    await chain_db.coro_persist_block_chain(blocks, BeaconBlock)
    return chain_db


async def get_genesis_chain_db():
    genesis = create_test_block()
    return await get_chain_db((genesis,))


async def _setup_alice_and_bob_factories(alice_chain_db, bob_chain_db):
    cancel_token = CancelToken('trinity.get_directly_linked_peers_without_handshake')

    #
    # Alice
    #
    alice_context = BeaconContext(
        chain_db=alice_chain_db,
        network_id=1,
    )

    alice_factory = BCCPeerFactory(
        privkey=ecies.generate_privkey(),
        context=alice_context,
        token=cancel_token,
    )

    #
    # Bob
    #
    bob_context = BeaconContext(
        chain_db=bob_chain_db,
        network_id=1,
    )

    bob_factory = BCCPeerFactory(
        privkey=ecies.generate_privkey(),
        context=bob_context,
        token=cancel_token,
    )

    return alice_factory, bob_factory


async def get_directly_linked_peers_without_handshake(alice_chain_db, bob_chain_db):
    alice_factory, bob_factory = await _setup_alice_and_bob_factories(alice_chain_db, bob_chain_db)

    return await _get_directly_linked_peers_without_handshake(
        alice_factory=alice_factory,
        bob_factory=bob_factory,
    )


async def get_directly_linked_peers(request, event_loop, alice_chain_db, bob_chain_db):
    alice_factory, bob_factory = await _setup_alice_and_bob_factories(
        alice_chain_db,
        bob_chain_db,
    )

    return await _get_directly_linked_peers(
        request,
        event_loop,
        alice_factory=alice_factory,
        bob_factory=bob_factory,
    )


async def get_directly_linked_peers_in_peer_pools(request,
                                                  event_loop,
                                                  alice_chain_db,
                                                  bob_chain_db):
    alice, bob = await get_directly_linked_peers(
        request,
        event_loop,
        alice_chain_db=alice_chain_db,
        bob_chain_db=bob_chain_db,
    )
    alice_peer_pool = BCCPeerPool(alice.privkey, alice.context)
    bob_peer_pool = BCCPeerPool(bob.privkey, bob.context)

    asyncio.ensure_future(alice_peer_pool.run())
    asyncio.ensure_future(bob_peer_pool.run())

    def finalizer():
        event_loop.run_until_complete(alice_peer_pool.cancel())
        event_loop.run_until_complete(bob_peer_pool.cancel())

    request.addfinalizer(finalizer)

    alice_peer_pool._add_peer(alice, [])
    bob_peer_pool._add_peer(bob, [])

    return alice, alice_peer_pool, bob, bob_peer_pool
