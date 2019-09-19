import asyncio

import pytest

from eth.chains.base import MiningChain
from eth.tools.builder.chain import (
    build,
    disable_pow_check,
    genesis,
    latest_mainnet_at,
    mine_block,
)

from trinity.db.eth1.header import AsyncHeaderDB
from trinity.protocol.les.api import LESAPI
from trinity.protocol.les.commands import Announce
from trinity.protocol.les.handshaker import LESHandshakeReceipt
from trinity.protocol.les.proto import LESProtocolV2
from trinity.tools.factories import (
    ChainContextFactory,
    LESV2PeerPairFactory,
)


@pytest.fixture
def bob_chain():
    chain = build(
        MiningChain,
        latest_mainnet_at(0),
        disable_pow_check(),
        genesis(),
    )
    return chain


@pytest.fixture
def alice_headerdb(bob_chain):
    bob_genesis = bob_chain.headerdb.get_canonical_block_header_by_number(0)

    chain = build(
        MiningChain,
        latest_mainnet_at(0),
        disable_pow_check(),
        genesis(params={"timestamp": bob_genesis.timestamp}),
    )
    return AsyncHeaderDB(chain.headerdb.db)


@pytest.fixture
async def alice_and_bob(alice_headerdb, bob_chain):
    pair_factory = LESV2PeerPairFactory(
        alice_client_version='alice',
        alice_peer_context=ChainContextFactory(headerdb=alice_headerdb),
        bob_client_version='bob',
        bob_peer_context=ChainContextFactory(headerdb=AsyncHeaderDB(bob_chain.headerdb.db)),
    )
    async with pair_factory as (alice, bob):
        yield alice, bob


@pytest.fixture
def alice(alice_and_bob):
    alice, _ = alice_and_bob
    return alice


@pytest.fixture
def bob(alice_and_bob):
    _, bob = alice_and_bob
    return bob


@pytest.mark.asyncio
async def test_les_api_properties(alice):
    assert alice.connection.has_logic(LESAPI.name)
    les_api = alice.connection.get_logic(LESAPI.name, LESAPI)

    assert les_api is alice.les_api

    les_receipt = alice.connection.get_receipt_by_type(LESHandshakeReceipt)

    assert les_api.network_id == les_receipt.network_id
    assert les_api.genesis_hash == les_receipt.genesis_hash

    assert les_api.head_info.head_hash == les_receipt.head_hash
    assert les_api.head_info.head_td == les_receipt.head_td
    assert les_api.head_info.head_number == les_receipt.head_number


@pytest.mark.asyncio
async def test_eth_api_head_info_updates_with_announce(alice, bob, bob_chain):
    # mine two blocks on bob's chain
    mine_block()(mine_block()(bob_chain))

    got_announce = asyncio.Event()

    async def _handle_announce(connection, msg):
        got_announce.set()
    alice.connection.add_command_handler(Announce, _handle_announce)

    bob_genesis = bob_chain.headerdb.get_canonical_block_header_by_number(0)
    les_api = alice.connection.get_logic(LESAPI.name, LESAPI)

    assert les_api.head_info.head_hash == bob_genesis.hash
    assert les_api.head_info.head_td == bob_genesis.difficulty
    assert les_api.head_info.head_number == 0

    les_proto = bob.connection.get_protocol_by_type(LESProtocolV2)
    head = bob_chain.get_canonical_head()
    assert head.block_number == 2
    total_difficulty = bob_chain.headerdb.get_score(head.hash)
    les_proto.send_announce(
        head.hash,
        head.block_number,
        total_difficulty,
    )

    await asyncio.wait_for(got_announce.wait(), timeout=1)

    assert alice.connection.has_logic(LESAPI.name)

    assert les_api.head_info.head_hash == head.hash
    assert les_api.head_info.head_td == total_difficulty
    assert les_api.head_info.head_number == 2
