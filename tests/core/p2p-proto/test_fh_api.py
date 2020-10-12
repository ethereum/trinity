import asyncio

import pytest

from trinity.protocol.fh.api import FirehoseAPI
from trinity.tools.factories import (
    BlockHashFactory,
    LatestETHPeerPairFactory,
)


@pytest.mark.asyncio
async def test_fh_api_property():
    async with LatestETHPeerPairFactory() as (alice, bob):
        assert alice.connection.has_logic(FirehoseAPI.name)
        fh_api = alice.connection.get_logic(FirehoseAPI.name, FirehoseAPI)

        assert fh_api is alice.fh_api


@pytest.mark.asyncio
async def test_new_block_witness_hashes():
    block_hash = BlockHashFactory()
    node_hashes = tuple(BlockHashFactory.create_batch(5))

    alice_got_witness_hashes = asyncio.Event()
    bob_got_witness_hashes = asyncio.Event()

    async def bob_recv_witness_hashes_cb(payload):
        assert payload.node_hashes == node_hashes
        bob_got_witness_hashes.set()

    async def alice_recv_witness_hashes_cb(payload):
        alice_got_witness_hashes.set()

    async with LatestETHPeerPairFactory() as (alice, bob):
        bob.fh_api.witnesses.subscribe(bob_recv_witness_hashes_cb)
        alice.fh_api.witnesses.subscribe(alice_recv_witness_hashes_cb)

        # Alice sends the witness hashes to bob
        alice.fh_api.send_new_block_witness_hashes(block_hash, node_hashes)

        await asyncio.wait_for(bob_got_witness_hashes.wait(), timeout=0.5)
        assert bob.fh_api.witnesses.has_witness(block_hash)
        assert bob.fh_api.witnesses.get_node_hashes(block_hash) == node_hashes

        # Bob wouldn't send the same hashes to alice, as it knows she already has them.
        bob.fh_api.send_new_block_witness_hashes(block_hash, node_hashes)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(alice_got_witness_hashes.wait(), timeout=0.5)

        # And the above will also cause bob to remove those witness hases from its history.
        assert not bob.fh_api.witnesses.has_witness(block_hash)
