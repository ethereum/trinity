import asyncio

from async_service import background_asyncio_service

import pytest

from trinity.protocol.fh.api import FirehoseAPI
from trinity.sync.beam.witness import WitnessBroadcaster
from trinity.sync.common.events import StatelessBlockImportDone
from trinity.tools.factories import (
    BlockFieldsFactory,
    Hash32Factory,
    LatestETHPeerPairFactory,
)

from tests.core.peer_helpers import MockPeerPoolWithConnectedPeers


class MockRandomBlock:

    def __init__(self):
        self.fields = BlockFieldsFactory()
        self.hash = self.fields.header.hash


@pytest.mark.asyncio
async def test_witness_broadcaster(event_bus):
    async with LatestETHPeerPairFactory() as (alice, bob):
        assert alice.connection.has_logic(FirehoseAPI.name)
        peer_pool = MockPeerPoolWithConnectedPeers([], event_bus=event_bus)
        broadcaster = WitnessBroadcaster(peer_pool, event_bus)
        async with background_asyncio_service(broadcaster):
            # FIXME: Wait until the broadcaster subscribed to the pool
            await asyncio.sleep(0.2)
            peer_pool._add_peer(alice)
            assert alice in broadcaster._firehose_peers

            block = MockRandomBlock()
            witness_hashes = Hash32Factory.create_batch(10)
            completed = True
            reorg = []
            await event_bus.broadcast(
                StatelessBlockImportDone(block, completed, reorg, witness_hashes, None))
            # FIXME
            await asyncio.sleep(0.2)

            assert set(witness_hashes) == set(bob.fh_api.witnesses.get_node_hashes(block.hash))
