import pytest

from eth.db.backends.memory import MemoryDB

from trinity.exceptions import WitnessHashesUnavailable
from trinity.protocol.wit.db import AsyncWitnessDB
from trinity.tools.factories import Hash32Factory


@pytest.mark.asyncio
async def test_persisting_and_looking_up():
    wit_db = AsyncWitnessDB(MemoryDB())

    hash1 = Hash32Factory()
    with pytest.raises(WitnessHashesUnavailable):
        await wit_db.coro_get_witness_hashes(hash1)

    hash1_witnesses = tuple(Hash32Factory.create_batch(5))
    await wit_db.coro_persist_witness_hashes(hash1, hash1_witnesses)
    assert await wit_db.coro_get_witness_hashes(hash1) == hash1_witnesses


@pytest.mark.asyncio
async def test_witness_for_recent_blocks():
    wit_db = AsyncWitnessDB(MemoryDB())
    hash1 = Hash32Factory()
    hash1_witnesses = tuple(Hash32Factory.create_batch(5))
    await wit_db.coro_persist_witness_hashes(hash1, hash1_witnesses)

    # *almost* push the first witness out of history
    for _ in range(wit_db._max_witness_history - 1):
        await wit_db.coro_persist_witness_hashes(Hash32Factory(), Hash32Factory.create_batch(2))

    # It should still be there...
    assert await wit_db.coro_get_witness_hashes(hash1) == hash1_witnesses

    # Until one more new witness is added.
    await wit_db.coro_persist_witness_hashes(Hash32Factory(), Hash32Factory.create_batch(2))

    # Now the old witness has been flushed out of the history
    with pytest.raises(WitnessHashesUnavailable):
        await wit_db.coro_get_witness_hashes(hash1)

    assert len(wit_db._get_recent_blocks_with_witnesses()) == wit_db._max_witness_history
