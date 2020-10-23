import pytest

from eth.db.atomic import AtomicDB

from trinity.protocol.wit.db import AsyncWitnessDB
from trinity.tools.factories import Hash32Factory


@pytest.mark.asyncio
async def test_db():
    wit_db = AsyncWitnessDB(AtomicDB())

    hash1 = Hash32Factory()
    with pytest.raises(KeyError):
        await wit_db.coro_get_witness_hashes(hash1)

    hash1_witnesses = tuple(Hash32Factory.create_batch(5))
    await wit_db.coro_persist_witness_hashes(hash1, hash1_witnesses)
    assert await wit_db.coro_get_witness_hashes(hash1) == hash1_witnesses

    for _ in range(wit_db._max_witness_history):
        await wit_db.coro_persist_witness_hashes(Hash32Factory(), Hash32Factory.create_batch(2))

    with pytest.raises(KeyError):
        await wit_db.coro_get_witness_hashes(hash1)

    assert len(wit_db._get_recent_blocks_with_witnesses()) == wit_db._max_witness_history
