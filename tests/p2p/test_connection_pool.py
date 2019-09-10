import pytest
from p2p.pool import ConnectionPool

from p2p.tools.factories import ConnectionPairFactory


@pytest.fixture
async def connections():
    async with ConnectionPairFactory() as (alice, _):
        async with ConnectionPairFactory() as (_, bob):
            async with ConnectionPairFactory(alice_client_version='carol') as (carol, _):
                yield alice, bob, carol


@pytest.mark.asyncio
async def test_connection_pool(connections):
    alice, bob, carol = connections

    pool = ConnectionPool()

    assert len(pool) == 0
    assert set(pool) == set()
    assert set(conn for conn in pool) == set()

    assert alice not in pool
    assert bob not in pool
    assert carol not in pool

    pool.add(alice)
    assert len(pool) == 1
    assert set(pool) == {alice}
    assert set(conn for conn in pool) == {alice}

    assert alice in pool
    assert bob not in pool
    assert carol not in pool

    pool.add(bob)
    assert len(pool) == 2
    assert set(pool) == {alice, bob}
    assert set(conn for conn in pool) == {alice, bob}

    assert alice in pool
    assert bob in pool
    assert carol not in pool

    pool.add(carol)
    assert len(pool) == 3
    assert set(pool) == {alice, bob, carol}
    assert set(conn for conn in pool) == {alice, bob, carol}

    assert alice in pool
    assert bob in pool
    assert carol in pool

    pool.remove(bob)

    assert len(pool) == 2
    assert bob not in pool

    with pytest.raises(KeyError):
        pool.remove(bob)
