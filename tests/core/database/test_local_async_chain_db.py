import pytest

from eth.db.atomic import (
    AtomicDB,
)
from eth.db.chain import (
    ChainDB,
)

from trinity.db.eth1.chain import (
    AsyncChainDB,
)


@pytest.fixture
def local_async_chaindb():
    core_db = AtomicDB()
    core_db[b'key-a'] = b'value-a'
    chain_db = ChainDB(core_db)
    return AsyncChainDB(chain_db)


@pytest.mark.asyncio
async def test_can_retrieve_data(local_async_chaindb):
    assert await local_async_chaindb.coro_exists(b'key-a')
