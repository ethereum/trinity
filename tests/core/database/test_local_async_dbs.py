import pytest

from eth.chains.mainnet import (
    MAINNET_GENESIS_HEADER,
)
from eth.db.atomic import (
    AtomicDB,
)
from eth.db.chain import (
    ChainDB,
)
from eth.db.header import (
    HeaderDB,
)

from trinity.db.eth1.chain import (
    AsyncChainDB,
)
from trinity.db.eth1.header import (
    AsyncHeaderDB,
)


@pytest.fixture
def local_async_chaindb():
    core_db = AtomicDB()
    core_db[b'key-a'] = b'value-a'
    chain_db = ChainDB(core_db)
    return AsyncChainDB(chain_db)


@pytest.fixture
def local_async_headerdb():
    core_db = AtomicDB()
    headerdb = HeaderDB(core_db)
    headerdb.persist_header(MAINNET_GENESIS_HEADER)
    return AsyncHeaderDB(headerdb)


@pytest.mark.asyncio
async def test_can_retrieve_data_from_chaindb(local_async_chaindb):
    assert await local_async_chaindb.coro_exists(b'key-a')


@pytest.mark.asyncio
async def test_can_retrieve_data_from_headerdb(local_async_headerdb):
    assert await local_async_headerdb.coro_header_exists(MAINNET_GENESIS_HEADER.hash)
