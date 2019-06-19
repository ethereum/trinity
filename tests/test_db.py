from eth.db.atomic import AtomicDB

import trio
import pathlib
import pytest
import tempfile
import pytest_trio
from trinity.db_manager import (
    DBManager,
    AsyncDBClient,
    DBClient,
)


@pytest.fixture
def ipc_path():
    with tempfile.TemporaryDirectory() as dir:
        yield pathlib.Path(dir) / "foo.ipc"


@pytest.fixture
def db():
    return AtomicDB()


@pytest_trio.trio_fixture
async def manager(db, ipc_path):
    m = DBManager(db)
    async with m.run(ipc_path):
        yield m


@pytest_trio.trio_fixture
async def async_client_db(manager, ipc_path):
    client_db = await AsyncDBClient.connect(ipc_path)
    return client_db


@pytest.mark.trio
async def test_the_thing(ipc_path, db, manager):
    db[b'key'] = b'value'

    client_db = await AsyncDBClient.connect(ipc_path)
    assert await client_db.get(b'key') == b'value'


@pytest.mark.trio
async def test_another(ipc_path, db, manager):
    db[b'key'] = b'value'

    def do_client():
        client_db = DBClient.connect(ipc_path)
        assert client_db.get(b'key') == b'value'

    await trio.run_sync_in_worker_thread(do_client)


@pytest.mark.trio
async def test_atomic_db_with_set_and_get(db, async_client_db):

    await async_client_db.set(b'key-1', b'value-1')
    await async_client_db.set(b'key-2', b'value-2')
    assert await async_client_db.get(b'key-1') == b'value-1'
    assert await async_client_db.get(b'key-2') == b'value-2'


@pytest.mark.trio
async def test_atomic_db_with_set_and_delete(db, async_client_db):
    db[b'key-1'] = b'origin'

    await async_client_db.delete(b'key-1')
    with pytest.raises(KeyError):
        db[b'key-1']

    with pytest.raises(KeyError):
        await async_client_db.get(b'key-1')

    assert not await async_client_db.exists(b'key-1')
