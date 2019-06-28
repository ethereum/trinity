from eth.db.atomic import AtomicDB

import pathlib
import pytest
import tempfile
import pytest_trio
from trinity.db.manager.manager import (
    DBManager,
)
from trinity.db.manager.client import (
    AsyncDBClient,
    SyncDBClient,
)


@pytest.fixture
def ipc_path():
    with tempfile.TemporaryDirectory() as dir:
        ipc_path = pathlib.Path(dir) / "db_manager.ipc"
        yield ipc_path


@pytest.fixture
def db():
    return AtomicDB()


@pytest.fixture
def db_manager(db, ipc_path):
    manager = DBManager(db)
    with manager.run(ipc_path) as running_manager:
        yield running_manager


@pytest_trio.trio_fixture
async def async_client_db(ipc_path, db_manager):
    client_db = await AsyncDBClient.connect(ipc_path)
    yield client_db
    await client_db._socket.aclose()


@pytest.fixture
def sync_client_db(ipc_path, db_manager):
    client_db = SyncDBClient.connect(ipc_path)
    yield client_db
    client_db._socket.close()


@pytest.mark.trio
async def test_read_sanity(ipc_path, db, db_manager):
    db[b'key'] = b'value'
    client_db = await AsyncDBClient.connect(ipc_path)
    assert await client_db.get(b'key') == b'value'


@pytest.mark.trio
async def test_atomic_db_with_set_and_get(async_client_db):
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

#
# Test SyncDBClient
#


def test_read_sanity_sync(ipc_path, db, db_manager):
    db[b'key'] = b'value'
    client_db = SyncDBClient.connect(ipc_path)
    assert client_db.get(b'key') == b'value'


def test_atomic_db_with_set_and_get_sync(sync_client_db):
    sync_client_db.set(b'key-1', b'value-1')
    sync_client_db.set(b'key-2', b'value-2')
    assert sync_client_db.get(b'key-1') == b'value-1'
    assert sync_client_db.get(b'key-2') == b'value-2'


def test_atomic_db_with_set_and_delete_sync(db, sync_client_db):
    db[b'key-1'] = b'origin'

    sync_client_db.delete(b'key-1')
    with pytest.raises(KeyError):
        db[b'key-1']

    with pytest.raises(KeyError):
        sync_client_db.get(b'key-1')

    assert not sync_client_db.exists(b'key-1')
