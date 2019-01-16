import multiprocessing
import random
import time

import pytest

from trinity.db.rocksdb import (
    ReadonlyRocksDB,
    RocksDB,
)
from trinity._utils.ipc import kill_process_gracefully


@pytest.fixture
def db_path(tmpdir):
    return tmpdir.mkdir("rocks_db_path")


@pytest.fixture
def db(db_path):
    return RocksDB(db_path=db_path)


DB_DATA = {
    b'key-%r' % i: b'value-%r' % i
    for i in range(1024)
}


def seed_database(db):
    for key, value in DB_DATA.items():
        db[key] = value


def do_random_reads(db_path):
    db = RocksDB(db_path=db_path, read_only=True)
    for _ in range(1024):
        idx = random.randint(0, 1023)
        key = b'key-%r' % idx
        expected = b'value-%r' % idx
        value = db[key]
        assert value == expected


def test_database_write_and_read_with_single_process(db):
    db[b"a"] = b"b"
    assert db[b"a"] == b"b"


def test_database_read_access_across_multiple_processes(db, db_path):
    seed_database(db)

    proc_a = multiprocessing.Process(target=do_random_reads, kwargs={'db_path': db_path})
    proc_b = multiprocessing.Process(target=do_random_reads, kwargs={'db_path': db_path})

    proc_a.start()
    proc_b.start()

    try:
        proc_a.join(2)
        proc_b.join(2)
    finally:
        kill_process_gracefully(proc_a)
        kill_process_gracefully(proc_b)

    assert proc_a.exitcode is 0
    assert proc_b.exitcode is 0


def test_database_read_access_across_multiple_processes_with_ongoing_writes(db, db_path):
    seed_database(db)

    data_to_write = {
        b'key-%r' % i: b'value-%r' % i
        for i in range(1024, 4096)
    }

    proc_a = multiprocessing.Process(target=do_random_reads, kwargs={'db_path': db_path})
    proc_b = multiprocessing.Process(target=do_random_reads, kwargs={'db_path': db_path})

    proc_a.start()
    proc_b.start()

    for key, value in data_to_write.items():
        db[key] = value

    try:
        proc_a.join(2)
        proc_b.join(2)
    finally:
        kill_process_gracefully(proc_a)
        kill_process_gracefully(proc_b)

    assert proc_a.exitcode is 0
    assert proc_b.exitcode is 0

    for key, value in data_to_write.items():
        assert db[key] == value


def test_native_readonly_connection_cant_access_data_written_after_connection(db, db_path):
    seed_database(db)

    new_key_and_data = b'new'

    def deferred_read(db_path):
        readonly_db = RocksDB(db_path=db_path, read_only=True)
        # Ensure the key does not exist when we first connect to the database
        assert not readonly_db.exists(new_key_and_data)
        # Wait until the master process has written the data
        time.sleep(0.2)
        # The key still not being present means, we are served a readonly *snapshot*
        # In other words, a readonly connection does not have access to data that is
        # written after the connections was made
        assert not readonly_db.exists(new_key_and_data)

    proc_a = multiprocessing.Process(target=deferred_read, kwargs={'db_path': db_path})
    proc_a.start()

    time.sleep(0.1)
    assert new_key_and_data not in db
    db[new_key_and_data] = new_key_and_data
    assert db[new_key_and_data] == new_key_and_data

    try:
        proc_a.join(2)
    finally:
        kill_process_gracefully(proc_a)

    assert proc_a.exitcode is 0


def test_readyonly_wrapper_allows_reading_ongoing_data(db, db_path):
    seed_database(db)

    new_key_and_data = b'new'

    def deferred_read(db_path):
        readonly_db = ReadonlyRocksDB(db_path=db_path)
        # Ensure the key does not exist when we first connect to the database
        assert not readonly_db.exists(new_key_and_data)
        # Wait until the master process has written the data
        time.sleep(0.2)
        # The key being present means `ReadonlyRocksDB` allows reading data that
        # was written after `ReadonlyRocksDB` was first initialized.
        assert readonly_db.exists(new_key_and_data)

    proc_a = multiprocessing.Process(target=deferred_read, kwargs={'db_path': db_path})
    proc_a.start()

    time.sleep(0.1)
    assert new_key_and_data not in db
    db[new_key_and_data] = new_key_and_data
    assert db[new_key_and_data] == new_key_and_data

    try:
        proc_a.join(2)
    finally:
        kill_process_gracefully(proc_a)

    assert proc_a.exitcode is 0
