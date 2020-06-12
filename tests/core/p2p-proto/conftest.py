import pytest

from eth.db.atomic import AtomicDB

from tests.core.integration_test_helpers import (
    load_fixture_db,
    load_mining_chain,
    DBFixture,
)


@pytest.fixture
def leveldb_20():
    yield from load_fixture_db(DBFixture.TWENTY_POW_HEADERS)


@pytest.fixture
def leveldb_1000():
    yield from load_fixture_db(DBFixture.THOUSAND_POW_HEADERS)


@pytest.fixture
def leveldb_uncle_chain():
    yield from load_fixture_db(DBFixture.UNCLE_CHAIN)


@pytest.fixture
def chaindb_uncle(leveldb_1000, leveldb_uncle_chain):
    canoncical_chain = load_mining_chain(AtomicDB(leveldb_1000))
    uncle_chain = load_mining_chain(AtomicDB(leveldb_uncle_chain))
    # This fixture shares a common history with `leveldb_1000` from genesis till block 474.
    # It then forks of and contains uncles from 475 till 1000. These numbers were picked because
    # it fully spans the first gap defined in `chaindb_with_gaps` (test_sync.py) and only
    # partially spans the second gap defined in `chaindb_with_gaps`.
    header_before_fork = canoncical_chain.get_canonical_block_header_by_number(474)
    assert uncle_chain.get_canonical_block_header_by_number(474) == header_before_fork
    # Forks at header 475
    fork_header = canoncical_chain.get_canonical_block_header_by_number(475)
    assert uncle_chain.get_canonical_block_header_by_number(475) != fork_header

    assert uncle_chain.chaindb.get_canonical_head().block_number == 1000
    return uncle_chain.chaindb


@pytest.fixture
def chaindb_1000(leveldb_1000):
    chain = load_mining_chain(AtomicDB(leveldb_1000))
    assert chain.chaindb.get_canonical_head().block_number == 1000
    return chain.chaindb


@pytest.fixture
def chaindb_20(leveldb_20):
    chain = load_mining_chain(AtomicDB(leveldb_20))
    assert chain.chaindb.get_canonical_head().block_number == 20
    return chain.chaindb


@pytest.fixture
def chaindb_fresh():
    chain = load_mining_chain(AtomicDB())
    assert chain.chaindb.get_canonical_head().block_number == 0
    return chain.chaindb
