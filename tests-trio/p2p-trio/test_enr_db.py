import pathlib

import pytest

from p2p.discv5.enr import ENR
from p2p.discv5.enr_db import (
    FileEnrDb,
    MemoryEnrDb,
    get_enr_filename,
)
from p2p.discv5.identity_schemes import (
    default_identity_scheme_registry,
    IdentitySchemeRegistry,
)

from p2p.tools.factories.discovery import (
    ENRFactory,
)
from p2p.tools.factories.keys import (
    PrivateKeyFactory,
)


@pytest.fixture
def memory_db():
    return MemoryEnrDb(default_identity_scheme_registry)


@pytest.fixture
def file_db_dir(tmp_path):
    return pathlib.Path(tmp_path)


@pytest.fixture
def file_db(file_db_dir):
    return FileEnrDb(default_identity_scheme_registry, file_db_dir)


@pytest.fixture(params=["file_db", "memory_db"])
def db(request):
    return request.getfixturevalue(request.param)


@pytest.mark.trio
async def test_insert(db):
    private_key = PrivateKeyFactory().to_bytes()
    enr = ENRFactory(private_key=private_key)

    await db.insert(enr)
    assert await db.contains(enr.node_id)
    assert await db.get(enr.node_id) == enr

    with pytest.raises(ValueError):
        await db.insert(enr)

    updated_enr = ENRFactory(private_key=private_key, sequence_number=enr.sequence_number + 1)
    with pytest.raises(ValueError):
        await db.insert(updated_enr)


@pytest.mark.trio
async def test_update(db):
    private_key = PrivateKeyFactory().to_bytes()
    enr = ENRFactory(private_key=private_key)

    with pytest.raises(KeyError):
        await db.update(enr)

    await db.insert(enr)

    await db.update(enr)
    assert await db.get(enr.node_id) == enr

    updated_enr = ENRFactory(private_key=private_key, sequence_number=enr.sequence_number + 1)
    await db.update(updated_enr)
    assert await db.get(enr.node_id) == updated_enr


@pytest.mark.trio
async def test_insert_or_update(db):
    private_key = PrivateKeyFactory().to_bytes()
    enr = ENRFactory(private_key=private_key)

    await db.insert_or_update(enr)
    assert await db.get(enr.node_id) == enr

    await db.insert_or_update(enr)
    assert await db.get(enr.node_id) == enr

    updated_enr = ENRFactory(private_key=private_key, sequence_number=enr.sequence_number + 1)
    await db.insert_or_update(updated_enr)
    assert await db.get(enr.node_id) == updated_enr


@pytest.mark.trio
async def test_memory_remove(memory_db):
    enr = ENRFactory()

    with pytest.raises(KeyError):
        await memory_db.remove(enr.node_id)

    await memory_db.insert(enr)
    await memory_db.remove(enr.node_id)

    assert not await memory_db.contains(enr.node_id)


@pytest.mark.trio
async def test_get(db):
    enr = ENRFactory()

    with pytest.raises(KeyError):
        await db.get(enr.node_id)

    await db.insert(enr)
    assert await db.get(enr.node_id) == enr


@pytest.mark.trio
async def test_contains(db):
    enr = ENRFactory()

    assert not await db.contains(enr.node_id)
    await db.insert(enr)
    assert await db.contains(enr.node_id)


@pytest.mark.trio
async def test_memory_checks_identity_scheme():
    empty_identity_scheme_registry = IdentitySchemeRegistry()
    memory_db = MemoryEnrDb(empty_identity_scheme_registry)
    enr = ENRFactory()

    with pytest.raises(ValueError):
        await memory_db.insert(enr)
    with pytest.raises(ValueError):
        await memory_db.insert_or_update(enr)


@pytest.mark.trio
async def test_file_db_loads_existing_enrs(file_db_dir):
    enr = ENRFactory()
    filename = get_enr_filename(enr)
    (file_db_dir / filename).write_text(repr(enr))
    file_db = FileEnrDb(default_identity_scheme_registry, file_db_dir)
    assert await file_db.get(enr.node_id) == enr


@pytest.mark.trio
async def test_file_db_ignores_bad_names(file_db_dir):
    enr = ENRFactory()
    filename = "not_a_valid_enr_name"
    (file_db_dir / filename).write_text(repr(enr))
    file_db = FileEnrDb(default_identity_scheme_registry, file_db_dir)
    assert not await file_db.contains(enr.node_id)


@pytest.mark.trio
async def test_file_db_ignores_invalid_enrs(file_db_dir):
    enr = ENRFactory()
    filename = get_enr_filename(enr)
    (file_db_dir / filename).write_text("invalid_encoding")
    file_db = FileEnrDb(default_identity_scheme_registry, file_db_dir)
    assert not await file_db.contains(enr.node_id)


@pytest.mark.trio
async def test_file_db_saves_enrs(file_db_dir, file_db):
    enr = ENRFactory()
    await file_db.insert(enr)
    filename = get_enr_filename(enr)
    assert (file_db_dir / filename).exists()
    assert (file_db_dir / filename).is_file()
    assert ENR.from_repr((file_db_dir / filename).read_text()) == enr
