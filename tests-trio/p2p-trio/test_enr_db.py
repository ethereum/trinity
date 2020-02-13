import pathlib
import socket

import pytest

from p2p.discv5.constants import IP_V4_ADDRESS_ENR_KEY
from p2p.discv5.enr_db import (
    FileNodeDB,
    MemoryNodeDB,
    get_node_filename,
)
from p2p.discv5.enr import ENR
from p2p.discv5.identity_schemes import (
    default_identity_scheme_registry,
    IdentitySchemeRegistry,
)

from p2p.kademlia import Node

from p2p.tools.factories.discovery import (
    ENRFactory,
)
from p2p.tools.factories.kademlia import IPAddressFactory
from p2p.tools.factories.keys import (
    PrivateKeyFactory,
)


@pytest.fixture
def memory_db():
    return MemoryNodeDB(default_identity_scheme_registry)


@pytest.fixture
def file_db_dir(tmp_path):
    return pathlib.Path(tmp_path)


@pytest.fixture
def file_db(file_db_dir):
    return FileNodeDB(default_identity_scheme_registry, file_db_dir)


@pytest.fixture(params=["file_db", "memory_db"])
def db(request):
    return request.getfixturevalue(request.param)


@pytest.mark.trio
async def test_insert(db):
    private_key = PrivateKeyFactory().to_bytes()
    node = Node(ENRFactory(private_key=private_key))

    await db.insert(node)
    assert await db.contains(node.id)
    db_node = await db.get(node.id)
    assert db_node == node
    assert db_node.enr == node.enr

    with pytest.raises(ValueError):
        await db.insert(node)

    updated_enr = ENRFactory(private_key=private_key, sequence_number=node.enr.sequence_number + 1)
    with pytest.raises(ValueError):
        await db.insert(Node(updated_enr))


@pytest.mark.trio
async def test_update(db):
    private_key = PrivateKeyFactory().to_bytes()
    node = Node(ENRFactory(private_key=private_key))

    with pytest.raises(KeyError):
        await db.update(node)

    await db.insert(node)

    await db.update(node)
    db_node = await db.get(node.id)
    assert db_node == node
    assert db_node.enr == node.enr

    updated_enr = ENRFactory(private_key=private_key, sequence_number=node.enr.sequence_number + 1)
    await db.update(Node(updated_enr))
    db_updated_node = await db.get(node.id)
    assert db_updated_node.enr == updated_enr


@pytest.mark.trio
async def test_update_with_stub_enr(db):
    # Not all nodes support the ENR extension in discv4, so internally we store an ENR with
    # sequence_number 0 for those. Normally, the update() method is a no-op when we pass it an ENR
    # with a sequence number that is not higher than the latest ENR we have for that node, but for
    # those stub ENRs we need update() to always override the current one with whatever it's
    # given.
    private_key = PrivateKeyFactory().to_bytes()
    enr = ENRFactory(
        private_key=private_key,
        sequence_number=0,
        custom_kv_pairs={
            IP_V4_ADDRESS_ENR_KEY: socket.inet_aton(IPAddressFactory.generate()),
        },
        signature=b''
    )
    node = Node(enr)
    await db.insert(node)

    await db.update(node)
    db_node = await db.get(node.id)
    assert db_node.enr == enr

    new_enr = ENRFactory(
        private_key=private_key,
        sequence_number=0,
        custom_kv_pairs={
            IP_V4_ADDRESS_ENR_KEY: socket.inet_aton(IPAddressFactory.generate()),
        },
        signature=b''
    )
    assert new_enr.node_id == enr.node_id
    assert new_enr != enr

    new_node = Node(new_enr)
    await db.update(new_node)

    new_db_node = await db.get(node.id)
    assert new_db_node.enr == new_enr


@pytest.mark.trio
async def test_insert_or_update(db):
    private_key = PrivateKeyFactory().to_bytes()
    node = Node(ENRFactory(private_key=private_key))

    await db.insert_or_update(node)
    db_node = await db.get(node.id)
    assert db_node.enr == node.enr
    assert db_node == node

    await db.insert_or_update(node)
    assert await db.get(node.id) == node

    updated_enr = ENRFactory(private_key=private_key, sequence_number=node.enr.sequence_number + 1)
    updated_node = Node(updated_enr)
    await db.insert_or_update(updated_node)
    db_updated_node = await db.get(node.id)
    assert db_updated_node.enr == updated_enr
    assert db_updated_node == updated_node


@pytest.mark.trio
async def test_memory_remove(memory_db):
    node = Node(ENRFactory())

    with pytest.raises(KeyError):
        await memory_db.remove(node.id)

    await memory_db.insert(node)
    await memory_db.remove(node.id)

    assert not await memory_db.contains(node.id)


@pytest.mark.trio
async def test_get(db):
    node = Node(ENRFactory())

    with pytest.raises(KeyError):
        await db.get(node.id)

    await db.insert(node)
    db_node = await db.get(node.id)
    assert db_node.enr == node.enr
    assert db_node == node


@pytest.mark.trio
async def test_contains(db):
    node = Node(ENRFactory())

    assert not await db.contains(node.id)
    await db.insert(node)
    assert await db.contains(node.id)


@pytest.mark.trio
async def test_memory_checks_identity_scheme():
    empty_identity_scheme_registry = IdentitySchemeRegistry()
    memory_db = MemoryNodeDB(empty_identity_scheme_registry)
    node = Node(ENRFactory())

    with pytest.raises(ValueError):
        await memory_db.insert(node)
    with pytest.raises(ValueError):
        await memory_db.insert_or_update(node)


@pytest.mark.trio
async def test_file_db_loads_existing_enrs(file_db_dir):
    node = Node(ENRFactory())
    filename = get_node_filename(node)
    (file_db_dir / filename).write_text(repr(node.enr))
    file_db = FileNodeDB(default_identity_scheme_registry, file_db_dir)
    db_node = await file_db.get(node.id)
    assert db_node.enr == node.enr
    assert db_node == node


@pytest.mark.trio
async def test_file_db_ignores_bad_names(file_db_dir):
    enr = ENRFactory()
    filename = "not_a_valid_enr_name"
    (file_db_dir / filename).write_text(repr(enr))
    file_db = FileNodeDB(default_identity_scheme_registry, file_db_dir)
    assert not await file_db.contains(enr.node_id)


@pytest.mark.trio
async def test_file_db_ignores_invalid_enrs(file_db_dir):
    node = Node(ENRFactory())
    filename = get_node_filename(node)
    (file_db_dir / filename).write_text("invalid_encoding")
    file_db = FileNodeDB(default_identity_scheme_registry, file_db_dir)
    assert not await file_db.contains(node.id)


@pytest.mark.trio
async def test_file_db_saves_enrs(file_db_dir, file_db):
    node = Node(ENRFactory())
    await file_db.insert(node)
    filename = get_node_filename(node)
    assert (file_db_dir / filename).exists()
    assert (file_db_dir / filename).is_file()
    loaded_enr = ENR.from_repr((file_db_dir / filename).read_text())
    assert loaded_enr == node.enr
