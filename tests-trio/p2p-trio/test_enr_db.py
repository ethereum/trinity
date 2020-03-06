import time

import pytest

from eth.db.backends.memory import MemoryDB

from p2p.node_db import NodeDB
from p2p.identity_schemes import (
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
def node_db():
    return NodeDB(default_identity_scheme_registry, MemoryDB())


def test_checks_identity_scheme():
    db = NodeDB(IdentitySchemeRegistry(), MemoryDB())
    enr = ENRFactory()

    with pytest.raises(ValueError):
        db.set_enr(enr)


def test_get_and_set_enr(node_db):
    private_key = PrivateKeyFactory().to_bytes()
    db = node_db
    enr = ENRFactory(private_key=private_key)

    with pytest.raises(KeyError):
        db.get_enr(enr.node_id)

    db.set_enr(enr)
    assert db.get_enr(enr.node_id) == enr

    updated_enr = ENRFactory(private_key=private_key, sequence_number=enr.sequence_number + 1)
    db.set_enr(updated_enr)
    assert db.get_enr(enr.node_id) == updated_enr

    with pytest.raises(ValueError):
        db.set_enr(enr)


def test_delete_enr(node_db):
    db = node_db
    enr = ENRFactory()

    with pytest.raises(KeyError):
        db.delete_enr(enr.node_id)

    db.set_enr(enr)
    db.delete_enr(enr.node_id)

    with pytest.raises(KeyError):
        db.get_enr(enr.node_id)


def test_get_and_set_last_pong_time(node_db):
    db = node_db
    enr = ENRFactory()

    with pytest.raises(KeyError):
        db.get_last_pong_time(enr.node_id)

    pong_time = int(time.monotonic())
    db.set_last_pong_time(enr.node_id, pong_time)

    assert db.get_last_pong_time(enr.node_id) == pong_time


def test_delete_last_pong_time(node_db):
    db = node_db
    enr = ENRFactory()

    with pytest.raises(KeyError):
        db.delete_last_pong_time(enr.node_id)

    pong_time = int(time.monotonic())
    db.set_last_pong_time(enr.node_id, pong_time)

    db.delete_last_pong_time(enr.node_id)

    with pytest.raises(KeyError):
        db.get_last_pong_time(enr.node_id)
