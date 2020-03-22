from trinity.db.manager import (
    DBManager,
    DBClient,
)


def test_db_manager_lifecycle(base_db, ipc_path):
    manager = DBManager(base_db)

    assert not manager.is_running
    assert not manager.is_stopped

    with manager.run(ipc_path):
        assert manager.is_running
        assert not manager.is_stopped

    assert not manager.is_running
    assert manager.is_stopped


def test_db_manager_lifecycle_with_connections(base_db, ipc_path):
    manager = DBManager(base_db)

    assert not manager.is_running
    assert not manager.is_stopped

    with manager.run(ipc_path):
        assert manager.is_running
        assert not manager.is_stopped

        with DBClient.connect(ipc_path), DBClient.connect(ipc_path):
            assert manager.is_running
            assert not manager.is_stopped

    assert not manager.is_running
    assert manager.is_stopped
