from typing import (
    Any,
    Dict,
    Iterator,
)
import uuid

from p2p.abc import ConnectionAPI, ConnectionPoolAPI


class ConnectionPool(ConnectionPoolAPI):
    _connections: Dict[uuid.UUID, ConnectionAPI]

    def __init__(self) -> None:
        self._connections = {}

    def __len__(self) -> int:
        return len(self._connections)

    def __iter__(self) -> Iterator[ConnectionAPI]:
        for connection in self._connections.values():
            yield connection

    def __contains__(self, connection: Any) -> bool:
        return connection.session_id in self._connections

    def add(self, connection: ConnectionAPI) -> None:
        self._connections[connection.session_id] = connection

    def remove(self, connection: ConnectionAPI) -> None:
        del self._connections[connection.session_id]
