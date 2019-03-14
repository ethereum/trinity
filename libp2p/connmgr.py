from abc import (
    ABC,
    abstractmethod,
)

from .p2pclient.datastructures import (
    PeerID,
)
from .p2pclient.p2pclient import (
    ConnectionManagerClient,
)


class BaseConnectionManager(ABC):
    """
    Reference: https://github.com/libp2p/go-libp2p-connmgr/blob/master/connmgr.go
    """

    @abstractmethod
    def tag_peer(self, peer_id: PeerID, tag: str, weight: int) -> None:
        pass

    @abstractmethod
    async def untag_peer(self, peer_id: PeerID, tag: str) -> None:
        pass

    @abstractmethod
    def trim(self) -> None:
        pass

    # @abstractmethod
    # def get_tag_info(self, peer_id):
    #     """
    #     Fetch the tag information associated with a given peer, nil is returned if p refers to
    #     an unknown peer.
    #     """
    #     pass

    # @abstractmethod
    # def get_info(self):
    #     """
    #     returns the configuration and status data for this connection manager.
    #     """
    #     pass


class DaemonConnectionManager(BaseConnectionManager):

    connmgr_client: ConnectionManagerClient

    def __init__(self, connmgr_client: ConnectionManagerClient):
        self.connmgr_client = connmgr_client

    async def tag_peer(self, peer_id: PeerID, tag: str, weight: int) -> None:
        await self.connmgr_client.tag_peer(peer_id, tag, weight)

    async def untag_peer(self, peer_id: PeerID, tag: str) -> None:
        await self.connmgr_client.untag_peer(peer_id, tag)

    async def trim(self) -> None:
        await self.connmgr_client.trim()
