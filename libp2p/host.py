import asyncio
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Sequence,
    Tuple,
)

from multiaddr import (
    Multiaddr,
)

from .p2pclient.datastructures import (
    PeerID,
    PeerInfo,
    StreamInfo,
)
from .p2pclient.p2pclient import (
    ControlClient,
    StreamHandler,
)


# TODO: combine host with network?

class BaseHost(ABC):
    """
    Reference:
        - py-libp2p: https://github.com/libp2p/py-libp2p/blob/master/libp2p/host/host_interface.py
    """

    @abstractmethod
    def get_id(self) -> PeerID:
        pass

    @abstractmethod
    def get_addrs(self) -> Sequence[Multiaddr]:
        pass

    @abstractmethod
    def new_stream(
            self,
            peer_id: PeerID,
            protocol_ids: Sequence[str]) -> Tuple[
            StreamInfo, asyncio.StreamReader, asyncio.StreamWriter]:
        pass

    @abstractmethod
    def connect(self, peer_info: PeerInfo) -> None:
        pass

    @abstractmethod
    def list_peers(self) -> Tuple[PeerInfo, ...]:
        pass

    @abstractmethod
    def disconnect(self, peer_id: PeerID) -> None:
        pass

    @abstractmethod
    def set_stream_handler(self, protocol_id: str, stream_handler: StreamHandler) -> None:
        pass

    # ignore network and mux now
    # reference: https://github.com/libp2p/py-libp2p/blob/master/libp2p/network/network_interface.py  # noqa: E501
    # @abstractmethod
    # def get_network(self):
    #     pass

    # @abstractmethod
    # def get_mux(self):
    #     pass


# TODO: leave the exceptions handling for now
#   we need uniform exceptions to work with both `DaemonHost` and `Libp2p.Host`

class DaemonHost(BaseHost):
    """
    Implement host with libp2p daemon bindings
    """
    control_client: ControlClient
    peer_info: PeerInfo = None

    def __init__(self, control_client: ControlClient):
        self.control_client = control_client

    async def get_peer_info(self):
        if self.peer_info is None:
            peer_id, maddrs = await self.control_client.identify()
            self.peer_info = PeerInfo(peer_id, maddrs)
        return self.peer_info

    async def get_id(self) -> PeerID:
        peer_info = await self.get_peer_info()
        return peer_info.peer_id

    async def get_addrs(self) -> Sequence[Multiaddr]:
        peer_info = await self.get_peer_info()
        return peer_info.addrs

    async def new_stream(
            self,
            peer_id: PeerID,
            protocol_ids: Sequence[str]) -> Tuple[
            StreamInfo, asyncio.StreamReader, asyncio.StreamWriter]:
        return await self.control_client.stream_open(
            peer_id=peer_id,
            protocols=protocol_ids,
        )

    async def connect(self, peer_info: PeerInfo) -> None:
        await self.control_client.connect(
            peer_id=peer_info.peer_id,
            maddrs=peer_info.addrs,
        )

    async def list_peers(self) -> Tuple[PeerInfo, ...]:
        return await self.control_client.list_peers()

    async def disconnect(self, peer_id: PeerID) -> None:
        await self.control_client.disconnect(peer_id=peer_id)

    async def set_stream_handler(self, protocol_id: str, stream_handler: StreamHandler) -> None:
        await self.control_client.stream_handler(
            proto=protocol_id,
            handler_cb=stream_handler,
        )
