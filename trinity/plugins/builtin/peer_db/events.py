from typing import Type, Tuple

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from eth_typing import Hash32

from p2p.kademlia import Node


class BasePeerDBEvent(BaseEvent):
    pass


class TrackPeerEvent(BasePeerDBEvent):
    def __init__(self, remote: Node, is_outbound: bool):
        self.remote = remote
        self.is_outbound = is_outbound


class TrackPeerMetaEvent(BasePeerDBEvent):
    def __init__(self,
                 remote: Node,
                 genesis_hash: Hash32 = None,
                 protocol: str = None,
                 protocol_version: int = None,
                 network_id: int = None) -> None:
        self.remote = remote
        self.genesis_hash = genesis_hash
        self.protocol = protocol
        self.protocol_version = protocol_version
        self.network_id = network_id


class GetPeerCandidatesResponse(BasePeerDBEvent):
    def __init__(self, candidates: Tuple[Node, ...]) -> None:
        self.candidates = candidates


class GetPeerCandidatesRequest(BaseRequestResponseEvent[GetPeerCandidatesResponse]):
    def __init__(self, num_requested: int, num_connected_peers: int) -> None:
        self.num_requested = num_requested
        self.num_connected_peers = num_connected_peers

    @staticmethod
    def expected_response_type() -> Type[GetPeerCandidatesResponse]:
        return GetPeerCandidatesResponse
