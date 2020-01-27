from dataclasses import (
    dataclass,
)
from typing import (
    Callable,
    Tuple,
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from p2p.abc import NodeAPI


class BaseDiscoveryServiceResponse(BaseEvent):
    pass


@dataclass
class PeerCandidatesResponse(BaseDiscoveryServiceResponse):

    candidates: Tuple[NodeAPI, ...]


class PeerCandidatesRequest(BaseRequestResponseEvent[PeerCandidatesResponse]):

    def __init__(self, max_candidates: int, should_skip_fn: Callable[[NodeAPI], bool]) -> None:
        self.max_candidates = max_candidates
        self.should_skip_fn = should_skip_fn

    @staticmethod
    def expected_response_type() -> Type[PeerCandidatesResponse]:
        return PeerCandidatesResponse


class RandomBootnodeRequest(BaseRequestResponseEvent[PeerCandidatesResponse]):

    @staticmethod
    def expected_response_type() -> Type[PeerCandidatesResponse]:
        return PeerCandidatesResponse
