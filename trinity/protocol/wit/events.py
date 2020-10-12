from dataclasses import (
    dataclass,
)
from typing import (
    Sequence,
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from eth_typing import (
    Hash32,
)

from p2p.abc import SessionAPI

from trinity.protocol.common.events import PeerPoolMessageEvent
from trinity.protocol.wit.commands import (
    BlockWitnessHashes,
    GetBlockWitnessHashes,
)


class BlockWitnessHashesEvent(PeerPoolMessageEvent):
    command: BlockWitnessHashes


class GetBlockWitnessHashesEvent(PeerPoolMessageEvent):
    command: GetBlockWitnessHashes


@dataclass
class SendBlockWitnessHashesEvent(PeerPoolMessageEvent):
    command: BlockWitnessHashes
    block_hash: Hash32


@dataclass
class GetBlockWitnessHashesResponse(BaseEvent):
    node_hashes: Sequence[Hash32]
    error: Exception = None


@dataclass
class GetBlockWitnessHashesRequest(BaseRequestResponseEvent[GetBlockWitnessHashesResponse]):
    session: SessionAPI
    block_hash: Hash32
    timeout: float

    @staticmethod
    def expected_response_type() -> Type[GetBlockWitnessHashesResponse]:
        return GetBlockWitnessHashesResponse
