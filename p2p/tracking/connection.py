from abc import ABC, abstractmethod
import asyncio
from typing import (
    Dict,
    Tuple,
    Type,
    Union,
)

from eth_typing import NodeID
from eth_utils.logging import get_extended_debug_logger

from p2p.abc import NodeAPI
from p2p.exceptions import (
    BaseP2PError,
    HandshakeFailure,
    HandshakeFailureTooManyPeers,
    MalformedMessage,
    NoMatchingPeerCapabilities,
    PeerConnectionLost,
    UnreachablePeer,
)


FAILURE_TIMEOUTS: Dict[Type[Exception], int] = {}


def register_error(exception: Union[Type[BaseP2PError], Type[asyncio.TimeoutError]],
                   timeout_seconds: int) -> None:
    if exception in FAILURE_TIMEOUTS:
        raise KeyError(f"Exception class already registered")
    FAILURE_TIMEOUTS[exception] = timeout_seconds


# Any timeout on the order of seconds makes no sense as we'll effectively end up retrying the
# blacklisted candidate before we even had a chance of trying new ones.
register_error(HandshakeFailure, 600)
register_error(PeerConnectionLost, 600)
register_error(UnreachablePeer, 600)
register_error(asyncio.TimeoutError, 600)
register_error(HandshakeFailureTooManyPeers, 600)
# A MalformedMessage is usually not a transient issue, so blacklist the remote for a long time.
register_error(MalformedMessage, 60 * 60)
# We (and our peer) are unlikely to grow new capabilities unless restarted, so no point in
# retrying those frequently.
register_error(NoMatchingPeerCapabilities, 60 * 60)


def get_timeout_for_failure(failure: BaseP2PError) -> int:
    for cls in type(failure).__mro__:
        if cls in FAILURE_TIMEOUTS:
            return FAILURE_TIMEOUTS[cls]
    failure_name = type(failure).__name__
    raise Exception(f'Unknown failure type: {failure_name}')


class BaseConnectionTracker(ABC):
    """
    Base API which defines the interface that the peer pool uses to record
    information about connection failures when attempting to connect to peers
    """
    logger = get_extended_debug_logger('p2p.tracking.connection.ConnectionTracker')

    def record_failure(self, remote: NodeAPI, failure: BaseP2PError) -> None:
        timeout_seconds = get_timeout_for_failure(failure)
        failure_name = type(failure).__name__

        return self.record_blacklist(remote, timeout_seconds, failure_name)

    @abstractmethod
    def record_blacklist(self, remote: NodeAPI, timeout_seconds: int, reason: str) -> None:
        ...

    @abstractmethod
    async def get_blacklisted(self) -> Tuple[NodeID, ...]:
        ...


class NoopConnectionTracker(BaseConnectionTracker):
    def record_blacklist(self, remote: NodeAPI, timeout_seconds: int, reason: str) -> None:
        pass

    async def get_blacklisted(self) -> Tuple[NodeID, ...]:
        return tuple()
