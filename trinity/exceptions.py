import pathlib

from p2p.exceptions import HandshakeFailure
from p2p.tracking.connection import register_error

from trinity.constants import BLACKLIST_SECONDS_WRONG_NETWORK_OR_GENESIS


class BaseTrinityError(Exception):
    """
    The base class for all Trinity errors.
    """
    pass


class AmbigiousFileSystem(BaseTrinityError):
    """
    Raised when the file system paths are unclear
    """
    pass


class MissingPath(BaseTrinityError):
    """
    Raised when an expected path is missing
    """
    def __init__(self, msg: str, path: pathlib.Path) -> None:
        super().__init__(msg)
        self.path = path


class SyncRequestAlreadyProcessed(BaseTrinityError):
    """
    Raised when a trie SyncRequest has already been processed.
    """
    pass


class OversizeObject(BaseTrinityError):
    """
    Raised when an object is bigger than comfortably fits in memory.
    """
    pass


class DAOForkCheckFailure(BaseTrinityError):
    """
    Raised when the DAO fork check with a certain peer is unsuccessful.
    """
    pass


class BadDatabaseError(BaseTrinityError):
    """
    The local network database is not in the expected format
     - empty
     - wrong schema version
     - missing tables
    """
    pass


class AttestationNotFound(BaseTrinityError):
    """
    Raised when attestion with given attestation root does not exist.
    """
    pass


class WrongNetworkFailure(HandshakeFailure):
    """
    Disconnected from the peer because it's on a different network than we're on
    """
    pass


register_error(WrongNetworkFailure, BLACKLIST_SECONDS_WRONG_NETWORK_OR_GENESIS)


class WrongForkIDFailure(HandshakeFailure):
    """
    Disconnected from the peer because it has an incompatible ForkID
    """
    pass


register_error(WrongForkIDFailure, BLACKLIST_SECONDS_WRONG_NETWORK_OR_GENESIS)


class WrongGenesisFailure(HandshakeFailure):
    """
    Disconnected from the peer because it has a different genesis than we do
    """
    pass


register_error(WrongGenesisFailure, BLACKLIST_SECONDS_WRONG_NETWORK_OR_GENESIS)


class RpcError(BaseTrinityError):
    """
    Raised when a JSON-RPC API request can not be fulfilled.
    """
    pass


class BaseForkIDValidationError(BaseTrinityError):
    """
    Base class for all ForkID validation errors.

    """
    pass


class RemoteChainIsStale(BaseForkIDValidationError):
    """
    Raised when a remote fork ID is a subset of our already applied forks, but the announced next
    fork block is not on our already passed chain.
    """
    pass


class LocalChainIncompatibleOrStale(BaseForkIDValidationError):
    """
    Raised when a remote fork ID does not match any local checksum variation, signalling that the
    two chains have diverged in the past at some point (possibly at genesis).
    """
    pass


class StateUnretrievable(BaseTrinityError):
    """
    Raised when state is missing locally, and cannot be retrieved from peers either, because
    the peers have pruned their state tries.
    """
    pass


class ENRMissingForkID(BaseTrinityError):
    """
    Raised when a peer sends us an ENR without a ForkID.
    """
    pass
