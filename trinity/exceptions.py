import pathlib

from p2p.exceptions import HandshakeFailure
from p2p.tracking.connection import register_error


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


class AlreadyWaiting(BaseTrinityError):
    """
    Raised when an attempt is made to wait for a certain message type from a
    peer when there is already an active wait for that message type.
    """
    pass


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


class WrongNetworkFailure(BaseTrinityError, HandshakeFailure):
    """
    Disconnected from the peer because it's on a different network than we're on
    """
    pass


class WrongGenesisFailure(BaseTrinityError, HandshakeFailure):
    """
    Disconnected from the peer because it has a different genesis than we do
    """
    pass


register_error(WrongNetworkFailure, 24 * 60 * 60)  # one day
register_error(WrongGenesisFailure, 24 * 60 * 60)  # one day
