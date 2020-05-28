from typing import (
    Any
)


class BaseP2PError(Exception):
    """
    The base class for all p2p errors.
    """
    pass


class DecryptionError(BaseP2PError):
    """
    Raised when a message could not be decrypted.
    """
    pass


class PeerConnectionLost(BaseP2PError):
    """
    Raised when the connection to a peer was lost.
    """
    pass


class ConnectionBusy(BaseP2PError):
    """
    Raised when an attempt is made to wait for a certain message type from a
    peer when there is already an active wait for that message type.
    """
    pass


class IneligiblePeer(BaseP2PError):
    """
    Raised when a peer is not a valid connection candidate.
    """
    pass


class HandshakeFailure(BaseP2PError):
    """
    Raised when the protocol handshake was unsuccessful.
    """
    pass


class HandshakeFailureTooManyPeers(HandshakeFailure):
    """
    The remote disconnected from us during a handshake because it has too many peers
    """
    pass


class MalformedMessage(BaseP2PError):
    """
    Raised when a p2p command is received with a malformed message
    """
    pass


class UnknownProtocol(BaseP2PError):
    """
    Raised in cases where a given protocol is not present such as not part of a
    Multiplexer connection.
    """
    pass


class UnknownProtocolCommand(BaseP2PError):
    """
    Raised when the received protocal command isn't known.
    """
    pass


class UnexpectedMessage(BaseP2PError):
    """
    Raised when the received message was unexpected.
    """
    pass


class UnreachablePeer(BaseP2PError):
    """
    Raised when a peer was unreachable.
    """
    pass


class EmptyGetBlockHeadersReply(BaseP2PError):
    """
    Raised when the received block headers were empty.
    """
    pass


class LESAnnouncementProcessingError(BaseP2PError):
    """
    Raised when an LES announcement could not be processed.
    """
    pass


class TooManyTimeouts(BaseP2PError):
    """
    Raised when too many timeouts occurred.
    """
    pass


class NoConnectedPeers(BaseP2PError):
    """
    Raised when we are not connected to any peers.
    """
    pass


class NoEligiblePeers(BaseP2PError):
    """
    Raised when none of our peers have the data we want.
    """
    pass


class NoIdlePeers(BaseP2PError):
    """
    Raised when none of our peers is idle and can be used for data requests.
    """
    pass


class EventLoopMismatch(BaseP2PError):
    """
    Raised when two different asyncio event loops are referenced, but must be equal
    """
    pass


class NoEligibleNodes(BaseP2PError):
    """
    Raised when there are no nodes which meet some filter criteria
    """
    pass


class BadAckMessage(BaseP2PError):
    """
    Raised when the ack message during a peer handshake is malformed
    """
    pass


class BadLESResponse(BaseP2PError):
    """
    Raised when the response to a LES request doesn't contain the data we asked for.

    The peer can be treated as violating protocol. Often, the repurcussion should be
    disconnection and blacklisting.
    """
    pass


class NoInternalAddressMatchesDevice(BaseP2PError):
    """
    Raised when no internal IP address matches the UPnP device that is being configured.
    """
    def __init__(self, *args: Any, device_hostname: str = None) -> None:
        super().__init__(*args)
        self.device_hostname = device_hostname


class AlreadyWaitingDiscoveryResponse(BaseP2PError):
    """
    Raised when we are already waiting for a discovery response from a given remote.
    """
    pass


class NoMatchingPeerCapabilities(BaseP2PError):
    """
    Raised during primary p2p handshake if there are no common capabilities with a peer.
    """
    pass


class ReceiptNotFound(BaseP2PError):
    """
    Raised when trying to retrieve a protocol receipt that isn't available
    """
    pass


class DuplicateAPI(BaseP2PError):
    """
    Raised when trying to add an API to a connection under an existing key
    """
    pass


class UnknownAPI(BaseP2PError):
    """
    Raised when trying to retrieve an API from a connection that.
    """
    pass


class CouldNotRetrieveENR(BaseP2PError):
    """
    Raised when we cannot get an ENR from a remote node.
    """
    pass


class PeerReporterRegistryError(BaseP2PError):
    """
    Raised when there is an error assigning or unassigning peers in the PeerReporterRegistry.
    """
    pass
