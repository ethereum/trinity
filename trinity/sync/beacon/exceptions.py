from trinity.exceptions import BaseTrinityError


class LeadingPeerNotFonud(BaseTrinityError):
    """
    Raised when unable to find a peer to sync
    """
    ...
