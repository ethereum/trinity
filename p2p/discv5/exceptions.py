from p2p.exceptions import (
    BaseP2PError,
)


class DiscV5Error(BaseP2PError):
    pass


class InvalidResponse(DiscV5Error):
    pass
