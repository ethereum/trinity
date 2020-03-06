from typing import (
    NamedTuple,
    NewType,
    Optional,
    TYPE_CHECKING,
)

from eth_typing import (
    Hash32,
)

from p2p.typing import SessionKeys

if TYPE_CHECKING:
    from p2p.enr import (  # noqa: F401
        ENR,
    )
    from p2p.discv5.messages import (  # noqa: F401
        BaseMessage,
    )
    from p2p.discv5.packets import (  # noqa: F401
        AuthHeaderPacket,
    )

Tag = NewType("Tag", bytes)

Topic = NewType("Topic", Hash32)


class HandshakeResult(NamedTuple):
    session_keys: SessionKeys
    enr: Optional["ENR"]
    message: Optional["BaseMessage"]
    auth_header_packet: Optional["AuthHeaderPacket"]
