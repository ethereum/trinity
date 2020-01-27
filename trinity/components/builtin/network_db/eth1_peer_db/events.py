from dataclasses import (
    dataclass,
)
import datetime
from typing import Optional

from lahja import (
    BaseEvent,
)

from eth_typing import Hash32

from p2p.abc import NodeAPI


class BasePeerDBEvent(BaseEvent):
    pass


@dataclass
class TrackPeerEvent(BasePeerDBEvent):

    remote: NodeAPI
    is_outbound: bool
    last_connected_at: Optional[datetime.datetime]
    genesis_hash: Hash32
    protocol: str
    protocol_version: int
    network_id: int
