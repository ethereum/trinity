import asyncio
from typing import (
    Tuple,
)

from cached_property import cached_property

from lahja import EndpointAPI

from eth_typing import BlockNumber

from eth.constants import GENESIS_BLOCK_NUMBER
from eth.rlp.headers import BlockHeader
from lahja import (
    BroadcastConfig,
)

from p2p.abc import BehaviorAPI, CommandAPI, HandshakerAPI, SessionAPI
from p2p.exceptions import PeerConnectionLost
from p2p.protocol import (
    Payload,
)

from trinity._utils.decorators import (
    async_suppress_exceptions,
)
from trinity.protocol.common.peer import (
    BaseChainPeer,
    BaseChainPeerFactory,
    BaseChainPeerPool,
)
from trinity.protocol.common.peer_pool_event_bus import (
    BaseProxyPeer,
    BaseProxyPeerPool,
    PeerPoolEventServer,
)
from trinity.protocol.common.typing import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)
from trinity.protocol.eth.peer import (
    ETHPeer,
    ETHPeerFactory,
    ETHPeerPool,
)
from trinity.protocol.eth.proto import (
    ETHHandshakeParams,
    ETHProtocol,
)

from .api import FirehoseAPI
from .handshaker import FirehoseHandshaker
from .proto import FirehoseProtocol


class FirehosePeer(ETHPeer):
    supported_sub_protocols = (ETHProtocol, FirehoseProtocol)
    sub_proto: ETHProtocol = None

    def get_behaviors(self) -> Tuple[BehaviorAPI, ...]:
        return super().get_behaviors() + (FirehoseAPI().as_behavior(),)

    @cached_property
    def fh_api(self) -> FirehoseAPI:
        return self.connection.get_logic(FirehoseAPI.name, FirehoseAPI)


# TODO: Maybe it's okay to keep using the ETHProxyPeer? Didn't create a Firehose one, yet


class FirehosePeerFactory(ETHPeerFactory):
    peer_class = FirehosePeer

    async def get_handshakers(self) -> Tuple[HandshakerAPI, ...]:
        handshakers = super().get_handshakers()

        # rather than regenerate these values, look them up from the eth handshaker
        eth_params = handshakers[-1].handshake_params

        if not isinstance(eth_params, ETHHandshakeParams):
            raise TypeError(
                "Expected eth handshake parameters to be the last handshaker. "
                f"Got: {type(eth_params)}"
            )

        handshake_params = FirehoseHandshakeParams(
            head_hash=eth_params.head_hash,
            genesis_hash=eth_params.genesis_hash,
            network_id=eth_params.network_id,
            version=FirehoseProtocol.version,
        )
        return handshakers + (
            FirehoseHandshaker(handshake_params),
        )


# TODO: Maybe it's okay to keep using the ETHPeerPoolEventServer? Didn't create a Firehose one, yet


class FirehosePeerPool(ETHPeerPool):
    peer_factory_class = FirehosePeerFactory


# TODO: Maybe it's okay to keep using the ETHProxyPeerPool? Didn't create a Firehose one, yet
