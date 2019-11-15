from typing import Tuple, cast

from cached_property import cached_property

from p2p.abc import BehaviorAPI, HandshakerAPI
from trinity.protocol.eth.handshaker import ETHHandshaker
from trinity.protocol.eth.payloads import StatusPayload as ETHStatusPayload
from trinity.protocol.eth.peer import ETHPeer, ETHPeerFactory, ETHPeerPool
from trinity.protocol.eth.proto import ETHProtocol

from .api import FirehoseAPI
from .handshaker import FirehoseHandshaker
from .proto import FirehoseHandshakeParams, FirehoseProtocol


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
        handshakers = await super().get_handshakers()

        # rather than regenerate these values, look them up from the eth handshaker
        eth_handshaker = cast(ETHHandshaker, handshakers[-1])
        eth_params = eth_handshaker.handshake_params

        if not isinstance(eth_params, ETHStatusPayload):
            raise TypeError(
                "Expected eth handshake parameters to be the last handshaker. "
                f"Got: {type(eth_params)}"
            )

        handshake_params = FirehoseHandshakeParams(
            version=FirehoseProtocol.version,
            network_id=eth_params.network_id,
            genesis_hash=eth_params.genesis_hash,
        )
        return handshakers + (
            FirehoseHandshaker(handshake_params),
        )


# TODO: Maybe it's okay to keep using the ETHPeerPoolEventServer? Didn't create a Firehose one, yet


class FirehosePeerPool(ETHPeerPool):
    peer_factory_class = FirehosePeerFactory


# TODO: Maybe it's okay to keep using the ETHProxyPeerPool? Didn't create a Firehose one, yet
