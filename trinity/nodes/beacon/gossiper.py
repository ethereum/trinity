from enum import Enum, unique
from typing import AsyncIterable, Callable

from libp2p.host.host_interface import IHost
from libp2p.peer.id import ID
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.gossipsub import PROTOCOL_ID as GOSSIPSUB_PROTOCOL_ID
from libp2p.pubsub.pb.rpc_pb2 import Message as GossipMessage
from libp2p.pubsub.pubsub import Pubsub, get_content_addressed_msg_id
from libp2p.pubsub.subscription import ISubscriptionAPI
import snappy
import ssz

from eth2.beacon.types.blocks import SignedBeaconBlock
from eth2.beacon.typing import ForkDigest

GossipValidator = Callable[[ID, GossipMessage], bool]
ForkDigestProvider = Callable[[], ForkDigest]


def _serialize_gossip(operation: ssz.Serializable) -> bytes:
    encoded_operation = ssz.encode(operation)
    return snappy.compress(encoded_operation)


def _deserialize_gossip(
    compressed_data: bytes, sedes: ssz.BaseSedes
) -> ssz.Serializable:
    data = snappy.decompress(compressed_data)
    return ssz.decode(data, sedes)


@unique
class PubSubTopic(Enum):
    beacon_block: str = "beacon_block"
    attestation: str = "beacon_attestation"


class Gossiper:
    GOSSIP_MAX_SIZE: int = 2 ** 20
    MAXIMUM_GOSSIP_CLOCK_DISPARITY = 0.5  # seconds

    # `D` (topic stable mesh target count)
    DEGREE: int = 6
    # `D_low` (topic stable mesh low watermark)
    DEGREE_LOW: int = 4
    # `D_high` (topic stable mesh high watermark)
    DEGREE_HIGH: int = 12
    # `D_lazy` (gossip target)
    DEGREE_LAZY: int = 6
    # `fanout_ttl` (ttl for fanout maps for topics we are not subscribed to
    #   but have published to seconds).
    FANOUT_TTL: int = 60
    # `gossip_advertise` (number of windows to gossip about).
    GOSSIP_WINDOW: int = 3
    # `gossip_history` (number of heartbeat intervals to retain message IDs).
    GOSSIP_HISTORY: int = 5
    # `heartbeat_interval` (frequency of heartbeat, seconds).
    HEARTBEAT_INTERVAL: int = 1  # seconds

    ATTESTATION_SUBNET_COUNT = 64
    ATTESTATION_PROPAGATION_SLOT_RANGE = 32

    def __init__(self, fork_digest_provider: ForkDigestProvider, host: IHost) -> None:
        self._fork_digest_provider = fork_digest_provider
        self._host = host
        gossipsub_router = GossipSub(
            protocols=[GOSSIPSUB_PROTOCOL_ID],
            degree=self.DEGREE,
            degree_low=self.DEGREE_LOW,
            degree_high=self.DEGREE_HIGH,
            time_to_live=self.FANOUT_TTL,
            gossip_window=self.GOSSIP_WINDOW,
            gossip_history=self.GOSSIP_HISTORY,
            heartbeat_interval=self.HEARTBEAT_INTERVAL,
        )
        self.gossipsub = gossipsub_router
        self.pubsub = Pubsub(
            host=self._host,
            router=gossipsub_router,
            msg_id_constructor=get_content_addressed_msg_id,
        )

    async def _subscribe_to_gossip_topic(
        self, topic: str, validator: GossipValidator
    ) -> ISubscriptionAPI:
        self.pubsub.set_topic_validator(topic, validator, False)
        return await self.pubsub.subscribe(topic)

    def _gossip_topic_id_for(self, topic: PubSubTopic) -> str:
        return f"/eth2/{self._fork_digest_provider().hex()}/{topic.value}/ssz_snappy"

    async def subscribe_gossip_channels(self) -> None:
        # TODO handle concurrently
        self._block_gossip = await self._subscribe_to_gossip_topic(
            self._gossip_topic_id_for(PubSubTopic.beacon_block),
            validator=_validate_beacon_block_gossip(self),
        )
        self._attestation_gossip = await self._subscribe_to_gossip_topic(
            self._gossip_topic_id_for(PubSubTopic.attestation),
            validator=_validate_attestation_gossip(self),
        )

    async def unsubscribe_gossip_channels(self) -> None:
        # TODO handle concurrently
        await self.pubsub.unsubscribe(
            self._gossip_topic_id_for(PubSubTopic.beacon_block)
        )
        await self.pubsub.unsubscribe(
            self._gossip_topic_id_for(PubSubTopic.attestation)
        )

    async def stream_block_gossip(self) -> AsyncIterable[SignedBeaconBlock]:
        async with self._block_gossip:
            async for block_message in self._block_gossip:
                if block_message.from_id == self.pubsub.my_id:
                    # FIXME: this check should happen inside `py-libp2p`
                    continue
                # TODO: validate block further at the p2p layer?
                block_data = block_message.data
                yield _deserialize_gossip(block_data, SignedBeaconBlock)

    async def broadcast_block(self, block: SignedBeaconBlock) -> None:
        gossip = _serialize_gossip(block)
        await self.pubsub.publish(
            self._gossip_topic_id_for(PubSubTopic.beacon_block), gossip
        )


def _always_success_validator(peer_id: ID, message: GossipMessage) -> bool:
    return True


def _validate_beacon_block_gossip(host: Gossiper) -> GossipValidator:
    # TODO: actual validations
    return _always_success_validator


def _validate_attestation_gossip(host: Gossiper) -> GossipValidator:
    # TODO: actual validations
    return _always_success_validator
