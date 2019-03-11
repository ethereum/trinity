from abc import (
    ABC,
    abstractmethod,
)
import asyncio
from typing import (
    Awaitable,
    Callable,
    Dict,
    Tuple,
)

from .p2pclient.datastructures import (
    PeerID,
)
from .p2pclient.p2pclient import (
    PubSubClient,
    read_pbmsg_safe,
)
from .p2pclient.pb import p2pd_pb2 as p2pd_pb


class BasePubSub(ABC):
    """
    Reference: https://github.com/libp2p/go-libp2p-pubsub/blob/master/pubsub.go
    """

    @abstractmethod
    def subscribe(self, topic: str):
        pass

    @abstractmethod
    def get_topics(self):
        pass

    @abstractmethod
    def publish(self, topic, data):
        pass

    @abstractmethod
    def list_peers(self, topic):
        pass

    # @abstractmethod
    # def register_topic_validator(self, topic, validator):
    #     pass

    # @abstractmethod
    # def unregister_topic_validator(self, topic):
    #     pass

    # @abstractmethod
    # def blacklist_peer(self, peer_id):
    #     pass


Validator = Callable[
    [PeerID, p2pd_pb.PSMessage],
    Awaitable[bool],
]


class DaemonPubSub(BasePubSub):
    """
    Implement pubsub with libp2p daemon pubsub
    """
    pubsub_client: PubSubClient
    _map_topic_stream: Dict[str, Tuple[asyncio.StreamReader, asyncio.StreamWriter]]
    _map_topic_task_listener: Dict[str, asyncio.Task]
    _validators: Dict[str, Validator]

    def __init__(self, pubsub_client: PubSubClient):
        self.pubsub_client = pubsub_client
        self._map_topic_stream = {}
        self._map_topic_task_listener = {}
        self._validators = {}

    def register_topic_validator(self, topic: str, validator: Validator):
        self._validators[topic] = validator

    async def subscribe(self, topic: str) -> None:
        if topic in self._map_topic_stream:
            raise ValueError(f"Topic {topic} has been subscribed. Unsubscribe it first.")
        reader, writer = await self.pubsub_client.subscribe(topic=topic)
        self._map_topic_stream[topic] = (reader, writer)
        self._map_topic_task_listener[topic] = asyncio.ensure_future(
            self._topic_listener(
                topic=topic,
                reader=reader,
                writer=writer,
            )
        )
        # yield to the `_topic_listener`
        await asyncio.sleep(0.001)

    async def unsubscribe(self, topic: str) -> None:
        if topic not in self._map_topic_stream:
            raise ValueError(f"Topic {topic} is not subscribed.")
        _, writer = self._map_topic_stream[topic]
        writer.close()
        task_listener = self._map_topic_task_listener[topic]
        task_listener.cancel()
        if not task_listener.cancelled() or not task_listener.done():
            # TODO: warn or raise?
            pass
        del self._map_topic_stream[topic]
        del self._map_topic_task_listener[topic]

    async def get_topics(self) -> Tuple[str, ...]:
        return await self.pubsub_client.get_topics()

    async def publish(self, topic: str, data: bytes) -> None:
        await self.pubsub_client.publish(topic=topic, data=data)

    async def list_peers(self, topic: str) -> Tuple[PeerID, ...]:
        return await self.pubsub_client.list_peers(topic=topic)

    @staticmethod
    async def _call_validator(
            topic: str,
            validator: Validator,
            src_peer: PeerID,
            ps_msg: p2pd_pb.PSMessage,
            writer: asyncio.StreamWriter) -> None:
        await validator(
            src_peer,
            ps_msg,
        )
        # TODO: write the validation result back to the daemon

    async def _topic_listener(
            self,
            topic: str,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter) -> None:
        while True:
            ps_msg = p2pd_pb.PSMessage()
            await read_pbmsg_safe(reader, ps_msg)
            if topic in self._validators:
                asyncio.ensure_future(
                    # FIXME: temporarily fill this with `from_field`(the origin of the message),
                    #   but it is not available in the upstream. This should be `src` instead.
                    self._call_validator(
                        topic,
                        self._validators[topic],
                        ps_msg.from_field,
                        ps_msg,
                        writer,
                    )
                )
