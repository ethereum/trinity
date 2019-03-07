from abc import (
    ABC,
    abstractmethod,
)

from .p2pclient.p2pclient import (
    PubSubClient,
)


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
    def list_peers(self, topic, data):
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


class DaemonPubSub(BasePubSub):
    """
    Implement pubsub with libp2p daemon pubsub
    """

    pubsub_client: PubSubClient

    def __init__(self, pubsub_client: PubSubClient):
        self.pubsub_client = pubsub_client

    @abstractmethod
    def subscribe(self, topic: str):
        self.pubsub_client.

    @abstractmethod
    def get_topics(self):
        pass

    @abstractmethod
    def publish(self, topic, data):
        pass

    @abstractmethod
    def list_peers(self, topic, data):
        pass
