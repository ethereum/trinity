import collections
import math
import operator
from typing import (
    DefaultDict,
    Deque,
    NamedTuple,
    Tuple,
)

from eth_utils import (
    encode_hex,
)
from eth_utils import toolz

from p2p.discv5.enr import ENR
from p2p.discv5.typing import (
    Topic,
)


class Ad(NamedTuple):
    enr: ENR
    registration_time: float


class TopicTable:
    def __init__(self, max_queue_size: int, max_total_size: int, target_ad_lifetime: float) -> None:
        self.max_queue_size = max_queue_size
        self.max_total_size = max_total_size
        self.target_ad_lifetime = target_ad_lifetime

        self.topic_queues: DefaultDict[Topic, Deque[Ad]] = collections.defaultdict(
            lambda: collections.deque(maxlen=self.max_queue_size),
        )
        self.total_size = 0

    def __len__(self) -> int:
        """Return the total number of ads in the table across all queues."""
        return self.total_size

    @property
    def is_full(self) -> bool:
        return len(self) >= self.max_total_size

    def is_queue_full(self, topic: Topic) -> bool:
        return len(self.topic_queues[topic]) >= self.max_queue_size

    def get_enrs_for_topic(self, topic: Topic) -> Tuple[ENR, ...]:
        """Get all ENRs registered for a given topic.

        The result will be ordered from newest to oldest entry.
        """
        return tuple(ad.enr for ad in self.topic_queues[topic])

    def get_wait_time(self, topic: Topic, current_time: float) -> float:
        """Return the time at which the next ad for a given topic can be added."""
        is_table_full = self.is_full
        is_queue_full = self.is_queue_full(topic)

        if not is_queue_full and not is_table_full:
            return 0

        if is_queue_full:
            queue = self.topic_queues[topic]
            oldest_registration_time_queue = queue[-1].registration_time
        else:
            oldest_registration_time_queue = -math.inf

        if is_table_full:
            oldest_ads = [queue[-1] for queue in self.topic_queues.values() if queue]
            oldest_reg_time = min(ad.registration_time for ad in oldest_ads)
            oldest_registration_time_table = oldest_reg_time
        else:
            oldest_registration_time_table = -math.inf

        next_registration_time = max(
            oldest_registration_time_queue,
            oldest_registration_time_table,
        ) + self.target_ad_lifetime
        return max(next_registration_time - current_time, 0)

    def register(self, topic: Topic, enr: ENR, current_time: float) -> None:
        """Register a new ad.

        A `ValueError` will be raised if the ad cannot be added because the table is full,
        because the node already is present in the queue, or because the topic's wait time is
        non-zero.
        """
        queue = self.topic_queues[topic]

        wait_time = self.get_wait_time(topic, current_time)
        if wait_time > 0:
            raise ValueError(f"Topic queue or table is full (time to wait: {wait_time})")

        present_node_ids = tuple(entry.node_id for entry in self.get_enrs_for_topic(topic))
        if enr.node_id in present_node_ids[:self.max_queue_size - 1]:
            raise ValueError(
                f"Topic queue already contains entry for node {encode_hex(enr.node_id)}"
            )

        if self.is_full:
            queues = [queue for queue in self.topic_queues.values() if queue]
            queue_with_oldest_ad = min(
                queues,
                key=toolz.compose(
                    operator.attrgetter("registration_time"),
                    operator.itemgetter(-1),
                )
            )
            queue_with_oldest_ad.pop()
            self.total_size -= 1

        self.total_size -= len(queue)
        queue.appendleft(Ad(enr=enr, registration_time=current_time))
        self.total_size += len(queue)
