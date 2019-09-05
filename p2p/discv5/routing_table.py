from collections import (
    deque,
)
from functools import (
    partial,
)
import logging
import secrets
from typing import (
    Any,
    Deque,
    Iterator,
    Collection,
)

from eth_utils import (
    big_endian_to_int,
    encode_hex,
)

from p2p.discv5.typing import (
    NodeID,
)


def compute_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    left_int = big_endian_to_int(left_node_id)
    right_int = big_endian_to_int(right_node_id)
    return left_int ^ right_int


def compute_log_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    distance = compute_distance(left_node_id, right_node_id)
    return distance.bit_length()


class FlatRoutingTable(Collection[NodeID]):

    logger = logging.getLogger("p2p.discv5.routing_table.FlatRoutingTable")

    def __init__(self, local_node_id: NodeID) -> None:
        self.local_node_id = local_node_id
        self.entries: Deque[NodeID] = deque()

    def add(self, node_id: NodeID) -> None:
        if node_id not in self:
            self.logger.debug("Adding entry %s", encode_hex(node_id))
            self.entries.appendleft(node_id)
        else:
            raise ValueError(f"Entry {encode_hex(node_id)} already present in the routing table")

    def update(self, node_id: NodeID) -> None:
        self.remove(node_id)
        self.add(node_id)

    def add_or_update(self, node_id: NodeID) -> None:
        try:
            self.remove(node_id)
        except KeyError:
            pass
        finally:
            self.add(node_id)

    def remove(self, node_id: NodeID) -> None:
        try:
            self.entries.remove(node_id)
        except ValueError:
            raise KeyError(f"Entry {encode_hex(node_id)} not present in the routing table")
        else:
            self.logger.debug("Removing entry %s", encode_hex(node_id))

    def __contains__(self, node_id: Any) -> bool:
        return node_id in self.entries

    def __len__(self) -> int:
        return len(self.entries)

    def __iter__(self) -> Iterator[NodeID]:
        return self.iter_nodes_around(self.local_node_id)

    def iter_nodes_around(self, center: NodeID) -> Iterator[NodeID]:
        # very inefficient, but easy to understand and most likely not a bottleneck
        node_ids = self.entries
        compute_distance_to_center = partial(compute_distance, center)
        return iter(sorted(node_ids, key=compute_distance_to_center))

    def iter_nodes_at_log_distance(self, log_distance: int) -> Iterator[NodeID]:
        compute_log_distance_to_center = partial(compute_log_distance, self.local_node_id)
        for node_id in self:
            log_distance_to_center = compute_log_distance_to_center(node_id)
            if log_distance_to_center < log_distance:
                continue
            elif log_distance_to_center == log_distance:
                yield node_id
            elif log_distance_to_center > log_distance:
                break

    def get_random_entry(self) -> NodeID:
        return secrets.choice(self.entries)

    def get_oldest_entry(self) -> NodeID:
        return self.entries[-1]
