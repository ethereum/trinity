import bisect
from functools import total_ordering
import ipaddress
import itertools
import logging
import operator
import random
import struct
import time
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Sized,
    Tuple,
    Type,
    TypeVar,
)
from urllib import parse as urlparse

from cached_property import cached_property

from eth_utils import (
    big_endian_to_int,
    decode_hex,
    remove_0x_prefix,
)

from eth_keys import (
    datatypes,
    keys,
)

from eth_hash.auto import keccak

from p2p.abc import AddressAPI, NodeAPI
from p2p import constants
from p2p.discv5.enr import ENR, IDENTITY_SCHEME_ENR_KEY
from p2p.discv5.constants import (
    IP_V4_ADDRESS_ENR_KEY,
    TCP_PORT_ENR_KEY,
    UDP_PORT_ENR_KEY,
)
from p2p.discv5.identity_schemes import V4CompatIdentityScheme
from p2p.discv5.typing import NodeID
from p2p.validation import validate_enode_uri


def int_to_big_endian4(integer: int) -> bytes:
    """ 4 bytes big endian integer"""
    return struct.pack('>I', integer)


def enc_port(p: int) -> bytes:
    return int_to_big_endian4(p)[-2:]


TAddress = TypeVar('TAddress', bound=AddressAPI)


class Address(AddressAPI):

    def __init__(self, ip: str, udp_port: int, tcp_port: int) -> None:
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self._ip = ipaddress.ip_address(ip)

    @property
    def is_loopback(self) -> bool:
        return self._ip.is_loopback

    @property
    def is_unspecified(self) -> bool:
        return self._ip.is_unspecified

    @property
    def is_reserved(self) -> bool:
        return self._ip.is_reserved

    @property
    def is_private(self) -> bool:
        return self._ip.is_private

    @property
    def ip(self) -> str:
        return str(self._ip)

    @cached_property
    def ip_packed(self) -> str:
        """The binary representation of this IP address."""
        return self._ip.packed

    def __eq__(self, other: Any) -> bool:
        return (self.ip, self.udp_port) == (other.ip, other.udp_port)

    def __repr__(self) -> str:
        return 'Address(%s:udp:%s|tcp:%s)' % (self.ip, self.udp_port, self.tcp_port)

    def to_endpoint(self) -> List[bytes]:
        return [self._ip.packed, enc_port(self.udp_port), enc_port(self.tcp_port)]

    @classmethod
    def from_endpoint(cls: Type[TAddress],
                      ip: str,
                      udp_port: bytes,
                      tcp_port: bytes = b'\x00\x00') -> TAddress:
        return cls(ip, big_endian_to_int(udp_port), big_endian_to_int(tcp_port))


TNode = TypeVar('TNode', bound=NodeAPI)


@total_ordering
class Node(NodeAPI):

    def __init__(self, enr: ENR) -> None:
        self._init(enr)

    def _init(self, enr: ENR) -> None:
        try:
            ip = enr[IP_V4_ADDRESS_ENR_KEY]
            udp_port = enr[UDP_PORT_ENR_KEY]
        except KeyError:
            self._address = None
        else:
            tcp_port = enr.get(TCP_PORT_ENR_KEY, udp_port)
            self._address = Address(ip, udp_port, tcp_port)
        # FIXME: ENRs may use different pubkey formats and this would break, so instead of storing
        # a PublicKey with a certain format here we should simply use the APIs in the
        # ENR.identity_scheme for the crypto related operations.
        self._pubkey = keys.PublicKey.from_compressed_bytes(enr.public_key)
        self._id = NodeID(keccak(self.pubkey.to_bytes()))
        self._id_int = big_endian_to_int(self.id)
        self._enr = enr

    @property
    def id(self) -> NodeID:
        return self._id

    @property
    def pubkey(self) -> keys.PublicKey:
        return self._pubkey

    @property
    def address(self) -> Address:
        return self._address

    @classmethod
    def from_pubkey_and_addr(
            cls: Type[TNode], pubkey: datatypes.PublicKey, address: AddressAPI) -> TNode:
        return cls(create_stub_enr(pubkey, address))

    @classmethod
    def from_uri(cls: Type[TNode], uri: str) -> TNode:
        if uri.startswith("enr:"):
            return cls.from_enr_repr(uri)
        else:
            return cls.from_enode_uri(uri)

    @classmethod
    def from_enr_repr(cls: Type[TNode], uri: str) -> TNode:
        return cls(ENR.from_repr(uri))

    @classmethod
    def from_enode_uri(cls: Type[TNode], uri: str) -> TNode:
        validate_enode_uri(uri)  # Be no more permissive than the validation
        parsed = urlparse.urlparse(uri)
        pubkey = keys.PublicKey(decode_hex(parsed.username))
        return cls.from_pubkey_and_addr(pubkey, Address(parsed.hostname, parsed.port, parsed.port))

    @property
    def enr(self) -> ENR:
        return self._enr

    def uri(self) -> str:
        hexstring = self.pubkey.to_hex()
        hexstring = remove_0x_prefix(hexstring)

        if self.address is not None:
            ip = self.address.ip
            tcp_port = self.address.tcp_port
        else:
            ip = None
            tcp_port = None

        return f'enode://{hexstring}@{ip}:{tcp_port}'

    def __str__(self) -> str:
        if self.address is not None:
            ip = self.address.ip
        else:
            ip = None

        return f"<Node({self.pubkey.to_hex()[:8]}@{ip})>"

    def __repr__(self) -> str:
        if self.address is not None:
            ip = self.address.ip
            tcp_port = self.address.tcp_port
        else:
            ip = None
            tcp_port = None

        return f"<Node({self.pubkey.to_hex()}@{ip}:{tcp_port})>"

    def distance_to(self, id: int) -> int:
        return self._id_int ^ id

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return super().__lt__(other)
        return self._id_int < other._id_int

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return super().__eq__(other)
        return self.pubkey == other.pubkey

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self.pubkey)

    def __getstate__(self) -> Dict[Any, Any]:
        return {'enr': repr(self.enr)}

    def __setstate__(self, state: Dict[Any, Any]) -> None:
        self._init(ENR.from_repr(state.pop('enr')))


@total_ordering
class KBucket(Sized):
    """A bucket of nodes whose IDs fall between the bucket's start and end.

    The bucket is kept sorted by time last seen—least-recently seen node at the head,
    most-recently seen at the tail.
    """
    def __init__(self, start: int, end: int, size: int = constants.KADEMLIA_BUCKET_SIZE) -> None:
        self.size = size
        self.start = start
        self.end = end
        self.nodes: List[NodeAPI] = []
        self.replacement_cache: List[NodeAPI] = []
        self.last_updated = time.monotonic()

    @property
    def midpoint(self) -> int:
        return self.start + (self.end - self.start) // 2

    def distance_to(self, id: int) -> int:
        return self.midpoint ^ id

    def nodes_by_distance_to(self, id: int) -> List[NodeAPI]:
        return sorted(self.nodes, key=operator.methodcaller('distance_to', id))

    def split(self) -> Tuple['KBucket', 'KBucket']:
        """Split at the median id"""
        splitid = self.midpoint
        lower = KBucket(self.start, splitid)
        upper = KBucket(splitid + 1, self.end)
        for node in self.nodes:
            node_id_int = node._id_int  # type: ignore
            bucket = lower if node_id_int <= splitid else upper
            bucket.add(node)
        for node in self.replacement_cache:
            node_id_int = node._id_int  # type: ignore
            bucket = lower if node_id_int <= splitid else upper
            bucket.replacement_cache.append(node)
        return lower, upper

    def remove_node(self, node: NodeAPI) -> None:
        if node not in self:
            return
        self.nodes.remove(node)
        if self.replacement_cache:
            replacement_node = self.replacement_cache.pop()
            self.nodes.append(replacement_node)

    def in_range(self, node_id: int) -> bool:
        return self.start <= node_id <= self.end

    @property
    def is_full(self) -> bool:
        return len(self) == self.size

    def add(self, node: NodeAPI) -> NodeAPI:
        """Try to add the given node to this bucket.

        If the node is already present, it is moved to the tail of the list, and we return None.

        If the node is not already present and the bucket has fewer than k entries, it is inserted
        at the tail of the list, and we return None.

        If the bucket is full, we add the node to the bucket's replacement cache and return the
        node at the head of the list (i.e. the least recently seen), which should be evicted if it
        fails to respond to a ping.
        """
        self.last_updated = time.monotonic()
        if node in self.nodes:
            self.nodes.remove(node)
            self.nodes.append(node)
        elif len(self) < self.size:
            self.nodes.append(node)
        else:
            if node in self.replacement_cache:
                self.replacement_cache.remove(node)
            elif len(self.replacement_cache) >= self.size:
                self.replacement_cache.pop(0)
            self.replacement_cache.append(node)
            return self.head
        return None

    @property
    def head(self) -> NodeAPI:
        """Least recently seen"""
        return self.nodes[0]

    def __contains__(self, node: NodeAPI) -> bool:
        return node in self.nodes

    def __len__(self) -> int:
        return len(self.nodes)

    def __lt__(self, other: 'KBucket') -> bool:
        if not isinstance(other, self.__class__):
            raise TypeError(f"Cannot compare KBucket with type {other.__class__}")
        return self.end < other.start


class RoutingTable:
    logger = logging.getLogger("p2p.kademlia.RoutingTable")

    def __init__(self, node_id: NodeID) -> None:
        self._initialized_at = time.monotonic()
        self.this_node_id_int = big_endian_to_int(node_id)
        self.buckets = [KBucket(0, constants.KADEMLIA_MAX_NODE_ID)]

    def iter_random(self) -> Iterator[NodeAPI]:
        # Create a new list with all available nodes as buckets can mutate while we're iterating.
        # This shouldn't use a significant amount of memory as the new list will keep just
        # references to the existing Node instances.
        nodes = list(itertools.chain(*[bucket.nodes for bucket in self.buckets]))
        random.shuffle(nodes)
        for node in nodes:
            yield node

    def split_bucket(self, index: int) -> None:
        bucket = self.buckets[index]
        a, b = bucket.split()
        self.buckets[index] = a
        self.buckets.insert(index + 1, b)

    @property
    def idle_buckets(self) -> List[KBucket]:
        idle_cutoff_time = time.monotonic() - constants.KADEMLIA_IDLE_BUCKET_REFRESH_INTERVAL
        return [b for b in self.buckets if b.last_updated < idle_cutoff_time]

    @property
    def not_full_buckets(self) -> List[KBucket]:
        return [b for b in self.buckets if not b.is_full]

    def remove_node(self, node: NodeAPI) -> None:
        node_id_int = node._id_int  # type: ignore
        binary_get_bucket_for_node(self.buckets, node_id_int).remove_node(node)

    def add_node(self, node: NodeAPI) -> NodeAPI:
        node_id_int = node._id_int  # type: ignore
        if node_id_int == self.this_node_id_int:
            raise ValueError("Cannot add this_node to routing table")
        bucket = binary_get_bucket_for_node(self.buckets, node_id_int)
        eviction_candidate = bucket.add(node)
        if eviction_candidate is not None:  # bucket is full
            # Split if the bucket has the local node in its range or if the depth is not congruent
            # to 0 mod KADEMLIA_BITS_PER_HOP
            depth = _compute_shared_prefix_bits(bucket.nodes)
            should_split = any((
                bucket.in_range(self.this_node_id_int),
                (depth % constants.KADEMLIA_BITS_PER_HOP != 0 and depth != constants.KADEMLIA_ID_SIZE),  # noqa: E501
            ))
            if should_split:
                self.split_bucket(self.buckets.index(bucket))
                return self.add_node(node)  # retry
            # Nothing added, ping eviction_candidate
            return eviction_candidate
        return None  # successfully added to not full bucket

    def get_node(self, node_id: NodeID) -> NodeAPI:
        """
        Return the Node with the given ID if it is in the routing table or the replacement cache.

        Raises a KeyError otherwise.
        """
        id_int = big_endian_to_int(node_id)
        bucket = binary_get_bucket_for_node(self.buckets, id_int)
        for node in itertools.chain(bucket.nodes, bucket.replacement_cache):
            if node.id == node_id:
                return node
        raise KeyError("Node {node_id} is not in routing table nor replacement cache")

    def get_bucket_for_node(self, node_id: int) -> KBucket:
        return binary_get_bucket_for_node(self.buckets, node_id)

    def buckets_by_distance_to(self, id: int) -> List[KBucket]:
        return sorted(self.buckets, key=operator.methodcaller('distance_to', id))

    def __contains__(self, node: NodeAPI) -> bool:
        node_id_int = node._id_int  # type: ignore
        return node in self.get_bucket_for_node(node_id_int)

    def __len__(self) -> int:
        return sum(len(b) for b in self.buckets)

    def __iter__(self) -> Iterable[NodeAPI]:
        for b in self.buckets:
            for n in b.nodes:
                yield n

    def neighbours(self, node_id: NodeID, k: int = constants.KADEMLIA_BUCKET_SIZE) -> List[NodeAPI]:
        """Return up to k neighbours of the given node."""
        id_int = big_endian_to_int(node_id)
        nodes = []
        # Sorting by bucket.midpoint does not work in edge cases, so build a short list of k * 2
        # nodes and sort it by distance_to.
        for bucket in self.buckets_by_distance_to(id_int):
            for n in bucket.nodes_by_distance_to(id_int):
                if n.id is not node_id:
                    nodes.append(n)
                    if len(nodes) == k * 2:
                        break
        return sort_by_distance(nodes, node_id)[:k]


def check_relayed_addr(sender: AddressAPI, addr: AddressAPI) -> bool:
    """Check if an address relayed by the given sender is valid.

    Reserved and unspecified addresses are always invalid.
    Private addresses are valid if the sender is a private host.
    Loopback addresses are valid if the sender is a loopback host.
    All other addresses are valid.
    """
    if addr.is_unspecified or addr.is_reserved:
        return False
    if addr.is_private and not sender.is_private:
        return False
    if addr.is_loopback and not sender.is_loopback:
        return False
    return True


def binary_get_bucket_for_node(buckets: List[KBucket], node_id: int) -> KBucket:
    """Given a list of ordered buckets, returns the bucket for a given node ID."""
    bucket_ends = [bucket.end for bucket in buckets]
    bucket_position = bisect.bisect_left(bucket_ends, node_id)
    # Prevents edge cases where bisect_left returns an out of range index
    try:
        bucket = buckets[bucket_position]
        assert bucket.start <= node_id <= bucket.end
        return bucket
    except (IndexError, AssertionError):
        raise ValueError(f"No bucket found for node with id {node_id}")


def _compute_shared_prefix_bits(nodes: List[NodeAPI]) -> int:
    """Count the number of prefix bits shared by all nodes."""
    def to_binary(x: int) -> str:  # left padded bit representation
        b = bin(x)[2:]
        return '0' * (constants.KADEMLIA_ID_SIZE - len(b)) + b

    if len(nodes) < 2:
        return constants.KADEMLIA_ID_SIZE

    bits = [
        to_binary(n._id_int)  # type: ignore
        for n in nodes
    ]
    for i in range(1, constants.KADEMLIA_ID_SIZE + 1):
        if len(set(b[:i] for b in bits)) != 1:
            return i - 1
    # This means we have at least two nodes with the same ID, so raise an AssertionError
    # because we don't want it to be caught accidentally.
    raise AssertionError("Unable to calculate number of shared prefix bits")


def sort_by_distance(nodes: Iterable[NodeAPI], target_id: NodeID) -> List[NodeAPI]:
    target_id_int = big_endian_to_int(target_id)
    return sorted(nodes, key=operator.methodcaller('distance_to', target_id_int))


def create_stub_enr(pubkey: datatypes.PublicKey, address: AddressAPI) -> ENR:
    return ENR(
        0,
        {
            IDENTITY_SCHEME_ENR_KEY: V4CompatIdentityScheme.id,
            V4CompatIdentityScheme.public_key_enr_key: pubkey.to_compressed_bytes(),
            IP_V4_ADDRESS_ENR_KEY: address.ip_packed,
            UDP_PORT_ENR_KEY: address.udp_port,
            TCP_PORT_ENR_KEY: address.tcp_port,
        },
        signature=b''
    )
