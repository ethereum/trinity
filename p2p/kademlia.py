import collections
import functools
import ipaddress
import itertools
import operator
import random
import struct
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Type,
    TypeVar,
    Tuple, Deque, Iterator)
from urllib import parse as urlparse

from cached_property import cached_property

from eth_utils import (
    big_endian_to_int,
    decode_hex,
    remove_0x_prefix,
    encode_hex)

from eth_keys import (
    datatypes,
    keys,
)

from eth_enr import ENR, ENRAPI, V4CompatIdentityScheme
from eth_enr.constants import (
    IP_V4_ADDRESS_ENR_KEY,
    UDP_PORT_ENR_KEY,
    TCP_PORT_ENR_KEY,
    IDENTITY_SCHEME_ENR_KEY,
)

from eth_hash.auto import keccak
from eth_typing import NodeID

from p2p.abc import AddressAPI, NodeAPI
from p2p.constants import NUM_ROUTING_TABLE_BUCKETS
from p2p._utils import get_logger
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


@functools.total_ordering
class Node(NodeAPI):

    def __init__(self, enr: ENRAPI) -> None:
        self._init(enr)

    def _init(self, enr: ENRAPI) -> None:
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
    def enr(self) -> ENRAPI:
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


def sort_by_distance(nodes: Iterable[NodeAPI], target_id: NodeID) -> List[NodeAPI]:
    target_id_int = big_endian_to_int(target_id)
    return sorted(nodes, key=operator.methodcaller('distance_to', target_id_int))


def create_stub_enr(pubkey: datatypes.PublicKey, address: AddressAPI) -> ENRAPI:
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


def compute_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    left_int = big_endian_to_int(left_node_id)
    right_int = big_endian_to_int(right_node_id)
    return left_int ^ right_int


def compute_log_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    if left_node_id == right_node_id:
        raise ValueError("Cannot compute log distance between identical nodes")
    distance = compute_distance(left_node_id, right_node_id)
    return distance.bit_length()


class KademliaRoutingTable:

    def __init__(self, center_node_id: NodeID, bucket_size: int) -> None:
        self.logger = get_logger("p2p.kademlia.KademliaRoutingTable")
        self.center_node_id = center_node_id
        self.bucket_size = bucket_size

        self.buckets: Tuple[Deque[NodeID], ...] = tuple(
            collections.deque(maxlen=bucket_size) for _ in range(NUM_ROUTING_TABLE_BUCKETS)
        )
        self.replacement_caches: Tuple[Deque[NodeID], ...] = tuple(
            collections.deque() for _ in range(NUM_ROUTING_TABLE_BUCKETS)
        )

        self.bucket_update_order: Deque[int] = collections.deque()

    def _contains(self, node_id: NodeID, include_replacement_cache: bool) -> bool:
        _, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(node_id)
        if include_replacement_cache:
            nodes = bucket + replacement_cache
        else:
            nodes = bucket
        return node_id in nodes

    def get_index_bucket_and_replacement_cache(self,
                                               node_id: NodeID,
                                               ) -> Tuple[int, Deque[NodeID], Deque[NodeID]]:
        index = compute_log_distance(self.center_node_id, node_id) - 1
        bucket = self.buckets[index]
        replacement_cache = self.replacement_caches[index]
        return index, bucket, replacement_cache

    def update(self, node_id: NodeID) -> NodeID:
        """Insert a node into the routing table or move it to the top if already present.

        If the bucket is already full, the node id will be added to the replacement cache and
        the oldest node is returned as an eviction candidate. Otherwise, the return value is
        `None`.
        """
        if node_id == self.center_node_id:
            raise ValueError("Cannot insert center node into routing table")

        bucket_index, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(
            node_id,
        )

        is_bucket_full = len(bucket) >= self.bucket_size
        is_node_in_bucket = node_id in bucket

        if not is_node_in_bucket and not is_bucket_full:
            self.logger.debug2("Adding %s to bucket %d", encode_hex(node_id), bucket_index)
            self.update_bucket_unchecked(node_id)
            eviction_candidate = None
        elif is_node_in_bucket:
            self.logger.debug2("Updating %s in bucket %d", encode_hex(node_id), bucket_index)
            self.update_bucket_unchecked(node_id)
            eviction_candidate = None
        elif not is_node_in_bucket and is_bucket_full:
            if node_id not in replacement_cache:
                self.logger.debug2(
                    "Adding %s to replacement cache of bucket %d",
                    encode_hex(node_id),
                    bucket_index,
                )
            else:
                self.logger.debug2(
                    "Updating %s in replacement cache of bucket %d",
                    encode_hex(node_id),
                    bucket_index,
                )
                replacement_cache.remove(node_id)
            replacement_cache.appendleft(node_id)
            eviction_candidate = bucket[-1]
        else:
            raise Exception("unreachable")

        return eviction_candidate

    def update_bucket_unchecked(self, node_id: NodeID) -> None:
        """Add or update assuming the node is either present already or the bucket is not full."""
        bucket_index, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(
            node_id,
        )

        for container in (bucket, replacement_cache):
            try:
                container.remove(node_id)
            except ValueError:
                pass
        bucket.appendleft(node_id)

        try:
            self.bucket_update_order.remove(bucket_index)
        except ValueError:
            pass
        self.bucket_update_order.appendleft(bucket_index)

    def remove(self, node_id: NodeID) -> None:
        """Remove a node from the routing table if it is present.

        If possible, the node will be replaced with the newest entry in the replacement cache.
        """
        bucket_index, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(
            node_id,
        )

        in_bucket = node_id in bucket
        in_replacement_cache = node_id in replacement_cache

        if in_bucket:
            bucket.remove(node_id)
            if replacement_cache:
                replacement_node_id = replacement_cache.popleft()
                self.logger.debug(
                    "Replacing %s from bucket %d with %s from replacement cache",
                    encode_hex(node_id),
                    bucket_index,
                    encode_hex(replacement_node_id),
                )
                bucket.append(replacement_node_id)
            else:
                self.logger.debug(
                    "Removing %s from bucket %d without replacement",
                    encode_hex(node_id),
                    bucket_index,
                )

        if in_replacement_cache:
            self.logger.debug(
                "Removing %s from replacement cache of bucket %d",
                encode_hex(node_id),
                bucket_index,
            )
            replacement_cache.remove(node_id)

        if not in_bucket and not in_replacement_cache:
            self.logger.debug(
                "Not removing %s as it is neither present in the bucket nor the replacement cache",
                encode_hex(node_id),
                bucket_index,
            )

        # bucket_update_order should only contain non-empty buckets, so remove it if necessary
        if not bucket:
            try:
                self.bucket_update_order.remove(bucket_index)
            except ValueError:
                pass

    def get_nodes_at_log_distance(self, log_distance: int) -> Tuple[NodeID, ...]:
        """Get all nodes in the routing table at the given log distance to the center."""
        if log_distance <= 0:
            raise ValueError(f"Log distance must be positive, got {log_distance}")
        elif log_distance > len(self.buckets):
            raise ValueError(
                f"Log distance must not be greater than {len(self.buckets)}, got {log_distance}"
            )
        return tuple(self.buckets[log_distance - 1])

    @property
    def is_empty(self) -> bool:
        return all(len(bucket) == 0 for bucket in self.buckets)

    def get_least_recently_updated_log_distance(self) -> int:
        """Get the log distance whose corresponding bucket was updated least recently.

        Only non-empty buckets are considered. If all buckets are empty, a `ValueError` is raised.
        """
        try:
            bucket_index = self.bucket_update_order[-1]
        except IndexError:
            raise ValueError("Routing table is empty")
        else:
            return bucket_index + 1

    def iter_nodes_around(self, reference_node_id: NodeID) -> Iterator[NodeID]:
        """Iterate over all nodes in the routing table ordered by distance to a given reference."""
        all_node_ids = itertools.chain(*self.buckets)
        distance_to_reference = functools.partial(compute_distance, reference_node_id)
        sorted_node_ids = sorted(all_node_ids, key=distance_to_reference)
        for node_id in sorted_node_ids:
            yield node_id

    def iter_all_random(self) -> Iterator[NodeID]:
        """
        Iterate over all nodes in the table (including ones in the replacement cache) in a random
        order.
        """
        # Create a new list with all available nodes as buckets can mutate while we're iterating.
        # This shouldn't use a significant amount of memory as the new list will keep just
        # references to the existing NodeID instances.
        node_ids = list(itertools.chain(*self.buckets, *self.replacement_caches))
        random.shuffle(node_ids)
        for node_id in node_ids:
            yield node_id
