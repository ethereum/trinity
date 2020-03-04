from functools import total_ordering
import ipaddress
import operator
import struct
from typing import (
    Any,
    Dict,
    Iterable,
    List,
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
