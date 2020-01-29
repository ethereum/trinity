from typing import Any

import factory

from eth_utils import int_to_big_endian

from p2p.abc import AddressAPI, NodeAPI
from p2p.kademlia import Node, Address

from .keys import PublicKeyFactory
from .socket import get_open_port


IPAddressFactory = factory.Faker("ipv4")


class AddressFactory(factory.Factory):
    class Meta:
        model = Address

    ip = IPAddressFactory
    udp_port = tcp_port = factory.LazyFunction(get_open_port)

    @classmethod
    def localhost(cls, *args: Any, **kwargs: Any) -> AddressAPI:
        return cls(*args, ip='127.0.0.1', **kwargs)


class NodeFactory(factory.Factory):
    class Meta:
        model = Node.from_pubkey_and_addr

    pubkey = factory.LazyFunction(PublicKeyFactory)
    address = factory.SubFactory(AddressFactory)

    @classmethod
    def with_nodeid(cls, nodeid: int, *args: Any, **kwargs: Any) -> NodeAPI:
        node = cls(*args, **kwargs)
        node._id_int = nodeid
        node._id = int_to_big_endian(nodeid)
        return node
