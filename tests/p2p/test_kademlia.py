from eth_hash.auto import keccak

from p2p.kademlia import (
    Address,
    Node,
    check_relayed_addr,
)
from p2p.tools.factories import (
    AddressFactory,
    ENRFactory,
    PrivateKeyFactory,
)


def test_node_from_enode_uri():
    pubkey = 'a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e3bec910127f134779fbcb0cb6d3331163c'  # noqa: E501
    ip = '52.16.188.185'
    port = 30303
    uri = 'enode://%s@%s:%d' % (pubkey, ip, port)
    node = Node.from_uri(uri)
    assert node.address.ip == ip
    assert node.address.udp_port == node.address.tcp_port == port
    assert node.pubkey.to_hex() == '0x' + pubkey


def test_node_from_enr_uri():
    privkey = PrivateKeyFactory()
    address = AddressFactory()
    enr = ENRFactory(private_key=privkey.to_bytes(), address=address)

    node = Node.from_uri(repr(enr))

    assert node.id == keccak(privkey.public_key.to_bytes())
    assert node.address == address


def test_node_constructor():
    privkey = PrivateKeyFactory()
    address = AddressFactory()
    enr = ENRFactory(private_key=privkey.to_bytes(), address=address)

    node = Node(enr)

    assert node.id == keccak(privkey.public_key.to_bytes())
    assert node.address == address


def test_check_relayed_addr():
    public_host = Address('8.8.8.8', 80, 80)
    local_host = Address('127.0.0.1', 80, 80)
    assert check_relayed_addr(local_host, local_host)
    assert not check_relayed_addr(public_host, local_host)

    private = Address('192.168.1.1', 80, 80)
    assert check_relayed_addr(private, private)
    assert not check_relayed_addr(public_host, private)

    reserved = Address('240.0.0.1', 80, 80)
    assert not check_relayed_addr(local_host, reserved)
    assert not check_relayed_addr(public_host, reserved)

    unspecified = Address('0.0.0.0', 80, 80)
    assert not check_relayed_addr(local_host, unspecified)
    assert not check_relayed_addr(public_host, unspecified)
