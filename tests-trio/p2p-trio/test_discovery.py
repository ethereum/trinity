import copy
import functools
import random
import re
import time

import trio

import pytest

import rlp

from eth_utils import decode_hex, int_to_big_endian

from eth_hash.auto import keccak

from eth_keys import keys

from eth.db.backends.memory import MemoryDB

from p2p import constants
from p2p.discv5.constants import (
    IP_V4_ADDRESS_ENR_KEY,
    TCP_PORT_ENR_KEY,
    UDP_PORT_ENR_KEY,
)
from p2p.discv5.enr import UnsignedENR, IDENTITY_SCHEME_ENR_KEY
from p2p.discv5.enr_db import NodeDB
from p2p.discv5.identity_schemes import default_identity_scheme_registry, V4IdentityScheme
from p2p.discovery import (
    CMD_FIND_NODE,
    CMD_NEIGHBOURS,
    CMD_PING,
    CMD_PONG,
    DiscoveryService,
    PROTO_VERSION,
    _get_msg_expiration,
    _extract_nodes_from_payload,
    _pack_v4,
    _unpack_v4,
)
from p2p.kademlia import Node
from p2p.tools.factories import (
    AddressFactory,
    ENRFactory,
    NodeFactory,
    PrivateKeyFactory,
)

from trinity.components.builtin.upnp.events import NewUPnPMapping


# Force our tests to fail quickly if they accidentally make network requests.
@pytest.fixture(autouse=True)
def short_timeout(monkeypatch):
    monkeypatch.setattr(constants, 'KADEMLIA_REQUEST_TIMEOUT', 0.05)


@pytest.mark.trio
async def test_ping_pong(manually_driven_discovery_pair):
    alice, bob = manually_driven_discovery_pair
    # Collect all pongs received by alice in a list for later inspection.
    got_pong = trio.Event()
    received_pongs = []

    async def recv_pong(node, payload, hash_):
        received_pongs.append((node, payload))
        got_pong.set()

    alice.recv_pong_v4 = recv_pong

    token = await alice.send_ping_v4(bob.this_node)

    with trio.fail_after(1):
        await bob.consume_datagram()
        await alice.consume_datagram()
        await got_pong.wait()

    assert len(received_pongs) == 1
    node, payload = received_pongs[0]
    assert node.id == bob.this_node.id
    assert token == payload[1]


def validate_node_enr(node, enr, sequence_number, extra_fields=tuple()):
    assert enr is not None
    enr.validate_signature()
    assert enr.sequence_number == sequence_number
    assert enr.identity_scheme == V4IdentityScheme
    assert node.pubkey.to_compressed_bytes() == enr.public_key
    assert node.address.ip_packed == enr[IP_V4_ADDRESS_ENR_KEY]
    assert node.address.udp_port == enr[UDP_PORT_ENR_KEY]
    assert node.address.tcp_port == enr[TCP_PORT_ENR_KEY]
    enr_items = enr.items()
    for extra_key, extra_value in extra_fields:
        assert (extra_key, extra_value) in enr_items


@pytest.mark.trio
async def test_get_local_enr(manually_driven_discovery):
    discovery = manually_driven_discovery

    enr = await discovery.get_local_enr()

    validate_node_enr(discovery.this_node, enr, sequence_number=1)

    old_node = copy.copy(discovery.this_node)
    # If our node's details change but an ENR refresh is not due yet, we'll get the ENR for the
    # old node.
    discovery.this_node.address.udp_port += 1
    assert discovery._local_enr_next_refresh > time.monotonic()
    enr = await discovery.get_local_enr()

    validate_node_enr(old_node, enr, sequence_number=1)

    # If a local ENR refresh is due, get_local_enr() will create a fresh ENR with a new sequence
    # number.
    discovery._local_enr_next_refresh = time.monotonic() - 1
    enr = await discovery.get_local_enr()

    validate_node_enr(discovery.this_node, enr, sequence_number=2)

    # The new ENR will also be stored in our DB.
    our_node = Node(discovery.node_db.get_enr(discovery.this_node.id))
    assert enr == our_node.enr

    # And the next refresh time will be updated.
    assert discovery._local_enr_next_refresh > time.monotonic()


@pytest.mark.trio
async def test_local_enr_on_startup(manually_driven_discovery):
    discovery = manually_driven_discovery

    validate_node_enr(discovery.this_node, discovery.this_node.enr, sequence_number=1)
    # Our local ENR will also be stored in our DB.
    our_enr = discovery.node_db.get_enr(discovery.this_node.id)
    assert discovery.this_node.enr == our_enr


@pytest.mark.trio
async def test_local_enr_fields(manually_driven_discovery):
    discovery = manually_driven_discovery

    async def test_field_provider(key, value):
        return (key, value)

    expected_fields = [(b'key1', b'value1'), (b'key2', b'value2')]
    field_providers = tuple(
        functools.partial(test_field_provider, key, value)
        for key, value in expected_fields
    )
    discovery.enr_field_providers = field_providers
    # Force a refresh or our local ENR.
    discovery._local_enr_next_refresh = time.monotonic() - 1
    enr = await discovery.get_local_enr()
    validate_node_enr(discovery.this_node, enr, sequence_number=2, extra_fields=expected_fields)


@pytest.mark.trio
async def test_request_enr(nursery, manually_driven_discovery_pair):
    alice, bob = manually_driven_discovery_pair
    # Pretend that bob and alice have already bonded, otherwise bob will ignore alice's ENR
    # request.
    bob.node_db.set_last_pong_time(alice.this_node.id, int(time.monotonic()))

    # Add a copy of Bob's node with a stub ENR to alice's RT as later we're going to check that it
    # gets updated with the received ENR.
    bobs_node_with_stub_enr = Node.from_pubkey_and_addr(
        bob.this_node.pubkey, bob.this_node.address)
    alice.node_db.set_last_pong_time(bob.this_node.id, int(time.monotonic()))
    alice.update_routing_table(bobs_node_with_stub_enr)
    assert alice.routing.get_node(bobs_node_with_stub_enr.id).enr.sequence_number == 0

    received_enr = None
    got_enr = trio.Event()

    async def fetch_enr(event):
        nonlocal received_enr
        received_enr = await alice.request_enr(bobs_node_with_stub_enr)
        event.set()

    # Start a task in the background that requests an ENR to bob and then waits for it.
    nursery.start_soon(fetch_enr, got_enr)

    # Bob will now consume one datagram containing the ENR_REQUEST from alice, and as part of that
    # will send an ENR_RESPONSE, which will then be consumed by alice, and as part of that it will
    # be fed into the request_enr() task we're running the background.
    with trio.fail_after(0.1):
        await bob.consume_datagram()
        await alice.consume_datagram()

    with trio.fail_after(1):
        await got_enr.wait()

    validate_node_enr(bob.this_node, received_enr, sequence_number=1)
    assert alice.routing.get_node(bob.this_node.id).enr == received_enr

    # Now, if Bob later sends us a new ENR with no endpoint information, we'll evict him from both
    # our DB and RT.
    sequence_number = bob.this_node.enr.sequence_number + 1
    new_unsigned_enr = UnsignedENR(
        sequence_number,
        kv_pairs={
            IDENTITY_SCHEME_ENR_KEY: V4IdentityScheme.id,
            V4IdentityScheme.public_key_enr_key: bob.pubkey.to_compressed_bytes(),
        }
    )
    bob.this_node = Node(new_unsigned_enr.to_signed_enr(bob.privkey.to_bytes()))

    received_enr = None
    got_new_enr = trio.Event()
    nursery.start_soon(fetch_enr, got_new_enr)
    with trio.fail_after(0.1):
        await bob.consume_datagram()
        await alice.consume_datagram()

    with trio.fail_after(1):
        await got_new_enr.wait()

    assert Node(received_enr).address is None
    with pytest.raises(KeyError):
        alice.routing.get_node(bob.this_node.id)
    with pytest.raises(KeyError):
        alice.node_db.get_enr(bob.this_node.id)


@pytest.mark.trio
async def test_find_node_neighbours(manually_driven_discovery_pair):
    alice, bob = manually_driven_discovery_pair
    # Add some nodes to bob's routing table so that it has something to use when replying to
    # alice's find_node.
    for _ in range(constants.KADEMLIA_BUCKET_SIZE * 2):
        bob.update_routing_table(NodeFactory())

    # Collect all neighbours packets received by alice in a list for later inspection.
    received_neighbours = []

    async def recv_neighbours(node, payload, hash_):
        received_neighbours.append((node, payload))

    alice.recv_neighbours_v4 = recv_neighbours
    # Pretend that bob and alice have already bonded, otherwise bob will ignore alice's find_node.
    bob.node_db.set_last_pong_time(alice.this_node.id, int(time.monotonic()))

    alice.send_find_node_v4(bob.this_node, alice.pubkey.to_bytes())

    with trio.fail_after(1):
        await bob.consume_datagram()
        # Alice needs to consume two datagrams here because we expect bob's response to be split
        # across two packets since a single one would be bigger than protocol's byte limit.
        await alice.consume_datagram()
        await alice.consume_datagram()
        # Bob should have sent two neighbours packets in order to keep the total packet size
        # under the 1280 bytes limit. However, the two consume_datagram() calls above will have
        # spawned background tasks so we take a few short naps here to wait for them to complete.
        while len(received_neighbours) != 2:
            await trio.sleep(0.01)

    packet1, packet2 = received_neighbours
    neighbours = []
    for packet in [packet1, packet2]:
        node, payload = packet
        assert node == bob.this_node
        neighbours.extend(_extract_nodes_from_payload(
            node.address, payload[0], bob.logger))
    assert len(neighbours) == constants.KADEMLIA_BUCKET_SIZE


@pytest.mark.trio
async def test_get_peer_candidates(manually_driven_discovery, monkeypatch):
    total_nodes = 10
    nodes = NodeFactory.create_batch(total_nodes)
    discovery = manually_driven_discovery
    for node in nodes:
        discovery.routing.add_node(node)

    discovery._random_lookup_calls = 0

    async def mock_lookup_random():
        discovery._random_lookup_calls += 1

    monkeypatch.setattr(discovery, 'lookup_random', mock_lookup_random)

    def should_skip(skip_list, candidate):
        return candidate in skip_list

    candidates = discovery.get_peer_candidates(functools.partial(should_skip, tuple()), total_nodes)
    assert sorted(candidates) == sorted(nodes)

    candidates = discovery.get_peer_candidates(
        functools.partial(should_skip, tuple()), total_nodes + 10)
    assert sorted(candidates) == sorted(nodes)
    # When we don't have enough candidates, a random lookup should be triggered.
    with trio.fail_after(0.5):
        while discovery._random_lookup_calls != 1:
            await trio.sleep(0.01)

    candidates = discovery.get_peer_candidates(
        functools.partial(should_skip, tuple()), total_nodes - 1)
    assert len(candidates) == total_nodes - 1

    skip_list = (nodes[0], nodes[5], nodes[8])
    candidates = discovery.get_peer_candidates(
        functools.partial(should_skip, skip_list), total_nodes)
    assert sorted(candidates) == sorted(set(nodes).difference(skip_list))
    with trio.fail_after(0.5):
        while discovery._random_lookup_calls != 2:
            await trio.sleep(0.01)


@pytest.mark.trio
async def test_handle_new_upnp_mapping(manually_driven_discovery, endpoint_server):
    manually_driven_discovery._event_bus = endpoint_server
    manually_driven_discovery.manager.run_daemon_task(
        manually_driven_discovery.handle_new_upnp_mapping)
    assert manually_driven_discovery.this_node.address.ip == '127.0.0.1'
    assert manually_driven_discovery.this_node.enr.sequence_number == 1

    await trio.hazmat.checkpoint()
    external_ip = '43.248.27.0'
    await endpoint_server.broadcast(NewUPnPMapping(external_ip))

    with trio.fail_after(0.5):
        while True:
            await trio.sleep(0.01)
            if manually_driven_discovery.this_node.address.ip == external_ip:
                break
    assert manually_driven_discovery.this_node.enr.sequence_number == 2


@pytest.mark.trio
async def test_protocol_bootstrap(monkeypatch):
    node1, node2 = NodeFactory.create_batch(2)
    discovery = MockDiscoveryService([node1, node2])
    invalidated_bonds = []

    def invalidate_bond(node_id):
        invalidated_bonds.append(node_id)

    async def bond(node):
        assert discovery.routing.add_node(node) is None
        return True

    monkeypatch.setattr(discovery, 'invalidate_bond', invalidate_bond)
    # Pretend we bonded successfully with our bootstrap nodes.
    monkeypatch.setattr(discovery, 'bond', bond)

    await discovery.bootstrap()

    assert sorted(invalidated_bonds) == sorted([node.id for node in [node1, node2]])
    assert len(discovery.messages) == 2
    # We don't care in which order the bootstrap nodes are contacted, nor which node_id was used
    # in the find_node request, so we just assert that we sent find_node msgs to both nodes.
    assert sorted([(node, cmd) for (node, cmd, _) in discovery.messages]) == sorted([
        (node1, 'find_node'),
        (node2, 'find_node')])


@pytest.mark.trio
async def test_wait_neighbours(nursery):
    service = MockDiscoveryService([])
    node = NodeFactory()

    # Schedule a call to service.recv_neighbours_v4() simulating a neighbours response from the
    # node we expect.
    neighbours = tuple(NodeFactory.create_batch(3))
    expiration = _get_msg_expiration()
    neighbours_msg_payload = [
        [n.address.to_endpoint() + [n.pubkey.to_bytes()] for n in neighbours],
        expiration]
    nursery.start_soon(service.recv_neighbours_v4, node, neighbours_msg_payload, b'')

    received_neighbours = await service.wait_neighbours(node)

    assert neighbours == received_neighbours
    # Ensure wait_neighbours() cleaned up after itself.
    assert not service.neighbours_channels.already_waiting_for(node)

    # If wait_neighbours() times out, we get an empty list of neighbours.
    received_neighbours = await service.wait_neighbours(node)

    assert received_neighbours == tuple()
    assert not service.neighbours_channels.already_waiting_for(node)


@pytest.mark.trio
async def test_bond(nursery, monkeypatch):
    discovery = MockDiscoveryService([])
    us = discovery.this_node
    node = NodeFactory()

    token = b'token'

    async def send_ping(node):
        return token

    # Do not send pings, instead simply return the pingid we'd expect back together with the pong.
    monkeypatch.setattr(discovery, 'send_ping_v4', send_ping)

    # Schedule a call to service.recv_pong() simulating a pong from the node we expect.
    enr_seq = 1
    pong_msg_payload = [
        us.address.to_endpoint(), token, _get_msg_expiration(), int_to_big_endian(enr_seq)]
    nursery.start_soon(discovery.recv_pong_v4, node, pong_msg_payload, b'')

    bonded = await discovery.bond(node)

    assert bonded
    assert discovery.is_bond_valid_with(node.id)

    # Upon successfully bonding, retrieval of the remote's ENR will be scheduled.
    with trio.fail_after(1):
        scheduled_enr_node_id, scheduled_enr_seq = await discovery.pending_enrs_consumer.receive()
    assert scheduled_enr_node_id == node.id
    assert scheduled_enr_seq == enr_seq

    # If we try to bond with any other nodes we'll timeout and bond() will return False.
    node2 = NodeFactory()
    bonded = await discovery.bond(node2)

    assert not bonded


@pytest.mark.trio
async def test_bond_short_circuits(monkeypatch):
    discovery = MockDiscoveryService([])
    bob = NodeFactory()
    # Pretend we have a valid bond with bob.
    discovery.node_db.set_last_pong_time(bob.id, int(time.monotonic()))

    class AttemptedNewBond(Exception):
        pass

    async def send_ping(node):
        raise AttemptedNewBond()

    monkeypatch.setattr(discovery, 'send_ping_v4', send_ping)

    # When we have a valid bond, we won't attempt a new one.
    assert discovery.is_bond_valid_with(bob.id)
    assert await discovery.bond(bob)


@pytest.mark.trio
async def test_fetch_enrs(nursery, manually_driven_discovery_pair):
    alice, bob = manually_driven_discovery_pair
    # Pretend that bob and alice have already bonded, otherwise bob will ignore alice's ENR
    # request.
    alice.node_db.set_last_pong_time(bob.this_node.id, int(time.monotonic()))
    bob.node_db.set_last_pong_time(alice.this_node.id, int(time.monotonic()))

    # Also add bob's node to alice's DB as when scheduling an ENR retrieval we only get the node ID
    # and need to look it up in the DB.
    alice.update_routing_table(bob.this_node)

    # This task will run in a loop consuming from the pending_enrs_consumer channel and requesting
    # ENRs.
    alice.manager.run_task(alice.fetch_enrs)

    with trio.fail_after(1):
        # Generate a new ENR for bob, because the old one alice already got when we manually added
        # bob's node to her DB above.
        bobs_new_enr = await bob._generate_local_enr(bob.this_node.enr.sequence_number + 1)
        bob.this_node = Node(bobs_new_enr)
        # This feeds a request to retrieve Bob's ENR to fetch_enrs(), which spawns a background
        # task to do it.
        await alice.pending_enrs_producer.send((bob.this_node.id, bobs_new_enr.sequence_number))
        # bob cosumes the ENR_REQUEST and replies with its own ENR
        await bob.consume_datagram()
        # alice consumes the ENR_RESPONSE, feeding the ENR to the background task started above.
        await alice.consume_datagram()
        # Now we need to wait a little bit here for that background task to pick it up and store
        # it in our DB.
        while True:
            await trio.sleep(0.1)
            try:
                bob_enr = alice.node_db.get_enr(bob.this_node.id)
            except KeyError:
                continue
            else:
                break

    assert bob_enr is not None
    assert bob_enr == await bob.get_local_enr()


@pytest.mark.trio
async def test_lookup_and_maybe_update_enr_new_node():
    discovery = MockDiscoveryService([])
    privkey = PrivateKeyFactory()
    address = AddressFactory()

    # When looking up the ENR for a node we haven't heard about before, we'll create a stub ENR
    # but it will not be inserted in our DB (that will happen only after we've successfully
    # bonded).
    enr = discovery.lookup_and_maybe_update_enr(privkey.public_key, address)
    assert enr.sequence_number == 0
    node = Node(enr)
    assert node.pubkey == privkey.public_key
    assert node.address == address
    with pytest.raises(KeyError):
        discovery.node_db.get_enr(node.id)


@pytest.mark.trio
async def test_lookup_and_maybe_update_enr_existing_node():
    discovery = MockDiscoveryService([])
    privkey = PrivateKeyFactory()
    address = AddressFactory()

    # When we have an ENR for the given pubkey, and its address matches the given one, the
    # existing ENR is returned.
    enr = ENRFactory(private_key=privkey.to_bytes(), address=address)
    discovery.node_db.set_enr(enr)
    lookedup_enr = discovery.lookup_and_maybe_update_enr(privkey.public_key, address)
    assert lookedup_enr == enr


@pytest.mark.trio
async def test_lookup_and_maybe_update_enr_existing_node_different_address():
    discovery = MockDiscoveryService([])
    privkey = PrivateKeyFactory()
    address = AddressFactory()

    # If the address given is different than the one we have in our DB, though, a stub ENR would
    # be created and stored in our DB, replacing the existing one.
    enr = ENRFactory(private_key=privkey.to_bytes(), address=address)
    discovery.node_db.set_enr(enr)
    new_address = AddressFactory()
    lookedup_enr = discovery.lookup_and_maybe_update_enr(privkey.public_key, new_address)
    assert lookedup_enr != enr
    assert lookedup_enr.public_key == enr.public_key
    assert lookedup_enr.sequence_number == 0
    assert Node(lookedup_enr).address == new_address
    assert lookedup_enr == discovery.node_db.get_enr(enr.node_id)


@pytest.mark.trio
async def test_update_routing_table():
    discovery = MockDiscoveryService([])
    node = NodeFactory()

    discovery.update_routing_table(node)

    assert node in discovery.routing


@pytest.mark.trio
async def test_update_routing_table_triggers_bond_if_eviction_candidate(
        manually_driven_discovery, monkeypatch):
    discovery = manually_driven_discovery
    old_node, new_node = NodeFactory.create_batch(2)

    bond_called = False

    async def bond(node):
        nonlocal bond_called
        bond_called = True
        assert node == old_node

    monkeypatch.setattr(discovery, 'bond', bond)
    # Pretend our routing table failed to add the new node by returning the least recently seen
    # node for an eviction check.
    monkeypatch.setattr(discovery.routing, 'add_node', lambda n: old_node)

    discovery.update_routing_table(new_node)

    assert new_node not in discovery.routing
    # The update_routing_table() call above will have scheduled a future call to discovery.bond() so
    # we need to wait a bit here to give it a chance to run.
    with trio.fail_after(0.5):
        while not bond_called:
            await trio.sleep(0.001)


@pytest.mark.trio
async def test_get_max_neighbours_per_packet(nursery):
    # This test is just a safeguard against changes that inadvertently modify the behaviour of
    # _get_max_neighbours_per_packet().
    assert DiscoveryService._get_max_neighbours_per_packet() == 12


def test_discover_v4_message_pack():
    sender, recipient = AddressFactory.create_batch(2)
    version = rlp.sedes.big_endian_int.serialize(PROTO_VERSION)
    payload = (version, sender.to_endpoint(), recipient.to_endpoint())
    privkey = PrivateKeyFactory()

    message = _pack_v4(CMD_PING.id, payload, privkey)

    pubkey, cmd_id, payload, _ = _unpack_v4(message)
    assert pubkey == privkey.public_key
    assert cmd_id == CMD_PING.id


def test_unpack_eip8_packets():
    # Test our _unpack() function against the sample packets specified in
    # https://github.com/ethereum/EIPs/blob/master/EIPS/eip-8.md
    for cmd, packets in eip8_packets.items():
        for _, packet in packets.items():
            pubkey, cmd_id, payload, _ = _unpack_v4(packet)
            assert pubkey.to_hex() == '0xca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd31387574077f301b421bc84df7266c44e9e6d569fc56be00812904767bf5ccd1fc7f'  # noqa: E501
            assert cmd.id == cmd_id


@pytest.mark.trio
async def test_bootstrap_nodes():
    private_key = PrivateKeyFactory().to_bytes()
    bootnode1 = ENRFactory(private_key=private_key)
    bootnode2 = ENRFactory()
    discovery = MockDiscoveryService([Node(bootnode1), Node(bootnode2)])

    assert discovery.node_db.get_enr(bootnode1.node_id) == bootnode1
    assert discovery.node_db.get_enr(bootnode2.node_id) == bootnode2
    assert [node.enr for node in discovery.bootstrap_nodes] == [bootnode1, bootnode2]

    # If our DB gets updated with a newer ENR of one of our bootnodes, the @bootstrap_nodes
    # property will reflect that.
    new_bootnode1 = ENRFactory(
        private_key=private_key, sequence_number=bootnode1.sequence_number + 1)
    discovery.node_db.set_enr(new_bootnode1)
    assert [node.enr for node in discovery.bootstrap_nodes] == [new_bootnode1, bootnode2]


class MockDiscoveryService(DiscoveryService):
    """A DiscoveryService that instead of sending messages over the wire simply stores them in
    self.messages.

    Do not attempt to run it as a service (e.g. with TrioManager.run_service()), because that
    won't work.
    """

    def __init__(self, bootnodes):
        privkey = keys.PrivateKey(keccak(b"seed"))
        self.messages = []
        node_db = NodeDB(default_identity_scheme_registry, MemoryDB())
        socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
        event_bus = None
        address = AddressFactory()
        super().__init__(
            privkey, address.udp_port, address.tcp_port, bootnodes, event_bus, socket, node_db)

    def send(self, node, msg_type, payload):
        # Overwrite our parent's send() to ensure no tests attempt to use us to go over the
        # network as that wouldn't work.
        raise ValueError("MockDiscoveryService must not be used to send network messages")

    async def send_ping_v4(self, node):
        echo = hex(random.randint(0, 2**256))[-32:]
        self.messages.append((node, 'ping', echo))
        return echo

    async def send_pong_v4(self, node, echo):
        self.messages.append((node, 'pong', echo))

    def send_find_node_v4(self, node, nodeid):
        self.messages.append((node, 'find_node', nodeid))

    def send_neighbours_v4(self, node, neighbours):
        self.messages.append((node, 'neighbours', neighbours))


def remove_whitespace(s):
    return re.sub(r"\s+", "", s)


eip8_packets = {
    CMD_PING: dict(
        # ping packet with version 4, additional list elements
        ping1=decode_hex(remove_whitespace("""
        e9614ccfd9fc3e74360018522d30e1419a143407ffcce748de3e22116b7e8dc92ff74788c0b6663a
        aa3d67d641936511c8f8d6ad8698b820a7cf9e1be7155e9a241f556658c55428ec0563514365799a
        4be2be5a685a80971ddcfa80cb422cdd0101ec04cb847f000001820cfa8215a8d790000000000000
        000000000000000000018208ae820d058443b9a3550102""")),

        # ping packet with version 555, additional list elements and additional random data
        ping2=decode_hex(remove_whitespace("""
        577be4349c4dd26768081f58de4c6f375a7a22f3f7adda654d1428637412c3d7fe917cadc56d4e5e
        7ffae1dbe3efffb9849feb71b262de37977e7c7a44e677295680e9e38ab26bee2fcbae207fba3ff3
        d74069a50b902a82c9903ed37cc993c50001f83e82022bd79020010db83c4d001500000000abcdef
        12820cfa8215a8d79020010db885a308d313198a2e037073488208ae82823a8443b9a355c5010203
        040531b9019afde696e582a78fa8d95ea13ce3297d4afb8ba6433e4154caa5ac6431af1b80ba7602
        3fa4090c408f6b4bc3701562c031041d4702971d102c9ab7fa5eed4cd6bab8f7af956f7d565ee191
        7084a95398b6a21eac920fe3dd1345ec0a7ef39367ee69ddf092cbfe5b93e5e568ebc491983c09c7
        6d922dc3""")),
    ),

    CMD_PONG: dict(
        # pong packet with additional list elements and additional random data
        pong=decode_hex(remove_whitespace("""
        09b2428d83348d27cdf7064ad9024f526cebc19e4958f0fdad87c15eb598dd61d08423e0bf66b206
        9869e1724125f820d851c136684082774f870e614d95a2855d000f05d1648b2d5945470bc187c2d2
        216fbe870f43ed0909009882e176a46b0102f846d79020010db885a308d313198a2e037073488208
        ae82823aa0fbc914b16819237dcd8801d7e53f69e9719adecb3cc0e790c57e91ca4461c9548443b9
        a355c6010203c2040506a0c969a58f6f9095004c0177a6b47f451530cab38966a25cca5cb58f0555
        42124e""")),
    ),

    CMD_FIND_NODE: dict(
        # findnode packet with additional list elements and additional random data
        findnode=decode_hex(remove_whitespace("""
        c7c44041b9f7c7e41934417ebac9a8e1a4c6298f74553f2fcfdcae6ed6fe53163eb3d2b52e39fe91
        831b8a927bf4fc222c3902202027e5e9eb812195f95d20061ef5cd31d502e47ecb61183f74a504fe
        04c51e73df81f25c4d506b26db4517490103f84eb840ca634cae0d49acb401d8a4c6b6fe8c55b70d
        115bf400769cc1400f3258cd31387574077f301b421bc84df7266c44e9e6d569fc56be0081290476
        7bf5ccd1fc7f8443b9a35582999983999999280dc62cc8255c73471e0a61da0c89acdc0e035e260a
        dd7fc0c04ad9ebf3919644c91cb247affc82b69bd2ca235c71eab8e49737c937a2c396""")),
    ),

    CMD_NEIGHBOURS: dict(
        # neighbours packet with additional list elements and additional random data
        neighbours=decode_hex(remove_whitespace("""
        c679fc8fe0b8b12f06577f2e802d34f6fa257e6137a995f6f4cbfc9ee50ed3710faf6e66f932c4c8
        d81d64343f429651328758b47d3dbc02c4042f0fff6946a50f4a49037a72bb550f3a7872363a83e1
        b9ee6469856c24eb4ef80b7535bcf99c0004f9015bf90150f84d846321163782115c82115db84031
        55e1427f85f10a5c9a7755877748041af1bcd8d474ec065eb33df57a97babf54bfd2103575fa8291
        15d224c523596b401065a97f74010610fce76382c0bf32f84984010203040101b840312c55512422
        cf9b8a4097e9a6ad79402e87a15ae909a4bfefa22398f03d20951933beea1e4dfa6f968212385e82
        9f04c2d314fc2d4e255e0d3bc08792b069dbf8599020010db83c4d001500000000abcdef12820d05
        820d05b84038643200b172dcfef857492156971f0e6aa2c538d8b74010f8e140811d53b98c765dd2
        d96126051913f44582e8c199ad7c6d6819e9a56483f637feaac9448aacf8599020010db885a308d3
        13198a2e037073488203e78203e8b8408dcab8618c3253b558d459da53bd8fa68935a719aff8b811
        197101a4b2b47dd2d47295286fc00cc081bb542d760717d1bdd6bec2c37cd72eca367d6dd3b9df73
        8443b9a355010203b525a138aa34383fec3d2719a0""")),
    ),
}
