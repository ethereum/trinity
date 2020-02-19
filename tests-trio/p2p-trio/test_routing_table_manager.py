from eth_utils.toolz import (
    sliding_window,
)

from hypothesis import (
    given,
    strategies as st,
)

import pytest

import pytest_trio

from async_service import background_trio_service
import trio
from trio.testing import (
    wait_all_tasks_blocked,
)

from eth.db.backends.memory import MemoryDB

from p2p.discv5.channel_services import (
    IncomingMessage,
)
from p2p.discv5.constants import (
    ROUTING_TABLE_PING_INTERVAL,
)
from p2p.discv5.enr_db import (
    NodeDB,
)
from p2p.discv5.identity_schemes import (
    default_identity_scheme_registry,
)
from p2p.discv5.messages import (
    FindNodeMessage,
    NodesMessage,
    PingMessage,
    PongMessage,
)
from p2p.discv5.message_dispatcher import (
    MessageDispatcher,
)
from p2p.discv5.routing_table import (
    compute_distance,
    compute_log_distance,
    KademliaRoutingTable,
)
from p2p.discv5.routing_table_manager import (
    iter_closest_nodes,
    partition_enr_indices_by_size,
    FindNodeHandlerService,
    PingHandlerService,
    PingSenderService,
)

from p2p.tools.factories.discovery import (
    EndpointFactory,
    ENRFactory,
    FindNodeMessageFactory,
    NodeIDFactory,
    IncomingMessageFactory,
    PingMessageFactory,
)


@pytest.fixture
def incoming_message_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def outgoing_message_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def endpoint_vote_channels():
    return trio.open_memory_channel(0)


@pytest.fixture
def local_enr():
    return ENRFactory()


@pytest.fixture
def remote_enr(remote_endpoint):
    return ENRFactory(
        custom_kv_pairs={
            b"ip": remote_endpoint.ip_address,
            b"udp": remote_endpoint.port,
        },
    )


@pytest.fixture
def remote_endpoint():
    return EndpointFactory()


@pytest.fixture
def empty_routing_table(local_enr):
    routing_table = KademliaRoutingTable(local_enr.node_id, 16)
    return routing_table


@pytest.fixture
def routing_table(empty_routing_table, remote_enr):
    empty_routing_table.update(remote_enr.node_id)
    return empty_routing_table


@pytest_trio.trio_fixture
async def filled_routing_table(routing_table, node_db):
    # add entries until the first bucket is full
    while len(routing_table.get_nodes_at_log_distance(255)) < routing_table.bucket_size:
        enr = ENRFactory()
        routing_table.update(enr.node_id)
        node_db.set_enr(enr)
    return routing_table


@pytest_trio.trio_fixture
async def node_db(local_enr, remote_enr):
    node_db = NodeDB(default_identity_scheme_registry, MemoryDB())
    node_db.set_enr(local_enr)
    node_db.set_enr(remote_enr)
    return node_db


@pytest_trio.trio_fixture
async def message_dispatcher(node_db, incoming_message_channels, outgoing_message_channels):
    message_dispatcher = MessageDispatcher(
        node_db=node_db,
        incoming_message_receive_channel=incoming_message_channels[1],
        outgoing_message_send_channel=outgoing_message_channels[0],
    )
    async with background_trio_service(message_dispatcher):
        yield message_dispatcher


@pytest_trio.trio_fixture
async def ping_handler_service(local_enr,
                               routing_table,
                               message_dispatcher,
                               node_db,
                               incoming_message_channels,
                               outgoing_message_channels):
    ping_handler_service = PingHandlerService(
        local_node_id=local_enr.node_id,
        routing_table=routing_table,
        message_dispatcher=message_dispatcher,
        node_db=node_db,
        outgoing_message_send_channel=outgoing_message_channels[0],
    )
    async with background_trio_service(ping_handler_service):
        yield ping_handler_service


@pytest_trio.trio_fixture
async def find_node_handler_service(local_enr,
                                    routing_table,
                                    message_dispatcher,
                                    node_db,
                                    incoming_message_channels,
                                    outgoing_message_channels,
                                    endpoint_vote_channels):
    find_node_handler_service = FindNodeHandlerService(
        local_node_id=local_enr.node_id,
        routing_table=routing_table,
        message_dispatcher=message_dispatcher,
        node_db=node_db,
        outgoing_message_send_channel=outgoing_message_channels[0],
    )
    async with background_trio_service(find_node_handler_service):
        yield find_node_handler_service


@pytest_trio.trio_fixture
async def ping_sender_service(local_enr,
                              routing_table,
                              message_dispatcher,
                              node_db,
                              incoming_message_channels,
                              endpoint_vote_channels):
    ping_sender_service = PingSenderService(
        local_node_id=local_enr.node_id,
        routing_table=routing_table,
        message_dispatcher=message_dispatcher,
        node_db=node_db,
        endpoint_vote_send_channel=endpoint_vote_channels[0],
    )
    async with background_trio_service(ping_sender_service):
        yield ping_sender_service


@pytest.mark.trio
async def test_ping_handler_sends_pong(ping_handler_service,
                                       incoming_message_channels,
                                       outgoing_message_channels,
                                       local_enr):
    ping = PingMessageFactory()
    incoming_message = IncomingMessageFactory(message=ping)
    await incoming_message_channels[0].send(incoming_message)
    await wait_all_tasks_blocked()

    outgoing_message = outgoing_message_channels[1].receive_nowait()
    assert isinstance(outgoing_message.message, PongMessage)
    assert outgoing_message.message.request_id == ping.request_id
    assert outgoing_message.message.enr_seq == local_enr.sequence_number
    assert outgoing_message.receiver_endpoint == incoming_message.sender_endpoint
    assert outgoing_message.receiver_node_id == incoming_message.sender_node_id


@pytest.mark.trio
async def test_ping_handler_updates_routing_table(ping_handler_service,
                                                  incoming_message_channels,
                                                  outgoing_message_channels,
                                                  local_enr,
                                                  remote_enr,
                                                  routing_table):
    distance = compute_log_distance(remote_enr.node_id, local_enr.node_id)
    other_node_id = NodeIDFactory.at_log_distance(local_enr.node_id, distance)
    routing_table.update(other_node_id)
    assert routing_table.get_nodes_at_log_distance(distance) == (other_node_id, remote_enr.node_id)

    ping = PingMessageFactory()
    incoming_message = IncomingMessageFactory(
        message=ping,
        sender_node_id=remote_enr.node_id,
    )
    await incoming_message_channels[0].send(incoming_message)
    await wait_all_tasks_blocked()

    assert routing_table.get_nodes_at_log_distance(distance) == (remote_enr.node_id, other_node_id)


@pytest.mark.trio
async def test_ping_handler_requests_updated_enr(ping_handler_service,
                                                 incoming_message_channels,
                                                 outgoing_message_channels,
                                                 local_enr,
                                                 remote_enr,
                                                 routing_table):
    ping = PingMessageFactory(enr_seq=remote_enr.sequence_number + 1)
    incoming_message = IncomingMessageFactory(message=ping)
    await incoming_message_channels[0].send(incoming_message)

    await wait_all_tasks_blocked()
    first_outgoing_message = outgoing_message_channels[1].receive_nowait()
    await wait_all_tasks_blocked()
    second_outgoing_message = outgoing_message_channels[1].receive_nowait()

    assert {
        first_outgoing_message.message.__class__,
        second_outgoing_message.message.__class__,
    } == {PongMessage, FindNodeMessage}

    outgoing_find_node = (
        first_outgoing_message if isinstance(first_outgoing_message.message, FindNodeMessage)
        else second_outgoing_message
    )

    assert outgoing_find_node.message.distance == 0
    assert outgoing_find_node.receiver_endpoint == incoming_message.sender_endpoint
    assert outgoing_find_node.receiver_node_id == incoming_message.sender_node_id


@pytest.mark.trio
async def test_find_node_handler_sends_nodes(find_node_handler_service,
                                             incoming_message_channels,
                                             outgoing_message_channels,
                                             local_enr):
    find_node = FindNodeMessageFactory(distance=0)
    incoming_message = IncomingMessageFactory(message=find_node)
    await incoming_message_channels[0].send(incoming_message)
    await wait_all_tasks_blocked()

    outgoing_message = outgoing_message_channels[1].receive_nowait()
    assert isinstance(outgoing_message.message, NodesMessage)
    assert outgoing_message.message.request_id == find_node.request_id
    assert outgoing_message.message.total == 1
    assert outgoing_message.message.enrs == (local_enr,)


@pytest.mark.trio
async def test_find_node_handler_sends_remote_enrs(find_node_handler_service,
                                                   incoming_message_channels,
                                                   outgoing_message_channels,
                                                   local_enr,
                                                   remote_enr):
    distance = compute_log_distance(local_enr.node_id, remote_enr.node_id)
    find_node = FindNodeMessageFactory(distance=distance)
    incoming_message = IncomingMessageFactory(message=find_node)
    await incoming_message_channels[0].send(incoming_message)
    await wait_all_tasks_blocked()

    outgoing_message = outgoing_message_channels[1].receive_nowait()
    assert isinstance(outgoing_message.message, NodesMessage)
    assert outgoing_message.message.request_id == find_node.request_id
    assert outgoing_message.message.total == 1
    assert outgoing_message.message.enrs == (remote_enr,)


@pytest.mark.trio
async def test_find_node_handler_sends_many_remote_enrs(find_node_handler_service,
                                                        incoming_message_channels,
                                                        outgoing_message_channels,
                                                        filled_routing_table,
                                                        node_db):
    distance = 255
    node_ids = filled_routing_table.get_nodes_at_log_distance(distance)
    assert len(node_ids) == filled_routing_table.bucket_size
    enrs = [node_db.get_enr(node_id) for node_id in node_ids]

    find_node = FindNodeMessageFactory(distance=distance)
    incoming_message = IncomingMessageFactory(message=find_node)
    await incoming_message_channels[0].send(incoming_message)

    outgoing_messages = []
    while True:
        await wait_all_tasks_blocked()
        try:
            outgoing_messages.append(outgoing_message_channels[1].receive_nowait())
        except trio.WouldBlock:
            break

    for outgoing_message in outgoing_messages:
        assert isinstance(outgoing_message.message, NodesMessage)
        assert outgoing_message.message.request_id == find_node.request_id
        assert outgoing_message.message.total == len(outgoing_messages)
        assert outgoing_message.message.enrs
    sent_enrs = [
        enr
        for outgoing_message in outgoing_messages
        for enr in outgoing_message.message.enrs
    ]
    assert sent_enrs == enrs


@pytest.mark.trio
async def test_find_node_handler_sends_empty(find_node_handler_service,
                                             incoming_message_channels,
                                             outgoing_message_channels,
                                             routing_table,
                                             node_db):
    distance = 5
    assert len(routing_table.get_nodes_at_log_distance(distance)) == 0

    find_node = FindNodeMessageFactory(distance=distance)
    incoming_message = IncomingMessageFactory(message=find_node)
    await incoming_message_channels[0].send(incoming_message)

    await wait_all_tasks_blocked()
    outgoing_message = outgoing_message_channels[1].receive_nowait()

    assert isinstance(outgoing_message.message, NodesMessage)
    assert outgoing_message.message.request_id == find_node.request_id
    assert outgoing_message.message.total == 1
    assert not outgoing_message.message.enrs


@pytest.mark.trio
async def test_send_ping(ping_sender_service,
                         routing_table,
                         incoming_message_channels,
                         outgoing_message_channels,
                         local_enr,
                         remote_enr,
                         remote_endpoint):
    with trio.fail_after(ROUTING_TABLE_PING_INTERVAL):
        outgoing_message = await outgoing_message_channels[1].receive()

    assert isinstance(outgoing_message.message, PingMessage)
    assert outgoing_message.receiver_endpoint == remote_endpoint
    assert outgoing_message.receiver_node_id == remote_enr.node_id


@pytest.mark.trio
async def test_send_endpoint_vote(ping_sender_service,
                                  routing_table,
                                  incoming_message_channels,
                                  outgoing_message_channels,
                                  endpoint_vote_channels,
                                  local_enr,
                                  remote_enr,
                                  remote_endpoint):
    # wait for ping
    with trio.fail_after(ROUTING_TABLE_PING_INTERVAL):
        outgoing_message = await outgoing_message_channels[1].receive()
    ping = outgoing_message.message

    # respond with pong
    fake_local_endpoint = EndpointFactory()
    pong = PongMessage(
        request_id=ping.request_id,
        enr_seq=0,
        packet_ip=fake_local_endpoint.ip_address,
        packet_port=fake_local_endpoint.port,
    )
    incoming_message = IncomingMessage(
        message=pong,
        sender_endpoint=outgoing_message.receiver_endpoint,
        sender_node_id=outgoing_message.receiver_node_id,
    )
    await incoming_message_channels[0].send(incoming_message)
    await wait_all_tasks_blocked()

    # receive endpoint vote
    endpoint_vote = endpoint_vote_channels[1].receive_nowait()
    assert endpoint_vote.endpoint == fake_local_endpoint
    assert endpoint_vote.node_id == incoming_message.sender_node_id


@given(
    sizes=st.lists(st.integers(min_value=1)),
    max_size=st.integers(min_value=1),
)
def test_enr_partitioning(sizes, max_size):
    partitions = partition_enr_indices_by_size(sizes, max_size)
    indices = [index for partition in partitions for index in partition]
    dropped_indices = tuple(index for index, size in enumerate(sizes) if size > max_size)

    assert len(indices) == len(set(indices))
    assert set(indices) == set(range(len(sizes))) - set(dropped_indices)
    assert all(index1 < index2 for index1, index2 in sliding_window(2, indices))

    partitioned_sizes = tuple(
        tuple(sizes[index] for index in partition)
        for partition in partitions
    )
    assert all(sum(partition) <= max_size for partition in partitioned_sizes)
    assert all(
        sum(partition) + next_partition[0] > max_size
        for partition, next_partition in sliding_window(2, partitioned_sizes)
    )


def test_closest_nodes_empty(empty_routing_table):
    target = NodeIDFactory()
    assert list(iter_closest_nodes(target, empty_routing_table, [])) == []


def test_closest_nodes_only_routing(empty_routing_table):
    target = NodeIDFactory()
    nodes = [NodeIDFactory() for _ in range(10)]
    for node in nodes:
        empty_routing_table.update(node)

    closest_nodes = list(iter_closest_nodes(target, empty_routing_table, []))
    assert closest_nodes == sorted(nodes, key=lambda node: compute_distance(target, node))


def test_closest_nodes_only_additional(empty_routing_table):
    target = NodeIDFactory()
    nodes = [NodeIDFactory() for _ in range(10)]
    closest_nodes = list(iter_closest_nodes(target, empty_routing_table, nodes))
    assert closest_nodes == sorted(nodes, key=lambda node: compute_distance(target, node))


def test_lookup_generator_mixed(empty_routing_table):
    target = NodeIDFactory()
    nodes = sorted(
        [NodeIDFactory() for _ in range(10)],
        key=lambda node: compute_distance(node, target)
    )
    nodes_in_routing_table = nodes[:3] + nodes[6:8]
    nodes_in_additional = nodes[3:6] + nodes[8:]
    for node in nodes_in_routing_table:
        empty_routing_table.update(node)
    closest_nodes = list(iter_closest_nodes(target, empty_routing_table, nodes_in_additional))
    assert closest_nodes == nodes
