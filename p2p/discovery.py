"""
The Node Discovery protocol provides a way to find RLPx nodes that can be connected to. It uses a
Kademlia-like protocol to maintain a distributed database of the IDs and endpoints of all
listening nodes.

More information at https://github.com/ethereum/devp2p/blob/master/rlpx.md#node-discovery
"""
import collections
import random
import time
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    TYPE_CHECKING,
)
from typing_extensions import Literal

from async_generator import aclosing

import trio

import eth_utils.toolz
from eth_utils import (
    ExtendedDebugLogger,
    get_extended_debug_logger,
)

from lahja import (
    EndpointAPI,
)

import rlp
from rlp.exceptions import DeserializationError

from eth_typing import BlockNumber, Hash32

from eth_utils import (
    encode_hex,
    to_bytes,
    to_hex,
    to_list,
    to_tuple,
    int_to_big_endian,
    big_endian_to_int,
    ValidationError,
)

from eth_keys import keys
from eth_keys import datatypes

from eth_hash.auto import keccak

from async_service import Service

from eth.abc import VirtualMachineAPI
from eth.constants import GENESIS_BLOCK_NUMBER

from p2p import constants
from p2p.abc import AddressAPI, ENR_FieldProvider, NodeAPI
from p2p.discv5.abc import EnrDbApi
from p2p.discv5.enr import ENR, UnsignedENR, IDENTITY_SCHEME_ENR_KEY
from p2p.discv5.identity_schemes import V4IdentityScheme
from p2p.discv5.constants import (
    IP_V4_ADDRESS_ENR_KEY,
    TCP_PORT_ENR_KEY,
    UDP_PORT_ENR_KEY,
)
from p2p.discv5.typing import NodeID
from p2p.events import (
    PeerCandidatesRequest,
    RandomBootnodeRequest,
    BaseRequestResponseEvent,
    PeerCandidatesResponse,
)
from p2p.exceptions import AlreadyWaitingDiscoveryResponse, CouldNotRetrieveENR, NoEligibleNodes
from p2p.forkid import ForkID, make_forkid
from p2p.kademlia import Address, Node, RoutingTable, check_relayed_addr, sort_by_distance
from p2p import trio_utils

if TYPE_CHECKING:
    from trinity.db.eth1.header import BaseAsyncHeaderDB


# V4 handler are async methods that take a Node, payload and msg_hash as arguments.
V4_HANDLER_TYPE = Callable[[NodeAPI, Tuple[Any, ...], Hash32], Awaitable[Any]]

# UDP packet constants.
MAC_SIZE = 256 // 8  # 32
SIG_SIZE = 520 // 8  # 65
HEAD_SIZE = MAC_SIZE + SIG_SIZE  # 97
EXPIRATION = 60  # let messages expire after N secondes
PROTO_VERSION = 4


class DefectiveMessage(Exception):
    pass


class WrongMAC(DefectiveMessage):
    pass


class DiscoveryCommand:
    def __init__(self, name: str, id: int) -> None:
        self.name = name
        self.id = id

    def __repr__(self) -> str:
        return 'Command(%s:%d)' % (self.name, self.id)


CMD_PING = DiscoveryCommand("ping", 1)
CMD_PONG = DiscoveryCommand("pong", 2)
CMD_FIND_NODE = DiscoveryCommand("find_node", 3)
CMD_NEIGHBOURS = DiscoveryCommand("neighbours", 4)
CMD_ENR_REQUEST = DiscoveryCommand("enr_request", 5)
CMD_ENR_RESPONSE = DiscoveryCommand("enr_response", 6)
CMD_ID_MAP = dict(
    (cmd.id, cmd)
    for cmd in [
        CMD_PING, CMD_PONG, CMD_FIND_NODE, CMD_NEIGHBOURS, CMD_ENR_REQUEST, CMD_ENR_RESPONSE])


class DiscoveryService(Service):
    _refresh_interval: int = 30
    _max_neighbours_per_packet_cache = None
    # Maximum number of ENR retrieval requests active at any moment. Need to be a relatively high
    # number as during a lookup() we'll bond and fetch ENRs of many nodes.
    _max_pending_enrs: int = 20
    _local_enr_refresh_interval: int = 60

    logger = get_extended_debug_logger('p2p.discovery.DiscoveryService')

    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 address: AddressAPI,
                 bootstrap_nodes: Sequence[NodeAPI],
                 event_bus: EndpointAPI,
                 socket: trio.socket.SocketType,
                 enr_db: EnrDbApi,
                 enr_field_providers: Sequence[ENR_FieldProvider] = tuple(),
                 ) -> None:
        self.privkey = privkey
        self.bootstrap_nodes = bootstrap_nodes
        self._event_bus = event_bus
        self.enr_response_channels = ExpectedResponseChannels[Tuple[ENR, Hash32]]()
        self.pong_channels = ExpectedResponseChannels[Tuple[Hash32, int]]()
        self.neighbours_channels = ExpectedResponseChannels[List[NodeAPI]]()
        self.ping_channels = ExpectedResponseChannels[None]()
        self.enr_field_providers = enr_field_providers
        self._enr_db = enr_db
        # FIXME: Use a concurrency-safe EnrDb implementation.
        self._enr_db_lock = trio.Lock()
        self._local_enr_next_refresh: float = time.monotonic()
        self._local_enr_lock = trio.Lock()
        self.parity_pong_tokens: Dict[Hash32, Hash32] = {}
        if socket.family != trio.socket.AF_INET:
            raise ValueError("Invalid socket family")
        elif socket.type != trio.socket.SOCK_DGRAM:
            raise ValueError("Invalid socket type")
        self.socket = socket
        self.pending_enrs_producer, self.pending_enrs_consumer = trio.open_memory_channel[
            Tuple[NodeAPI, int]](self._max_pending_enrs)

        # This is a stub Node instance (which will have an ENR with sequence number 0) that will
        # be replaced in _init(). We could defer its creation until _init() is called by run(),
        # but doing this here simplifies things as we don't have to store the Address in another
        # instance attribute.
        self.this_node = Node.from_pubkey_and_addr(self.pubkey, address)
        self.routing = RoutingTable(self.this_node.id)

    async def _init(self) -> None:
        async with self._enr_db_lock:
            try:
                enr = await self._enr_db.get(NodeID(self.this_node.id_bytes))
            except KeyError:
                # Either we have a fresh DB or our private key has changed, so create a new ENR
                # with sequence number 1.
                enr = await self._generate_local_enr(sequence_number=1)
                await self._enr_db.insert(enr)

        self.this_node = Node(enr)
        # This is a no-op when we generate a fresh ENR above, but we need to run this without
        # acquiring self._enr_db_lock, so we run it here for simplicity.
        await self._maybe_update_local_enr()

    async def consume_datagrams(self) -> None:
        while self.manager.is_running:
            await self.consume_datagram()

    async def handle_get_peer_candidates_requests(self) -> None:
        async for event in self._event_bus.stream(PeerCandidatesRequest):
            nodes = tuple(self.get_nodes_to_connect(event.max_candidates))
            self.logger.debug2("Broadcasting peer candidates (%s)", nodes)
            await self._event_bus.broadcast(
                event.expected_response_type()(nodes),
                event.broadcast_config()
            )

    async def handle_get_random_bootnode_requests(self) -> None:
        async for event in self._event_bus.stream(RandomBootnodeRequest):

            nodes = tuple(self.get_random_bootnode())

            self.logger.debug2("Broadcasting random boot nodes (%s)", nodes)
            await self._event_bus.broadcast(
                event.expected_response_type()(nodes),
                event.broadcast_config()
            )

    async def run(self) -> None:
        await self._init()
        self.logger.info("Running on %s", self.this_node.uri())
        self.run_daemons_and_bootstrap()
        await self.manager.wait_finished()

    def run_daemons_and_bootstrap(self) -> None:
        self.manager.run_daemon_task(self.handle_get_peer_candidates_requests)
        self.manager.run_daemon_task(self.handle_get_random_bootnode_requests)
        self.manager.run_daemon_task(self.periodically_refresh)
        self.manager.run_daemon_task(self.report_stats)
        self.manager.run_daemon_task(self.fetch_enrs)
        self.manager.run_daemon_task(self.consume_datagrams)
        self.manager.run_task(self.bootstrap)

    async def periodically_refresh(self) -> None:
        async for _ in trio_utils.every(self._refresh_interval):
            await self.lookup_random()

    async def fetch_enrs(self) -> None:
        async with self.pending_enrs_consumer:
            async for (remote, enr_seq) in self.pending_enrs_consumer:
                self.logger.debug2("Received request to fetch ENR for %s", remote)
                self.manager.run_task(self._ensure_enr, remote, enr_seq)

    async def _ensure_enr(self, node: NodeAPI, enr_seq: int) -> None:
        # TODO: Check that we've recently bonded with the remote. For now it shouldn't be a
        # problem as this is only triggered once we successfully bonded with a peer.
        async with self._enr_db_lock:
            try:
                enr = await self._enr_db.get(NodeID(node.id_bytes))
            except KeyError:
                pass
            else:
                if enr.sequence_number >= enr_seq:
                    self.logger.debug2("Already got latest ENR for %s", node)
                    return

        try:
            await self.get_enr(node)
        except CouldNotRetrieveENR as e:
            self.logger.debug("Failed to retrieve ENR for %s: %s", node, e)

    async def report_stats(self) -> None:
        async for _ in trio_utils.every(self._refresh_interval):
            self.logger.debug("============================= Stats =======================")
            full_buckets = [bucket for bucket in self.routing.buckets if bucket.is_full]
            total_nodes = sum([len(bucket) for bucket in self.routing.buckets])
            nodes_in_replacement_cache = sum(
                [len(bucket.replacement_cache) for bucket in self.routing.buckets])
            self.logger.debug(
                "Routing table has %s nodes in %s buckets (%s of which are full), and %s nodes "
                "are in the replacement cache", total_nodes, len(self.routing.buckets),
                len(full_buckets), nodes_in_replacement_cache)
            self.logger.debug("ENR DB has a total of %s entries", len(self._enr_db))
            self.logger.debug("===========================================================")

    def update_routing_table(self, node: NodeAPI) -> None:
        """Update the routing table entry for the given node."""
        eviction_candidate = self.routing.add_node(node)
        if eviction_candidate:
            # This means we couldn't add the node because its bucket is full, so schedule a bond()
            # with the least recently seen node on that bucket. If the bonding fails the node will
            # be removed from the bucket and a new one will be picked from the bucket's
            # replacement cache.
            self.logger.debug2(
                "Routing table's bucket is full, couldn't add %s. "
                "Checking if %s is still responding, will evict if not", node, eviction_candidate)
            self.manager.run_task(self.bond, eviction_candidate)

    async def bond(self, node: NodeAPI) -> bool:
        """Bond with the given node.

        Bonding consists of pinging the node, waiting for a pong and maybe a ping as well.
        It is necessary to do this at least once before we send find_node requests to a node.
        """
        if node == self.this_node:
            # FIXME: We should be able to get rid of this check, but for now issue a warning.
            self.logger.warning("Attempted to bond with self; this shouldn't happen")
            return False

        if node.is_bond_valid:
            return True

        token = await self.send_ping_v4(node)
        send_chan, recv_chan = trio.open_memory_channel[Tuple[Hash32, int]](1)
        try:
            with trio.fail_after(constants.KADEMLIA_REQUEST_TIMEOUT):
                received_token, enr_seq = await self.pong_channels.receive_one(
                    node, send_chan, recv_chan)
        except AlreadyWaitingDiscoveryResponse:
            self.logger.debug("Bonding failed, already waiting pong from %s", node)
            return False
        except trio.TooSlowError:
            self.logger.debug("Bonding with %s timed out", node)
            return False

        if received_token != token:
            self.logger.info(
                "Bonding with %s failed, expected pong with token %s, but got %s",
                node, token, received_token)
            self.routing.remove_node(node)
            return False

        ping_send_chan, ping_recv_chan = trio.open_memory_channel[None](1)
        try:
            # Give the remote node a chance to ping us before we move on and
            # start sending find_node requests. It is ok for us to timeout
            # here as that just means the remote remembers us -- that's why we use
            # move_on_after() instead of fail_after().
            with trio.move_on_after(constants.KADEMLIA_REQUEST_TIMEOUT):
                await self.ping_channels.receive_one(node, ping_send_chan, ping_recv_chan)
        except AlreadyWaitingDiscoveryResponse:
            self.logger.debug("bonding failed, already waiting for ping")
            return False

        self.logger.debug("bonding completed successfully with %s", node)
        self.update_routing_table(node)
        if enr_seq is not None:
            self.schedule_enr_retrieval(node, enr_seq)
        return True

    def schedule_enr_retrieval(self, node: NodeAPI, enr_seq: int) -> None:
        self.logger.debug("scheduling ENR retrieval from %s", node)
        try:
            self.pending_enrs_producer.send_nowait((node, enr_seq))
        except trio.WouldBlock:
            self.logger.warning("Failed to schedule ENR retrieval; channel buffer is full")

    async def request_enr(self, remote: NodeAPI) -> ENR:
        # No need to use a timeout because bond() takes care of that internally.
        await self.bond(remote)
        token = self.send_enr_request(remote)
        send_chan, recv_chan = trio.open_memory_channel[Tuple[ENR, Hash32]](1)
        try:
            with trio.fail_after(constants.KADEMLIA_REQUEST_TIMEOUT):
                enr, received_token = await self.enr_response_channels.receive_one(
                    remote, send_chan, recv_chan)
        except trio.TooSlowError:
            raise CouldNotRetrieveENR(f"Timed out waiting for ENR from {remote}")
        except AlreadyWaitingDiscoveryResponse:
            raise CouldNotRetrieveENR(f"Already waiting for ENR from {remote}")

        if received_token != token:
            raise CouldNotRetrieveENR(
                f"Got ENR from {remote} with token {received_token!r} but expected {token!r}")

        return enr

    async def _generate_local_enr(self, sequence_number: int) -> ENR:
        kv_pairs = {
            IDENTITY_SCHEME_ENR_KEY: V4IdentityScheme.id,
            V4IdentityScheme.public_key_enr_key: self.pubkey.to_compressed_bytes(),
            IP_V4_ADDRESS_ENR_KEY: self.this_node.address.ip_packed,
            UDP_PORT_ENR_KEY: self.this_node.address.udp_port,
            TCP_PORT_ENR_KEY: self.this_node.address.tcp_port,
        }
        for field_provider in self.enr_field_providers:
            key, value = await field_provider()
            if key in kv_pairs:
                raise AssertionError(
                    "ENR field provider attempted to override already used key: %s", key)
            kv_pairs[key] = value
        unsigned_enr = UnsignedENR(sequence_number, kv_pairs)
        return unsigned_enr.to_signed_enr(self.privkey.to_bytes())

    async def get_local_enr(self) -> ENR:
        """
        Get our own ENR.

        If the cached version of our ENR (self.this_node.enr) has been updated less than
        self._local_enr_next_refresh seconds ago, return it. Otherwise we generate a fresh ENR
        (with our current sequence number), compare it to the current one and then either:

          1. Return the current one if they're identical
          2. Create a new ENR with a new sequence number (current + 1), update self.this_node with
             it and return it.
        """
        if self._local_enr_next_refresh <= time.monotonic():
            async with self._local_enr_lock:
                await self._maybe_update_local_enr()

        return self.this_node.enr

    async def _maybe_update_local_enr(self) -> None:
        self._local_enr_next_refresh = time.monotonic() + self._local_enr_refresh_interval

        # Re-generate our ENR from scratch and compare it to the current one to see if we
        # must create a new one with a higher sequence number.
        current_enr = await self._generate_local_enr(self.this_node.enr.sequence_number)
        if current_enr == self.this_node.enr:
            return

        # Either our node's details (e.g. IP address) or one of our ENR fields have changed, so
        # generate a new one with a higher sequence number.
        enr = await self._generate_local_enr(self.this_node.enr.sequence_number + 1)
        self.this_node.enr = enr
        self.logger.info(
            "Node details changed, generated new local ENR with sequence number %d",
            enr.sequence_number)

        async with self._enr_db_lock:
            await self._enr_db.update(enr)

    async def get_local_enr_seq(self) -> int:
        enr = await self.get_local_enr()
        return enr.sequence_number

    async def get_enr(self, remote: NodeAPI) -> ENR:
        """Get the most recent ENR for the given node and update our local DB if necessary.

        Raises CouldNotRetrieveENR if we can't get a ENR from the remote node.
        """
        enr = await self.request_enr(remote)
        self.logger.debug2("Got ENR with seq-id %s for %s", enr.sequence_number, remote)
        async with self._enr_db_lock:
            await self._enr_db.insert_or_update(enr)
            return await self._enr_db.get(NodeID(remote.id_bytes))

    async def wait_neighbours(self, remote: NodeAPI) -> Tuple[NodeAPI, ...]:
        """Wait for a neihgbours packet from the given node.

        Returns the list of neighbours received.
        """
        neighbours: List[NodeAPI] = []
        send_chan, recv_chan = trio.open_memory_channel[List[NodeAPI]](1)
        with trio.move_on_after(constants.KADEMLIA_REQUEST_TIMEOUT) as cancel_scope:
            # Responses to a FIND_NODE request are usually split between multiple
            # NEIGHBOURS packets, so we may have to read from the channel multiple times.
            gen = self.neighbours_channels.receive(remote, send_chan, recv_chan)
            # mypy thinks wrapping our generator turns it into something else, so ignore.
            async with aclosing(gen):  # type: ignore
                async for batch in gen:
                    self.logger.debug2(
                        f'got expected neighbours response from {remote}: {batch}')
                    neighbours.extend(batch)
                    if len(neighbours) >= constants.KADEMLIA_BUCKET_SIZE:
                        break
            self.logger.debug2(f'got expected neighbours response from {remote}')
        if cancel_scope.cancelled_caught:
            self.logger.debug2(
                f'timed out waiting for {constants.KADEMLIA_BUCKET_SIZE} neighbours from '
                f'{remote}, got only {len(neighbours)}')
        return tuple(n for n in neighbours if n != self.this_node)

    def _send_find_node(self, node: NodeAPI, target_node_id: int) -> None:
        self.send_find_node_v4(node, target_node_id)

    async def lookup(self, node_id: int) -> Tuple[NodeAPI, ...]:
        """Lookup performs a network search for nodes close to the given target.

        It approaches the target by querying nodes that are closer to it on each iteration.  The
        given target does not need to be an actual node identifier.
        """
        nodes_asked: Set[NodeAPI] = set()
        nodes_seen: Set[NodeAPI] = set()

        async def _find_node(node_id: int, remote: NodeAPI) -> Tuple[NodeAPI, ...]:
            self._send_find_node(remote, node_id)
            candidates = await self.wait_neighbours(remote)
            if not candidates:
                self.logger.debug("got no candidates from %s, returning", remote)
                return tuple()
            all_candidates = tuple(c for c in candidates if c not in nodes_seen)
            candidates = tuple(
                c for c in all_candidates
                if (not self.ping_channels.already_waiting_for(c) and
                    not self.pong_channels.already_waiting_for(c))
            )
            self.logger.debug2("got %s new candidates", len(candidates))
            # Add new candidates to nodes_seen so that we don't attempt to bond with failing ones
            # in the future.
            nodes_seen.update(candidates)
            bonded = await trio_utils.gather(*((self.bond, c) for c in candidates))
            self.logger.debug2("bonded with %s candidates", bonded.count(True))
            return tuple(c for c in candidates if bonded[candidates.index(c)])

        def _exclude_if_asked(nodes: Iterable[NodeAPI]) -> List[NodeAPI]:
            nodes_to_ask = list(set(nodes).difference(nodes_asked))
            return sort_by_distance(nodes_to_ask, node_id)[:constants.KADEMLIA_FIND_CONCURRENCY]

        closest = self.routing.neighbours(node_id)
        self.logger.debug("starting lookup; initial neighbours: %s", closest)
        nodes_to_ask = _exclude_if_asked(closest)
        while nodes_to_ask:
            self.logger.debug2("node lookup; querying %s", nodes_to_ask)
            nodes_asked.update(nodes_to_ask)
            next_find_node_queries = (
                (_find_node, node_id, n)
                for n
                in nodes_to_ask
                if not self.neighbours_channels.already_waiting_for(n)
            )
            results = await trio_utils.gather(*next_find_node_queries)
            for candidates in results:
                closest.extend(candidates)
            # Need to sort again and pick just the closest k nodes to ensure we converge.
            closest = sort_by_distance(
                eth_utils.toolz.unique(closest), node_id)[:constants.KADEMLIA_BUCKET_SIZE]
            nodes_to_ask = _exclude_if_asked(closest)

        self.logger.debug(
            "lookup finished for target %s; closest neighbours: %s", to_hex(node_id), closest
        )
        return tuple(closest)

    async def lookup_random(self) -> Tuple[NodeAPI, ...]:
        return await self.lookup(random.randint(0, constants.KADEMLIA_MAX_NODE_ID))

    def get_random_bootnode(self) -> Iterator[NodeAPI]:
        if self.bootstrap_nodes:
            yield random.choice(self.bootstrap_nodes)
        else:
            self.logger.warning('No bootnodes available')

    def get_nodes_to_connect(self, count: int) -> Iterator[NodeAPI]:
        return self.routing.get_random_nodes(count)

    @property
    def pubkey(self) -> datatypes.PublicKey:
        return self.privkey.public_key

    def _get_handler(self, cmd: DiscoveryCommand) -> V4_HANDLER_TYPE:
        if cmd == CMD_PING:
            return self.recv_ping_v4
        elif cmd == CMD_PONG:
            return self.recv_pong_v4
        elif cmd == CMD_FIND_NODE:
            return self.recv_find_node_v4
        elif cmd == CMD_NEIGHBOURS:
            return self.recv_neighbours_v4
        elif cmd == CMD_ENR_REQUEST:
            return self.recv_enr_request
        elif cmd == CMD_ENR_RESPONSE:
            return self.recv_enr_response
        else:
            raise ValueError(f"Unknown command: {cmd}")

    @classmethod
    def _get_max_neighbours_per_packet(cls) -> int:
        if cls._max_neighbours_per_packet_cache is not None:
            return cls._max_neighbours_per_packet_cache
        cls._max_neighbours_per_packet_cache = _get_max_neighbours_per_packet()
        return cls._max_neighbours_per_packet_cache

    async def bootstrap(self) -> None:
        for node in self.bootstrap_nodes:
            uri = node.uri()
            pubkey, _, uri_tail = uri.partition('@')
            pubkey_head = pubkey[:16]
            pubkey_tail = pubkey[-8:]
            self.logger.debug("full-bootnode: %s", uri)
            self.logger.debug("bootnode: %s...%s@%s", pubkey_head, pubkey_tail, uri_tail)

        bonding_queries = (
            (self.bond, n)
            for n
            in self.bootstrap_nodes
            if (not self.ping_channels.already_waiting_for(n) and
                not self.pong_channels.already_waiting_for(n))
        )
        bonded = await trio_utils.gather(*bonding_queries)
        if not any(bonded):
            self.logger.info("Failed to bond with bootstrap nodes %s", self.bootstrap_nodes)
            return
        await self.lookup_random()

    def send(self, node: NodeAPI, msg_type: DiscoveryCommand, payload: Sequence[Any]) -> bytes:
        message = _pack_v4(msg_type.id, payload, self.privkey)
        self.manager.run_task(
            self.socket.sendto, message, (node.address.ip, node.address.udp_port))
        return message

    async def consume_datagram(self) -> None:
        datagram, (ip_address, port) = await self.socket.recvfrom(
            constants.DISCOVERY_DATAGRAM_BUFFER_SIZE)
        address = Address(ip_address, port, port)
        self.logger.debug2("Received datagram from %s", address)
        self.manager.run_task(self.receive, address, datagram)

    async def receive(self, address: AddressAPI, message: bytes) -> None:
        try:
            remote_pubkey, cmd_id, payload, message_hash = _unpack_v4(message)
        except DefectiveMessage as e:
            self.logger.error('error unpacking message (%s) from %s: %s', message, address, e)
            return

        try:
            cmd = CMD_ID_MAP[cmd_id]
        except KeyError:
            self.logger.warning("Ignoring uknown msg type: %s; payload=%s", cmd_id, payload)
            return
        self.logger.debug2("Received %s with payload: %s", cmd.name, payload)
        try:
            node = self.routing.get_node(node_id_from_pubkey(remote_pubkey))
        except KeyError:
            node = Node.from_pubkey_and_addr(remote_pubkey, address)
        else:
            if node.address != address:
                # If this node's address changed since we last heard from it we create a new
                # instance as that will end up replacing the old one when we move it to the front
                # of the bucket (which is done every time we receive a msg from a node).
                node = Node.from_pubkey_and_addr(remote_pubkey, address)
        handler = self._get_handler(cmd)
        await handler(node, payload, message_hash)

    def _is_msg_expired(self, rlp_expiration: bytes) -> bool:
        expiration = rlp.sedes.big_endian_int.deserialize(rlp_expiration)
        if time.time() > expiration:
            self.logger.debug('Received message already expired')
            return True
        return False

    async def recv_pong_v4(self, node: NodeAPI, payload: Sequence[Any], _: Hash32) -> None:
        # The pong payload should have at least 3 elements: to, token, expiration
        if len(payload) < 3:
            self.logger.warning('Ignoring PONG msg with invalid payload: %s', payload)
            return
        elif len(payload) == 3:
            _, token, expiration = payload[:3]
            enr_seq = None
        else:
            _, token, expiration, enr_seq = payload[:4]
            enr_seq = big_endian_to_int(enr_seq)
        if self._is_msg_expired(expiration):
            return
        self.logger.debug2('<<< pong (v4) from %s (token == %s)', node, encode_hex(token))
        node.last_pong = time.monotonic()
        await self.process_pong_v4(node, token, enr_seq)

    async def recv_neighbours_v4(self, remote: NodeAPI, payload: Sequence[Any], _: Hash32) -> None:
        # The neighbours payload should have 2 elements: nodes, expiration
        if len(payload) < 2:
            self.logger.warning('Ignoring NEIGHBOURS msg with invalid payload: %s', payload)
            return
        nodes, expiration = payload[:2]
        if self._is_msg_expired(expiration):
            return
        neighbours = []
        # If any of the nodes we received are already in our routing table, use them instead as
        # we may have bonded and then won't have to do so again.
        for node in _extract_nodes_from_payload(remote.address, nodes, self.logger):
            try:
                existing_node = self.routing.get_node(node_id_from_pubkey(node.pubkey))
                if existing_node.address == node.address:
                    node = existing_node
            except KeyError:
                pass
            neighbours.append(node)
        self.logger.debug2('<<< neighbours from %s: %s', remote, neighbours)
        try:
            channel = self.neighbours_channels.get_channel(remote)
        except KeyError:
            self.logger.debug(f'unexpected neighbours from {node}, probably came too late')
            return

        try:
            await channel.send(neighbours)
        except trio.BrokenResourceError:
            # This means the receiver has already closed, probably because it timed out.
            pass

    async def recv_ping_v4(
            self, node: NodeAPI, payload: Sequence[Any], message_hash: Hash32) -> None:
        # The ping payload should have at least 4 elements: [version, from, to, expiration], with
        # an optional 5th element for the node's ENR sequence number.
        if len(payload) < 4:
            self.logger.warning('Ignoring PING msg with invalid payload: %s', payload)
            return
        elif len(payload) == 4:
            _, _, _, expiration = payload[:4]
            enr_seq = None
        else:
            _, _, _, expiration, enr_seq = payload[:5]
            enr_seq = big_endian_to_int(enr_seq)
        self.logger.debug2('<<< ping(v4) from %s, enr_seq=%s', node, enr_seq)
        if self._is_msg_expired(expiration):
            return
        await self.process_ping(node, message_hash)
        await self.send_pong_v4(node, message_hash)

    async def recv_find_node_v4(self, node: NodeAPI, payload: Sequence[Any], _: Hash32) -> None:
        # The find_node payload should have 2 elements: node_id, expiration
        if len(payload) < 2:
            self.logger.warning('Ignoring FIND_NODE msg with invalid payload: %s', payload)
            return
        node_id, expiration = payload[:2]
        self.logger.debug2('<<< find_node from %s', node)
        if self._is_msg_expired(expiration):
            return
        if not node.is_bond_valid:
            self.logger.debug(
                "Ignoring find_node request from node (%s) we haven't bonded with", node)
            return
        self.update_routing_table(node)
        found = self.routing.neighbours(big_endian_to_int(node_id))
        self.send_neighbours_v4(node, found)

    async def recv_enr_request(
            self, node: NodeAPI, payload: Sequence[Any], msg_hash: Hash32) -> None:
        # The enr_request payload should have at least one element: expiration.
        if len(payload) < 1:
            self.logger.warning('Ignoring ENR_REQUEST msg with invalid payload: %s', payload)
            return
        expiration = payload[0]
        if self._is_msg_expired(expiration):
            return
        if not node.is_bond_valid:
            self.logger.debug("Ignoring ENR_REQUEST from node (%s) we haven't bonded with", node)
            return
        enr = await self.get_local_enr()
        self.logger.debug("Sending local ENR to %s: %s", node, enr)
        payload = (msg_hash, ENR.serialize(enr))
        self.send(node, CMD_ENR_RESPONSE, payload)

    async def recv_enr_response(
            self, node: NodeAPI, payload: Sequence[Any], msg_hash: Hash32) -> None:
        # The enr_response payload should have at least two elements: request_hash, enr.
        if len(payload) < 2:
            self.logger.warning('Ignoring ENR_RESPONSE msg with invalid payload: %s', payload)
            return
        token, serialized_enr = payload[:2]
        try:
            enr = ENR.deserialize(serialized_enr)
        except DeserializationError as error:
            raise ValidationError("ENR in response is not properly encoded") from error
        try:
            channel = self.enr_response_channels.get_channel(node)
        except KeyError:
            self.logger.debug("Unexpected ENR_RESPONSE from %s", node)
            return
        enr.validate_signature()
        self.logger.debug(
            "Received ENR %s (%s) with expected response token: %s",
            enr, enr.items(), encode_hex(token))
        try:
            await channel.send((enr, token))
        except trio.BrokenResourceError:
            # This means the receiver has already closed, probably because it timed out.
            pass

    def send_enr_request(self, node: NodeAPI) -> Hash32:
        message = self.send(node, CMD_ENR_REQUEST, [_get_msg_expiration()])
        token = Hash32(message[:MAC_SIZE])
        self.logger.debug("Sending ENR request with token: %s", encode_hex(token))
        return token

    async def send_ping_v4(self, node: NodeAPI) -> Hash32:
        version = rlp.sedes.big_endian_int.serialize(PROTO_VERSION)
        expiration = _get_msg_expiration()
        local_enr_seq = await self.get_local_enr_seq()
        payload = (version, self.this_node.address.to_endpoint(), node.address.to_endpoint(),
                   expiration, int_to_big_endian(local_enr_seq))
        message = self.send(node, CMD_PING, payload)
        # Return the msg hash, which is used as a token to identify pongs.
        token = Hash32(message[:MAC_SIZE])
        self.logger.debug2('>>> ping (v4) %s (token == %s)', node, encode_hex(token))
        # XXX: This hack is needed because there are lots of parity 1.10 nodes out there that send
        # the wrong token on pong msgs (https://github.com/paritytech/parity/issues/8038). We
        # should get rid of this once there are no longer too many parity 1.10 nodes out there.
        parity_token = keccak(message[HEAD_SIZE + 1:])
        self.parity_pong_tokens[parity_token] = token
        return token

    def send_find_node_v4(self, node: NodeAPI, target_node_id: int) -> None:
        expiration = _get_msg_expiration()
        node_id = int_to_big_endian(
            target_node_id).rjust(constants.KADEMLIA_PUBLIC_KEY_SIZE // 8, b'\0')
        self.logger.debug2('>>> find_node to %s', node)
        self.send(node, CMD_FIND_NODE, (node_id, expiration))

    async def send_pong_v4(self, node: NodeAPI, token: Hash32) -> None:
        expiration = _get_msg_expiration()
        self.logger.debug2('>>> pong %s', node)
        local_enr_seq = await self.get_local_enr_seq()
        payload = (node.address.to_endpoint(), token, expiration, int_to_big_endian(local_enr_seq))
        self.send(node, CMD_PONG, payload)

    def send_neighbours_v4(self, node: NodeAPI, neighbours: List[NodeAPI]) -> None:
        nodes = []
        neighbours = sorted(neighbours)
        for n in neighbours:
            nodes.append(n.address.to_endpoint() + [n.pubkey.to_bytes()])

        expiration = _get_msg_expiration()
        max_neighbours = self._get_max_neighbours_per_packet()
        for i in range(0, len(nodes), max_neighbours):
            self.logger.debug2('>>> neighbours to %s: %s',
                               node, neighbours[i:i + max_neighbours])
            payload = NeighboursPacket(
                neighbours=nodes[i:i + max_neighbours],
                expiration=expiration)
            self.send(node, CMD_NEIGHBOURS, payload)

    async def process_pong_v4(self, remote: NodeAPI, token: Hash32, enr_seq: int) -> None:
        # XXX: This hack is needed because there are lots of parity 1.10 nodes out there that send
        # the wrong token on pong msgs (https://github.com/paritytech/parity/issues/8038). We
        # should get rid of this once there are no longer too many parity 1.10 nodes out there.
        if token in self.parity_pong_tokens:
            # This is a pong from a buggy parity node, so need to lookup the actual token we're
            # expecting.
            token = self.parity_pong_tokens.pop(token)
        else:
            # This is a pong from a non-buggy node, so just cleanup self.parity_pong_tokens.
            self.parity_pong_tokens = eth_utils.toolz.valfilter(
                lambda val: val != token, self.parity_pong_tokens)

        try:
            channel = self.pong_channels.get_channel(remote)
        except KeyError:
            # This is probably a Node which changed its identity since it was added to the DHT,
            # causing us to expect a pong signed with a certain key when in fact it's using
            # a different one. Another possibility is that the pong came after we've given up
            # waiting.
            self.logger.debug(f'Unexpected pong from {remote} with token {encode_hex(token)}')
            return

        try:
            await channel.send((token, enr_seq))
        except trio.BrokenResourceError:
            # This means the receiver has already closed, probably because it timed out.
            pass

    async def process_ping(self, remote: NodeAPI, hash_: Hash32) -> None:
        """Process a received ping packet.

        A ping packet may come any time, unrequested, or may be prompted by us bond()ing with a
        new node. In the former case we'll just update the sender's entry in our routing table and
        reply with a pong, whereas in the latter we'll also send an empty msg on the appropriate
        channel from ping_channels, to notify any coroutine waiting for a ping.
        """
        if remote == self.this_node:
            self.logger.info('Invariant: received ping from this_node: %s', remote)
            return
        else:
            self.update_routing_table(remote)

        try:
            channel = self.ping_channels.get_channel(remote)
        except KeyError:
            return

        try:
            await channel.send(None)
        except trio.BrokenResourceError:
            # This means the receiver has already closed, probably because it timed out.
            pass


class PreferredNodeDiscoveryService(DiscoveryService):
    """
    A DiscoveryService which has a list of preferred nodes which it will prioritize using before
    trying to find nodes.  Each preferred node can only be used once every
    preferred_node_recycle_time seconds.
    """
    preferred_nodes: Sequence[NodeAPI] = None
    preferred_node_recycle_time: int = 300
    _preferred_node_tracker: Dict[NodeAPI, float] = None

    logger = get_extended_debug_logger('p2p.discovery.PreferredNodeDiscoveryService')

    def __init__(self,
                 privkey: datatypes.PrivateKey,
                 address: AddressAPI,
                 bootstrap_nodes: Sequence[NodeAPI],
                 preferred_nodes: Sequence[NodeAPI],
                 event_bus: EndpointAPI,
                 socket: trio.socket.SocketType,
                 enr_db: EnrDbApi,
                 enr_field_providers: Optional[Sequence[ENR_FieldProvider]] = tuple()
                 ) -> None:
        super().__init__(
            privkey, address, bootstrap_nodes, event_bus, socket, enr_db, enr_field_providers)
        self.preferred_nodes = preferred_nodes
        self.logger.info('Preferred peers: %s', self.preferred_nodes)
        self._preferred_node_tracker = collections.defaultdict(lambda: 0)

    @to_tuple
    def _get_eligible_preferred_nodes(self) -> Iterator[NodeAPI]:
        """
        Return nodes from the preferred_nodes which have not been used within
        the last preferred_node_recycle_time
        """
        for node in self.preferred_nodes:
            last_used = self._preferred_node_tracker[node]
            if time.time() - last_used > self.preferred_node_recycle_time:
                yield node

    def _get_random_preferred_node(self) -> NodeAPI:
        """
        Return a random node from the preferred list.
        """
        eligible_nodes = self._get_eligible_preferred_nodes()
        if not eligible_nodes:
            raise NoEligibleNodes("No eligible preferred nodes available")
        node = random.choice(eligible_nodes)
        return node

    def get_random_bootnode(self) -> Iterator[NodeAPI]:
        """
        Return a single node to bootstrap, preferring nodes from the preferred list.
        """
        try:
            node = self._get_random_preferred_node()
            self._preferred_node_tracker[node] = time.time()
            yield node
        except NoEligibleNodes:
            yield from super().get_random_bootnode()

    def get_nodes_to_connect(self, count: int) -> Iterator[NodeAPI]:
        """
        Return up to `count` nodes, preferring nodes from the preferred list.
        """
        preferred_nodes = self._get_eligible_preferred_nodes()[:count]
        for node in preferred_nodes:
            self._preferred_node_tracker[node] = time.time()
            yield node

        num_nodes_needed = max(0, count - len(preferred_nodes))
        yield from super().get_nodes_to_connect(num_nodes_needed)


class StaticDiscoveryService(Service):
    """A 'discovery' service that does not connect to any nodes."""
    _static_peers: Tuple[NodeAPI, ...]
    _event_bus: EndpointAPI

    logger = get_extended_debug_logger('p2p.discovery.StaticDiscoveryService')

    def __init__(
            self,
            event_bus: EndpointAPI,
            static_peers: Sequence[NodeAPI]) -> None:
        self._event_bus = event_bus
        self._static_peers = tuple(static_peers)

    async def handle_get_peer_candidates_requests(self) -> None:
        async for event in self._event_bus.stream(PeerCandidatesRequest):
            candidates = self._select_nodes(event.max_candidates)
            await self._broadcast_nodes(event, candidates)

    async def handle_get_random_bootnode_requests(self) -> None:
        async for event in self._event_bus.stream(RandomBootnodeRequest):
            candidates = self._select_nodes(1)
            await self._broadcast_nodes(event, candidates)

    def _select_nodes(self, max_nodes: int) -> Tuple[NodeAPI, ...]:
        if max_nodes >= len(self._static_peers):
            candidates = self._static_peers
            self.logger.debug2("Replying with all static nodes: %r", candidates)
        else:
            candidates = tuple(random.sample(self._static_peers, max_nodes))
            self.logger.debug2("Replying with subset of static nodes: %r", candidates)
        return candidates

    async def _broadcast_nodes(
            self,
            event: BaseRequestResponseEvent[PeerCandidatesResponse],
            nodes: Sequence[NodeAPI]) -> None:
        await self._event_bus.broadcast(
            event.expected_response_type()(tuple(nodes)),
            event.broadcast_config()
        )

    async def run(self) -> None:
        self.manager.run_daemon_task(self.handle_get_peer_candidates_requests)
        self.manager.run_daemon_task(self.handle_get_random_bootnode_requests)

        await self.manager.wait_finished()


class NoopDiscoveryService(Service):
    'A stub "discovery service" which does nothing'
    logger = get_extended_debug_logger('p2p.discovery.NoopDiscoveryService')

    def __init__(self, event_bus: EndpointAPI) -> None:
        self._event_bus = event_bus

    async def handle_get_peer_candidates_requests(self) -> None:
        async for event in self._event_bus.stream(PeerCandidatesRequest):
            self.logger.debug("Servicing request for more peer candidates")

            await self._event_bus.broadcast(
                event.expected_response_type()(tuple()),
                event.broadcast_config()
            )

    async def handle_get_random_bootnode_requests(self) -> None:
        async for event in self._event_bus.stream(RandomBootnodeRequest):
            self.logger.debug("Servicing request for boot nodes")

            await self._event_bus.broadcast(
                event.expected_response_type()(tuple()),
                event.broadcast_config()
            )

    async def run(self) -> None:
        self.manager.run_daemon_task(self.handle_get_peer_candidates_requests)
        self.manager.run_daemon_task(self.handle_get_random_bootnode_requests)

        await self.manager.wait_finished()


@to_list
def _extract_nodes_from_payload(
        sender: AddressAPI,
        payload: List[Tuple[str, bytes, bytes, bytes]],
        logger: ExtendedDebugLogger) -> Iterator[NodeAPI]:
    for item in payload:
        ip, udp_port, tcp_port, node_id = item
        address = Address.from_endpoint(ip, udp_port, tcp_port)
        if check_relayed_addr(sender, address):
            yield Node.from_pubkey_and_addr(keys.PublicKey(node_id), address)
        else:
            logger.debug("Skipping invalid address %s relayed by %s", address, sender)


class NeighboursPacket(NamedTuple):
    neighbours: List[List[bytes]]
    expiration: bytes


def _get_max_neighbours_per_packet() -> int:
    # As defined in https://github.com/ethereum/devp2p/blob/master/rlpx.md, the max size of a
    # datagram must be 1280 bytes, so when sending neighbours packets we must include up to
    # _max_neighbours_per_packet and if there's more than that split them across multiple
    # packets.
    # Use an IPv6 address here as we're interested in the size of the biggest possible node
    # representation.
    addr = Address('::1', 30303, 30303)
    node_data = addr.to_endpoint() + [b'\x00' * (constants.KADEMLIA_PUBLIC_KEY_SIZE // 8)]
    neighbours = [node_data]
    expiration = _get_msg_expiration()
    payload = rlp.encode(
        NeighboursPacket(neighbours=neighbours, expiration=expiration))
    while HEAD_SIZE + len(payload) <= 1280:
        neighbours.append(node_data)
        payload = rlp.encode(
            NeighboursPacket(neighbours=neighbours, expiration=expiration))
    return len(neighbours) - 1


def _pack_v4(cmd_id: int, payload: Sequence[Any], privkey: datatypes.PrivateKey) -> bytes:
    """Create and sign a UDP message to be sent to a remote node.

    See https://github.com/ethereum/devp2p/blob/master/rlpx.md#node-discovery for information on
    how UDP packets are structured.
    """
    cmd_id_bytes = to_bytes(cmd_id)
    encoded_data = cmd_id_bytes + rlp.encode(payload)
    signature = privkey.sign_msg(encoded_data)
    message_hash = keccak(signature.to_bytes() + encoded_data)
    return message_hash + signature.to_bytes() + encoded_data


def _unpack_v4(message: bytes) -> Tuple[datatypes.PublicKey, int, Tuple[Any, ...], Hash32]:
    """Unpack a discovery v4 UDP message received from a remote node.

    Returns the public key used to sign the message, the cmd ID, payload and hash.
    """
    message_hash = Hash32(message[:MAC_SIZE])
    if message_hash != keccak(message[MAC_SIZE:]):
        raise WrongMAC("Wrong msg mac")
    signature = keys.Signature(message[MAC_SIZE:HEAD_SIZE])
    signed_data = message[HEAD_SIZE:]
    remote_pubkey = signature.recover_public_key_from_msg(signed_data)
    cmd_id = message[HEAD_SIZE]
    payload = tuple(rlp.decode(message[HEAD_SIZE + 1:], strict=False))
    return remote_pubkey, cmd_id, payload, message_hash


def _get_msg_expiration() -> bytes:
    return rlp.sedes.big_endian_int.serialize(int(time.time() + EXPIRATION))


TMsg = TypeVar("TMsg")


class ExpectedResponseChannels(Generic[TMsg]):

    def __init__(self) -> None:
        self._channels: Dict[NodeAPI, trio.abc.SendChannel[TMsg]] = {}

    def already_waiting_for(self, remote: NodeAPI) -> bool:
        return remote in self._channels

    def get_channel(self, remote: NodeAPI) -> trio.abc.SendChannel[TMsg]:
        return self._channels[remote]

    async def receive_one(
        self,
        remote: NodeAPI,
        send_chan: trio.abc.SendChannel[TMsg],
        recv_chan: trio.abc.ReceiveChannel[TMsg]
    ) -> TMsg:
        """
        Add send_chan to our dict of channels keyed by remote node and wait for a single message
        on the given receive channel.

        Closes the channel once a message is received.

        Returns the read message.
        """
        gen = self.receive(remote, send_chan, recv_chan)
        # mypy thinks wrapping our generator turns it into something else, so ignore.
        async with aclosing(gen):  # type: ignore
            async for msg in gen:
                break
        return msg

    async def receive(
        self,
        remote: NodeAPI,
        send_chan: trio.abc.SendChannel[TMsg],
        recv_chan: trio.abc.ReceiveChannel[TMsg]
    ) -> AsyncIterator[TMsg]:
        """
        Add send_chan to our dict of channels keyed by remote node and wait for multiple messages
        on the given receive channel, yielding each of them as they are received.

        Callers must ensure the generator is closed when they are done, otherwise the GC will
        attempt to do so and will fail because that includes async operations. See
        https://github.com/python-trio/trio/issues/265 for more. In order to ensure the generator
        is closed, use async_generator.aclosing(), like this:

            async with aclosing(channels.receive(remote, sc, rc)) as gen:
                async for msg in gen:
                    ...

        Closes the channel upon exiting.
        """
        if remote in self._channels:
            raise AlreadyWaitingDiscoveryResponse(f"Already waiting on response from: {remote}")

        self._channels[remote] = send_chan
        try:
            async with recv_chan:
                while True:
                    yield await recv_chan.receive()
        finally:
            self._channels.pop(remote, None)


async def generate_eth_cap_enr_field(
        vm_config: Tuple[Tuple[BlockNumber, Type[VirtualMachineAPI]], ...],
        headerdb: 'BaseAsyncHeaderDB',
) -> Tuple[Literal[b'eth'], Tuple[ForkID]]:
    head = await headerdb.coro_get_canonical_head()
    genesis_hash = await headerdb.coro_get_canonical_block_hash(GENESIS_BLOCK_NUMBER)
    forkid = make_forkid(genesis_hash, head.block_number, vm_config)
    return (b'eth', (forkid,))


def node_id_from_pubkey(pubkey: keys.PublicKey) -> int:
    return big_endian_to_int(keccak(pubkey.to_bytes()))
