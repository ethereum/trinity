import logging

import trio
import pytest_trio

from async_generator import asynccontextmanager

from async_service import background_trio_service

from eth_hash.auto import keccak

from eth_keys import keys

from p2p.discovery import DiscoveryService
from p2p.discv5.enr_db import MemoryEnrDb
from p2p.discv5.identity_schemes import default_identity_scheme_registry
from p2p.kademlia import Address


# Silence factory-boy logs; we're not interested in them.
logging.getLogger("factory").setLevel(logging.WARN)


@pytest_trio.trio_fixture
async def socket_pair():
    sending_socket = trio.socket.socket(
        family=trio.socket.AF_INET,
        type=trio.socket.SOCK_DGRAM,
    )
    receiving_socket = trio.socket.socket(
        family=trio.socket.AF_INET,
        type=trio.socket.SOCK_DGRAM,
    )
    # specifying 0 as port number results in using random available port
    await sending_socket.bind(("127.0.0.1", 0))
    await receiving_socket.bind(("127.0.0.1", 0))
    return sending_socket, receiving_socket


@asynccontextmanager
async def _manually_driven_discovery(seed, socket, nursery):
    ip, port = socket.getsockname()
    discovery = ManuallyDrivenDiscoveryService(
        keys.PrivateKey(keccak(seed)),
        Address(ip, port, port),
        bootstrap_nodes=[],
        event_bus=None,
        socket=socket,
        enr_db=MemoryEnrDb(default_identity_scheme_registry))
    async with background_trio_service(discovery):
        # Wait until we're fully initialized (i.e. until the ENR stub created in the constructor
        # is replaced with the real one).
        while discovery.this_node.enr.sequence_number == 0:
            await trio.hazmat.checkpoint()
        yield discovery


@pytest_trio.trio_fixture
async def manually_driven_discovery(nursery):
    socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
    async with _manually_driven_discovery(b'seed', socket, nursery) as discovery:
        yield discovery


@pytest_trio.trio_fixture
async def manually_driven_discovery_pair(nursery, socket_pair):
    async with _manually_driven_discovery(b'seed1', socket_pair[0], nursery) as discovery1:
        async with _manually_driven_discovery(b'seed2', socket_pair[1], nursery) as discovery2:
            yield discovery1, discovery2


class ManuallyDrivenDiscoveryService(DiscoveryService):
    """
    A DiscoveryService that can be executed with TrioManager.run_service() but which doesn't
    run any daemons nor bootstraps itself. Instead one must schedule any tasks manually.
    """

    def run_daemons_and_bootstrap(self) -> None:
        pass

    async def consume_datagram(self) -> None:
        await super().consume_datagram()
        # Our parent's consume_datagram() starts a background task to process the msg, so we yield
        # control here to give that a chance to run. This avoid us having to do so in every test
        # that calls consume_datagram().
        await trio.hazmat.checkpoint()
