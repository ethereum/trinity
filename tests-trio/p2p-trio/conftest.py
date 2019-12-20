import trio
import pytest_trio

from async_service import TrioManager

from eth_hash.auto import keccak

from eth_keys import keys

from p2p.discovery import DiscoveryService
from p2p.kademlia import Address


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


async def _manually_driven_discovery(seed, socket, nursery):
    discovery = ManuallyDrivenDiscoveryService(
        keys.PrivateKey(keccak(seed)),
        Address(*socket.getsockname()),
        bootstrap_nodes=[],
        event_bus=None,
        socket=socket)
    nursery.start_soon(TrioManager.run_service, discovery)
    await discovery.ready_to_drive.wait()
    return discovery


@pytest_trio.trio_fixture
async def manually_driven_discovery(nursery):
    socket = trio.socket.socket(family=trio.socket.AF_INET, type=trio.socket.SOCK_DGRAM)
    discovery = await _manually_driven_discovery(b'seed', socket, nursery)
    yield discovery
    discovery.manager.cancel()
    await discovery.manager.wait_finished()


@pytest_trio.trio_fixture
async def manually_driven_discovery_pair(nursery, socket_pair):
    discovery1 = await _manually_driven_discovery(b'seed1', socket_pair[0], nursery)
    discovery2 = await _manually_driven_discovery(b'seed2', socket_pair[1], nursery)
    yield discovery1, discovery2
    discovery1.manager.cancel()
    discovery2.manager.cancel()
    await discovery1.manager.wait_finished()
    await discovery2.manager.wait_finished()


class ManuallyDrivenDiscoveryService(DiscoveryService):
    """A DiscoveryService that can be executed with TrioManager.run_service() but which doesn't
    run any background tasks (e.g. bootstrapping) by itself. Instead one must schedule any tasks
    manually.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ready_to_drive = trio.Event()

    async def run(self) -> None:
        self.ready_to_drive.set()
        await self.manager.wait_finished()
