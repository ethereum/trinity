import asyncio
import functools

import pytest

from p2p.disconnect import DisconnectReason
from p2p.manager import enforce_dial_in_to_out_ratio
from p2p.p2p_proto import Disconnect
from p2p.pool import ConnectionPool
from p2p.service import run_service

from p2p.tools.paragon import ParagonHandshaker
from p2p.tools.factories import (
    get_open_port,
    PoolManagerFactory,
    PrivateKeyFactory,
    make_connection,
)


@pytest.fixture
def alice_pool():
    return ConnectionPool()


@pytest.fixture
def alice_private_key():
    return PrivateKeyFactory()


@pytest.fixture
async def alice_manager(alice_pool, alice_private_key):
    async def paragon_handshaker_provider():
        return ParagonHandshaker()

    conn_manager = PoolManagerFactory(
        pool=alice_pool,
        private_key=alice_private_key,
        p2p_handshake_params__client_version_string='alice',
        handshaker_providers=(paragon_handshaker_provider,),
    )
    async with run_service(conn_manager):
        yield conn_manager


async def paragon_handshaker_provider():
    return ParagonHandshaker()


@pytest.fixture
async def bob_manager():
    conn_manager = PoolManagerFactory(
        p2p_handshake_params__client_version_string='bob',
        handshaker_providers=(paragon_handshaker_provider,),
    )
    async with run_service(conn_manager):
        yield conn_manager


@pytest.mark.asyncio
async def test_connection_manager_pool_changed_direct_await(alice_manager):
    ready = asyncio.Event()
    pool_changed = asyncio.Event()

    async def _do_wait():
        ready.set()
        await alice_manager.wait_pool_changed()
        pool_changed.set()

    asyncio.ensure_future(_do_wait())
    await ready.wait()

    async with alice_manager.listen('0.0.0.0', get_open_port()) as alice_remote:
        assert not pool_changed.is_set()
        await asyncio.wait_for(make_connection(alice_remote), timeout=1)
        # ensure the pool changed event was set
        await asyncio.wait_for(pool_changed.wait(), timeout=1)


@pytest.mark.asyncio
async def test_connection_manager_pool_changed_as_context_manager(alice_manager):
    ready = asyncio.Event()
    pool_changed = asyncio.Event()

    async def _do_wait():
        async with alice_manager.wait_pool_changed():
            ready.set()
        pool_changed.set()

    asyncio.ensure_future(_do_wait())
    await ready.wait()

    async with alice_manager.listen('0.0.0.0', get_open_port()) as alice_remote:
        assert not pool_changed.is_set()
        await asyncio.wait_for(make_connection(alice_remote), timeout=1)
        # ensure the pool changed event was set
        await asyncio.wait_for(pool_changed.wait(), timeout=1)


@pytest.mark.asyncio
async def test_connection_manager_listen(alice_manager,
                                         alice_pool):
    async with alice_manager.listen('0.0.0.0', get_open_port()) as alice_remote:
        assert len(alice_pool) == 0

        async with alice_manager.wait_pool_changed():
            alice = await asyncio.wait_for(make_connection(alice_remote), timeout=1)

        assert alice.client_version_string == 'alice'
        assert alice.remote == alice_remote

        assert len(alice_pool) == 1


@pytest.mark.asyncio
async def test_connection_manager_seek_connections(alice_manager,
                                                   alice_pool,
                                                   bob_manager):
    async with alice_manager.listen('0.0.0.0', get_open_port()) as alice_remote:
        async def alice_provider(pool):
            return (alice_remote,)

        async with alice_manager.wait_pool_changed(), bob_manager.wait_pool_changed():
            bob_manager.run_task(bob_manager.seek_connections((alice_provider,)))

        bob_pool = bob_manager.pool

        assert len(bob_pool) == 1
        assert len(alice_pool) == 1

        alice = tuple(bob_pool)[0]
        bob = tuple(alice_pool)[0]

        assert alice.client_version_string == 'alice'
        assert alice.remote == alice_remote
        assert bob.client_version_string == 'bob'
        assert bob.remote.pubkey == bob_manager.public_key


@pytest.mark.asyncio
async def test_connection_manager_enforces_max_connections():
    manager = PoolManagerFactory(capacity_limiter__num_tokens=2)
    async with run_service(manager):
        async with manager.listen('0.0.0.0', get_open_port()) as listen_remote:
            # fill pool up with connections
            for _ in range(2):
                async with manager.wait_pool_changed():
                    await make_connection(listen_remote)

            assert len(manager.pool) == 2

            connection = await make_connection(listen_remote)
            async with run_service(connection):
                disconnect_reason = asyncio.Future()

                async def _record_disconnect_reason(connection, msg) -> None:
                    disconnect_reason.set_result(DisconnectReason(msg['reason']))

                connection.add_command_handler(Disconnect, _record_disconnect_reason)
                connection.start_protocol_streams()

                assert disconnect_reason.done() is False
                reason = await asyncio.wait_for(disconnect_reason, timeout=1)
                assert reason is DisconnectReason.too_many_peers


@pytest.mark.asyncio
async def test_connection_manager_enforces_dial_in_to_out_ratio(alice_manager, bob_manager):
    # register the `on_connect` handler to enforce `max_connections`
    alice_manager.on_connect(functools.partial(enforce_dial_in_to_out_ratio, ratio=0.75))

    async with alice_manager.listen('0.0.0.0', get_open_port()) as alice_remote:
        async with bob_manager.listen('0.0.0.0', get_open_port()) as bob_remote:
            assert len(alice_manager.pool) == 0

            async with alice_manager.wait_pool_changed():
                await make_connection(alice_remote)

            assert len(alice_manager.pool) == 1

            # now we dial again which should be rejected since the connection
            # pool is 100% dial-in connections.
            got_disconnect = asyncio.Event()
            carol = await make_connection(alice_remote)

            async def _exit_on_disconnect(connection, msg) -> None:
                got_disconnect.set()
                connection.cancel_nowait()

            async with run_service(carol):
                carol.add_command_handler(Disconnect, _exit_on_disconnect)
                carol.start_protocol_streams()
                await asyncio.wait_for(got_disconnect.wait(), timeout=1)

            # now we have alice dial-out to bob which brings the connection
            # ratio to 50/50
            async with alice_manager.wait_pool_changed():
                await alice_manager.dial(bob_remote)

            assert len(alice_manager.pool) == 2

            # ratio of 1:1 (0.5) should be allowed in.
            async with alice_manager.wait_pool_changed():
                await make_connection(alice_remote)
            assert len(alice_manager.pool) == 3

            # ratio of 2:1 (0.666) should be allowed in.
            async with alice_manager.wait_pool_changed():
                await make_connection(alice_remote)
            assert len(alice_manager.pool) == 4

            # should no longer be allowed in as would exceed dial in/out ratio
            wait_changed = asyncio.ensure_future(alice_manager.wait_pool_changed())
            await make_connection(alice_remote)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(wait_changed, timeout=0.1)
