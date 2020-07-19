import asyncio
import contextlib

import pytest

from p2p.disconnect import DisconnectReason
from p2p.logic import BaseLogic
from p2p.p2p_proto import Disconnect
from p2p.qualifiers import always
from p2p.tools.factories import ParagonPeerPairFactory
from p2p.tools.paragon.api import ParagonAPI


@pytest.mark.asyncio
async def test_disconnect_on_cancellation():
    got_disconnect = asyncio.Event()

    async def _handle_disconnect(conn, cmd):
        got_disconnect.set()

    async with ParagonPeerPairFactory() as (alice, bob):
        bob.connection.add_command_handler(Disconnect, _handle_disconnect)
        await alice.manager.stop()
        await asyncio.wait_for(got_disconnect.wait(), timeout=1)
        assert bob.remote_disconnect_reason == DisconnectReason.CLIENT_QUITTING


@pytest.mark.asyncio
async def test_closes_connection_on_cancellation():
    async with ParagonPeerPairFactory() as (alice, _):
        await alice.manager.stop()
        await alice.connection.manager.wait_finished()
        assert alice.connection.is_closing


@pytest.mark.asyncio
async def test_cancels_on_received_disconnect():
    async with ParagonPeerPairFactory() as (alice, bob):
        # Here we send only a Disconnect msg because we want to ensure that will cause bob to
        # cancel itself even if alice accidentally leaves her connection open. If we used
        # alice.cancel() to send the Disconnect msg, alice would also close its connection,
        # causing bob to detect it, close its own and cause the peer to be cancelled.
        alice._p2p_api.disconnect(DisconnectReason.CLIENT_QUITTING)
        await asyncio.wait_for(bob.connection.manager.wait_finished(), timeout=1)
        assert bob.connection.is_closing
        assert bob.remote_disconnect_reason == DisconnectReason.CLIENT_QUITTING


class BehaviorCrash(Exception):
    pass


class CrashingLogic(BaseLogic):
    async def crash(self):
        # If we crash immediately the peer may not have enough time to start and the test will
        # fail, so wait a bit before crashing.
        await asyncio.sleep(0.01)
        raise BehaviorCrash()

    @contextlib.asynccontextmanager
    async def apply(self, connection):
        yield asyncio.create_task(self.crash())


@pytest.mark.asyncio
async def test_propagates_behavior_crashes(monkeypatch):

    def init(self):
        self.add_child_behavior(CrashingLogic().as_behavior(always))

    monkeypatch.setattr(ParagonAPI, '__init__', init)
    with pytest.raises(BehaviorCrash):
        async with ParagonPeerPairFactory() as (alice, _):
            await asyncio.wait_for(alice.manager.wait_finished(), timeout=0.5)

    assert alice.manager.is_cancelled
