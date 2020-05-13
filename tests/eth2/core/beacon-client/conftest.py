import functools

import pytest
import trio

from p2p.tools.factories.socket import get_open_port

from eth2.clock import Clock
from eth2.api.http.app import app as quart_app
from eth2.api.http.client import BeaconAPI
from eth2.api.http.ir import SyncerAPI, SyncStatus, Context
from eth2.beacon.state_machines.forks.skeleton_lake.config import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.typing import Slot
from eth2.beacon.tools.factories import BeaconChainFactory


@pytest.fixture
def beacon_chain():
    return BeaconChainFactory()


@pytest.fixture
async def genesis_time():
    return int(trio.current_time())


@pytest.fixture
def clock(beacon_chain, genesis_time):
    return Clock(
        MINIMAL_SERENITY_CONFIG.SECONDS_PER_SLOT,
        genesis_time,
        MINIMAL_SERENITY_CONFIG.SLOTS_PER_EPOCH,
        MINIMAL_SERENITY_CONFIG.SECONDS_PER_SLOT * MINIMAL_SERENITY_CONFIG.SLOTS_PER_EPOCH,
        trio.current_time,
    )


def _mk_syncer() -> SyncerAPI:
    class _sync(SyncerAPI):
        async def get_status(self) -> SyncStatus:
            return SyncStatus(False, Slot(0), Slot(0), Slot(0))

    return _sync()


@pytest.fixture
def app_context(beacon_chain, genesis_time, clock):
    return Context(
        client_identifier='trinity/testing',
        genesis_time=genesis_time,
        eth2_config=MINIMAL_SERENITY_CONFIG,
        syncer=_mk_syncer(),
        chain=beacon_chain,
        clock=clock,
        _broadcast_operations=set(),
    )


@pytest.fixture
async def app(app_context):
    quart_app.config['context'] = app_context
    await quart_app.startup()
    try:
        yield quart_app
    finally:
        await quart_app.shutdown()


@pytest.fixture
def http_port():
    return get_open_port()


@pytest.fixture
async def http_app(app_context, http_port):
    quart_app.config['context'] = app_context
    async with trio.open_nursery() as nursery:
        nursery.start_soon(functools.partial(quart_app.run_task, host="localhost", port=http_port))
        try:
            yield quart_app
        finally:
            nursery.cancel_scope.cancel()


@pytest.fixture
def http_client(http_app, http_port):
    base_url = f"http://localhost:{http_port}"
    return BeaconAPI(base_url)
