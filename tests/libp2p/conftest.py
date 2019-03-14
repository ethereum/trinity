import pytest

from libp2p.connmgr import (
    DaemonConnectionManager,
)
from libp2p.dht import (
    DaemonDHT,
)
from libp2p.mock import (
    MockControlClient,
    MockConnectionManagerClient,
    MockPubSubClient,
    MockDHTClient,
)
from libp2p.host import (
    DaemonHost,
)
from libp2p.pubsub import (
    DaemonPubSub,
)


@pytest.fixture('module')
def num_nodes():
    return 3


@pytest.fixture
def controlcs(num_nodes):
    map_peer_id_to_control_client = {}
    return tuple(
        MockControlClient(
            map_peer_id_to_control_client=map_peer_id_to_control_client,
        )
        for _ in range(num_nodes)
    )


@pytest.fixture
def daemon_hosts(controlcs):
    return tuple(
        DaemonHost(controlc)
        for controlc in controlcs
    )


@pytest.fixture
async def pubsubcs(controlcs):
    map_pid_to_pubsubc = {}
    pscs = tuple(
        MockPubSubClient(control_client, map_pid_to_pubsubc)
        for control_client in controlcs
    )
    for psc in pscs:
        await psc.listen()
    yield pscs
    for psc in pscs:
        await psc.close_listener()


@pytest.fixture
async def daemon_pubsubs(pubsubcs):
    pubsubs = tuple(
        DaemonPubSub(pubsub_client=pubsubc)
        for pubsubc in pubsubcs
    )
    yield pubsubs
    # clean up
    for daemon_pubsub in pubsubs:
        topics = await daemon_pubsub.get_topics()
        for topic in topics:
            await daemon_pubsub.unsubscribe(topic)


@pytest.fixture
def dhtcs(controlcs):
    map_peer_id_to_dht_client = {}
    return tuple(
        MockDHTClient(
            control_client=control_client,
            map_peer_id_to_dht_client=map_peer_id_to_dht_client,
        )
        for control_client in controlcs
    )


@pytest.fixture
async def daemon_dhts(dhtcs):
    return tuple(
        DaemonDHT(dht_client=dhtc)
        for dhtc in dhtcs
    )


@pytest.fixture
def connmgrcs(controlcs):
    return tuple(
        MockConnectionManagerClient(
            control_client=control_client,
            low_water_mark=1,
            high_water_mark=3,
        )
        for control_client in controlcs
    )


@pytest.fixture
async def daemon_connmgrs(connmgrcs):
    return tuple(
        DaemonConnectionManager(connmgr_client=connmgrc)
        for connmgrc in connmgrcs
    )
