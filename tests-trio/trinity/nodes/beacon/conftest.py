import pytest

from eth2.clock import Clock
from trinity._utils.version import construct_trinity_client_identifier
from trinity.nodes.beacon.full import BeaconNode


@pytest.fixture
def clock(eth2_config, chain_config, seconds_per_epoch, get_trio_time):
    return Clock(
        eth2_config.SECONDS_PER_SLOT,
        chain_config.genesis_time,
        eth2_config.SLOTS_PER_EPOCH,
        seconds_per_epoch,
        time_provider=get_trio_time,
    )


@pytest.fixture
def beacon_node(eth2_config, chain_config, node_key, database_dir, chain_class, clock):
    validator_api_port = 0
    client_id = construct_trinity_client_identifier()
    return BeaconNode(
        node_key,
        eth2_config,
        chain_config,
        database_dir,
        chain_class,
        clock,
        validator_api_port,
        client_id,
    )
