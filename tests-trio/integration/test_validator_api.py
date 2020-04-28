import pytest
import trio

from eth2.validator_client.beacon_node import BeaconNode as BeaconNodeClient
from trinity._utils.version import construct_trinity_client_identifier
from trinity.nodes.beacon.full import BeaconNode


@pytest.mark.trio
async def test_beacon_node_and_validator_client_can_talk(
    autojump_clock,
    node_key,
    eth2_config,
    chain_config,
    database_dir,
    chain_class,
    get_trio_time,
):
    client_id = construct_trinity_client_identifier()
    validator_api_port = 0
    node = BeaconNode(
        node_key,
        eth2_config,
        chain_config,
        database_dir,
        chain_class,
        validator_api_port,
        client_id,
        get_trio_time,
    )

    a_short_timeout = 5
    with trio.move_on_after(a_short_timeout):
        async with trio.open_nursery() as nursery:
            await nursery.start(node.run)
            api_client = BeaconNodeClient(
                chain_config.genesis_time,
                f"http://localhost:{node.validator_api_port}",
                eth2_config.SECONDS_PER_SLOT,
            )
            await api_client.load_client_version()
            assert api_client.client_version == client_id
