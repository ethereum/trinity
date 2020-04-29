import pytest
import trio

from trinity._utils.version import construct_trinity_client_identifier
from trinity.nodes.beacon.full import BeaconNode


@pytest.mark.trio
async def test_beacon_node_can_count_slots(
    autojump_clock,
    node_key,
    eth2_config,
    chain_config,
    database_dir,
    chain_class,
    get_trio_time,
):
    validator_api_port = 0
    client_id = construct_trinity_client_identifier()
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

    some_slots = 10
    a_future_slot = node.current_tick.slot + some_slots
    seconds = some_slots * eth2_config.SECONDS_PER_SLOT
    with trio.move_on_after(seconds):
        await node.run()
    assert node.current_tick.slot == a_future_slot
