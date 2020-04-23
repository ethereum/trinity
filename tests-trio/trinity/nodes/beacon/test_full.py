import pytest
import trio

from trinity.nodes.beacon.full import BeaconNode


def get_trio_time():
    return trio.current_time()


@pytest.mark.trio
async def test_beacon_node_can_count_slots(
    autojump_clock, node_key, eth2_config, chain_config, database_dir, chain_class
):
    # NOTE: overwrite to sync with trio's time
    chain_config._genesis_state.genesis_time = (
        get_trio_time() - 10 * eth2_config.SECONDS_PER_SLOT
    )

    node = BeaconNode(
        node_key, eth2_config, chain_config, database_dir, chain_class, get_trio_time
    )

    current_tick = node._current_tick

    some_slots = 10
    a_future_slot = current_tick.slot + some_slots
    seconds = some_slots * eth2_config.SECONDS_PER_SLOT
    with trio.move_on_after(seconds):
        await node.run()
    assert node._current_tick.slot == a_future_slot
