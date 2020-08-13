import platform

from async_service.trio import background_trio_service
import pytest
import trio

from eth2.beacon.constants import GENESIS_SLOT
from eth2.validator_client.beacon_node import BeaconNode as BeaconNodeClient
from eth2.validator_client.client import Client as ValidatorClient
from eth2.validator_client.key_store import KeyStore

# NOTE: seeing differences in ability to connect depending on platform.
# This could be specific to our trio HTTP server (somehow...) so try removing after deprecation...
local_host_name = "127.0.0.1"  # linux default
if platform.system() == "Darwin":
    local_host_name = "localhost"  # macOS variant


@pytest.mark.trio
async def test_beacon_node_and_validator_client_can_talk(
    autojump_clock,
    clock,
    beacon_node,
    eth2_config,
    chain_config,
    seconds_per_epoch,
    sample_bls_key_pairs,
    client_id,
    # NOTE: temporarily disable BLS while it standardizes
    no_op_bls,
):
    starting_head_slot = beacon_node._chain.get_canonical_head().slot
    assert starting_head_slot == GENESIS_SLOT

    async with trio.open_nursery() as nursery:
        await nursery.start(beacon_node.run)

        api_client = BeaconNodeClient(
            chain_config.genesis_time,
            f"http://{local_host_name}:{beacon_node.validator_api_port}",
            eth2_config.SECONDS_PER_SLOT,
        )
        async with api_client:
            # sanity check
            assert api_client.client_version == client_id

            key_store = KeyStore(sample_bls_key_pairs)
            validator = ValidatorClient(key_store, clock, api_client)

            with trio.move_on_after(seconds_per_epoch):
                async with background_trio_service(validator):
                    await trio.sleep(seconds_per_epoch * 2)
            nursery.cancel_scope.cancel()
    sent_operations_for_broadcast = api_client._broadcast_operations
    received_operations_for_broadcast = (
        beacon_node._validator_api_server.context._broadcast_operations
    )

    # temporary until we update to the new API
    # NOTE: with new API, remove assertion and deletion
    assert validator._duty_store._store[((1 << 64) - 1, 0)]
    del validator._duty_store._store[((1 << 64) - 1, 0)]

    # NOTE: this is the easiest condition to pass while suggesting this is working
    # As the other parts of the project shore up, we should do stricter testing to ensure
    # the operations we expect (and that they exist...) get across the gap from
    # validator to beacon node
    assert received_operations_for_broadcast.issubset(sent_operations_for_broadcast)
    assert beacon_node._chain.get_canonical_head().slot > starting_head_slot
