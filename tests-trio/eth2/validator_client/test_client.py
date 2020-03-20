from async_service.exceptions import DaemonTaskExit
from async_service.trio import background_trio_service
import pytest
import trio

from eth2.beacon.typing import CommitteeIndex
from eth2.validator_client.beacon_node import MockBeaconNode
from eth2.validator_client.client import Client
from eth2.validator_client.clock import Clock
from eth2.validator_client.duty import AttestationDuty, BlockProposalDuty, DutyType
from eth2.validator_client.key_store import InMemoryKeyStore
from eth2.validator_client.randao import mk_randao_provider
from eth2.validator_client.signatory import sign
from eth2.validator_client.tick import Tick


def _mk_duty_fetcher(public_key, slots_per_epoch, seconds_per_slot):
    """"
    This ``duty_fetcher`` works by just returning a block proposal duty for the current slot
    and an attestation duty for the same slot but one epoch ahead.

    It is expected that another component will filter slashable duties.
    """

    def duty_fetcher(
        current_tick, _public_keys, target_epoch, _slots_per_epoch, _seconds_per_slot
    ):
        duties = ()
        if target_epoch < 0:
            return duties

        some_slot = current_tick.slot
        if current_tick.epoch >= 0:
            block_proposal_duty = BlockProposalDuty(
                public_key, Tick(0, some_slot, target_epoch, 0), current_tick
            )
            duties += (block_proposal_duty,)
        attestation_duty = AttestationDuty(
            public_key,
            Tick(0, some_slot + slots_per_epoch, target_epoch, 1),
            current_tick,
            CommitteeIndex(22),
        )
        duties += (attestation_duty,)
        return duties

    return duty_fetcher


@pytest.mark.trio
async def test_client_works(
    autojump_clock, sample_bls_key_pair, seconds_per_slot, slots_per_epoch
):
    """
    This test constructs a ``Client`` with enough known inputs to compute an expected set of
    signatures after running for a given amount of time. The test fails if the expected signatures
    are not observed as outputs of the client.
    """
    slots_per_epoch = 4
    # NOTE: start 2 epochs ahead of genesis to emulate the client
    # waiting to the time it can start polling the beacon node
    # and the getting duties in the first epoch in the epoch prior to genesis
    total_epochs_to_run = 4
    epochs_before_genesis_to_start = 2
    epochs_after_genesis_to_end = total_epochs_to_run - epochs_before_genesis_to_start
    # Set genesis so that we aren't aligned with a slot, which could hide some
    # bugs we will otherwise see...
    non_aligned_time = trio.current_time() + seconds_per_slot / 3
    seconds_per_epoch = seconds_per_slot * slots_per_epoch
    genesis_time = int(
        non_aligned_time + epochs_before_genesis_to_start * seconds_per_epoch
    )

    public_key = tuple(sample_bls_key_pair.keys())[0]
    key_store = InMemoryKeyStore(sample_bls_key_pair)
    beacon_node = MockBeaconNode(
        slots_per_epoch,
        seconds_per_slot,
        duty_fetcher=_mk_duty_fetcher(public_key, slots_per_epoch, seconds_per_slot),
    )

    clock = Clock(
        seconds_per_slot,
        genesis_time,
        slots_per_epoch,
        seconds_per_epoch,
        trio.current_time,
    )
    client = Client(key_store, clock, beacon_node)

    randao_provider = mk_randao_provider(key_store.private_key_for)

    try:
        async with background_trio_service(client):
            await trio.sleep(total_epochs_to_run * seconds_per_epoch)
    except DaemonTaskExit:
        # NOTE: there is a race condition in ``async_service`` that will
        # trigger ``DaemonTaskExit`` when the test would otherwise pass.
        # See: https://github.com/ethereum/async-service/issues/54
        pass

    fulfilled_duties = tuple(
        filter(
            lambda duty: duty.tick_for_execution.epoch < epochs_after_genesis_to_end,
            beacon_node.given_duties,
        )
    )
    assert len(beacon_node.published_signatures) == len(fulfilled_duties)
    for duty in fulfilled_duties:
        if duty.duty_type == DutyType.Attestation:
            operation = await beacon_node.fetch_attestation(
                duty.validator_public_key,
                duty.tick_for_execution.slot,
                duty.committee_index,
            )
        else:
            randao_reveal = randao_provider(
                duty.validator_public_key, duty.tick_for_execution.epoch
            )
            operation = await beacon_node.fetch_block_proposal(
                duty.tick_for_execution.slot, randao_reveal
            )

        observed_signature = beacon_node.published_signatures[duty]
        expected_signature = sign(duty, operation, key_store.private_key_for)
        assert observed_signature == expected_signature
