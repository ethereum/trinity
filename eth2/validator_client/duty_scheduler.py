import logging
from typing import Collection, Tuple, cast

from eth_typing import BLSPubkey
from trio.abc import SendChannel

from eth2.beacon.typing import Epoch
from eth2.clock import Tick
from eth2.validator_client.abc import BeaconNodeAPI
from eth2.validator_client.duty import AttestationDuty, Duty, DutyType
from eth2.validator_client.duty_store import DutyStore
from eth2.validator_client.typing import RandaoProvider, ResolvedDuty
from trinity.exceptions import BeaconNodeRequestFailure

logger = logging.getLogger("eth2.validator_client.duty_scheduler")


async def resolve_duty(
    beacon_node: BeaconNodeAPI,
    randao_provider: RandaoProvider,
    duty: Duty,
    resolved_duties: SendChannel[ResolvedDuty],
) -> None:
    if duty.duty_type == DutyType.Attestation:
        duty = cast(AttestationDuty, duty)

        try:
            attestation = await beacon_node.fetch_attestation(
                duty.validator_public_key,
                duty.tick_for_execution.slot,
                duty.committee_index,
            )
        except BeaconNodeRequestFailure as err:
            logger.warning("could not fetch attestation from beacon node: %s", err)
        else:
            if attestation:
                await resolved_duties.send((duty, attestation))

    elif duty.duty_type == DutyType.BlockProposal:
        randao_reveal = randao_provider(
            duty.validator_public_key, duty.tick_for_execution.epoch
        )
        block_proposal = await beacon_node.fetch_block_proposal(
            duty.tick_for_execution.slot, randao_reveal
        )
        if block_proposal:
            await resolved_duties.send((duty, block_proposal))
    else:
        raise NotImplementedError(
            "request to resolve a non-supported type of duty: %s", duty
        )


async def _dispatch_duties_for(
    tick: Tick,
    duty_store: DutyStore,
    beacon_node: BeaconNodeAPI,
    duty_dispatcher: SendChannel[Duty],
) -> None:
    duties = await duty_store.duties_at_tick(tick)
    if not duties:
        return
    logger.debug("%s: got duties %s to execute", tick, duties)
    for duty in duties:
        await duty_dispatcher.send(duty)


async def _fetch_latest_duties(
    tick: Tick,
    beacon_node: BeaconNodeAPI,
    validator_public_keys: Collection[BLSPubkey],
    duty_store: DutyStore,
) -> None:
    current_epoch = tick.epoch
    next_epoch = Epoch(current_epoch + 1)

    try:
        current_duties = await beacon_node.fetch_duties(
            tick, validator_public_keys, current_epoch
        )
        upcoming_duties = await beacon_node.fetch_duties(
            tick, validator_public_keys, next_epoch
        )
    except BeaconNodeRequestFailure as err:
        logger.warning(
            "could not fetch latest duties from beacon node at %s: %s", tick, err
        )
    else:
        latest_duties = cast(Tuple[Duty, ...], current_duties) + cast(
            Tuple[Duty, ...], upcoming_duties
        )
        if not latest_duties:
            return

        logger.debug("%s: found %d duties", tick, len(latest_duties))

        # TODO manage duties correctly, accounting for re-orgs, etc.
        # NOTE: the naive strategy is likely "last write wins"
        await duty_store.add_duties(*latest_duties)


async def schedule_and_dispatch_duties_at_tick(
    tick: Tick,
    beacon_node: BeaconNodeAPI,
    validator_public_keys: Collection[BLSPubkey],
    duty_store: DutyStore,
    duty_dispatcher: SendChannel[Duty],
) -> None:
    await _fetch_latest_duties(tick, beacon_node, validator_public_keys, duty_store)
    await _dispatch_duties_for(tick, duty_store, beacon_node, duty_dispatcher)
