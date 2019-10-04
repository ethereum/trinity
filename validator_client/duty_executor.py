import logging
from typing import Any, AsyncIterable, Collection, Tuple

import trio

from validator_client.beacon_node import BeaconNode
from validator_client.clock import Tick
from validator_client.context import Context
from validator_client.duty import Duty, DutyType
from validator_client.duty_store import DutyStore

logger = logging.getLogger("validator_client.duty_executor")
logger.setLevel(logging.DEBUG)

Signable = Any


async def _resolve_duty(
    beacon_node: BeaconNode, duty: Duty, send_channel: trio.MemorySendChannel
) -> None:
    if duty.duty_type == DutyType.Attestation:
        attestation = await beacon_node.fetch_attestation(
            duty.validator_public_key, duty.slot, duty.committee_index
        )
        await send_channel.send((duty, attestation))
    elif duty.duty_type == DutyType.BlockProposal:
        block_proposal = await beacon_node.fetch_block_proposal(
            duty.validator_public_key, duty.slot
        )
        await send_channel.send((duty, block_proposal))
    else:
        raise NotImplementedError("request to resolve a non-supported type of duty")


async def _resolve_duties(
    beacon_node: BeaconNode, duties: Collection[Duty]
) -> Collection[Tuple[Duty, Signable]]:
    async with trio.open_nursery() as nursery:
        send_channel, recv_channel = trio.open_memory_channel(0)
        for duty in duties:
            nursery.start_soon(_resolve_duty, beacon_node, duty, send_channel)

        results = ()
        async for value in recv_channel:
            results += (value,)

            if len(results) == len(duties):
                break

        await send_channel.aclose()
        await recv_channel.aclose()
    return results


async def duty_executor(
    context: Context,
    clock: AsyncIterable[Tick],
    duty_store: DutyStore,
    beacon_node: BeaconNode,
) -> None:
    async for tick in clock:
        duties = await duty_store.duties_at_tick(tick)
        if not duties:
            continue
        logger.info("got duties %s to execute at tick %s", duties, tick)
        yield await _resolve_duties(beacon_node, duties)
