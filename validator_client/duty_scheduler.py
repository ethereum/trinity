import logging
from typing import AsyncIterable

from async_service.base import Service

from validator_client.beacon_node import MockBeaconNode as BeaconNode
from validator_client.clock import Tick
from validator_client.context import Context
from validator_client.duty_store import DutyStore

logger = logging.getLogger("validator_client.duty_scheduler")
logger.setLevel(logging.DEBUG)


def _filter_new_duties(old, new):
    return new


def _get_duties_for(tick, duties):
    return duties


async def _select_distinct_epochs(clock):
    current_epoch = None
    async for tick in clock:
        if tick.epoch != current_epoch:
            current_epoch = tick.epoch
            yield tick


class DutyScheduler(Service):
    """
    ``DutyScheduler`` manages the set of duties for the validator public keys in the key store.

    This task involves polling the beacon node periodically to get the latest set of assignments
    based on the beacon chain state.
    """

    def __init__(
        self,
        context: Context,
        clock: AsyncIterable,
        duty_store: DutyStore,
        beacon_node: BeaconNode,
    ) -> None:
        self._context = context
        self._epoch_clock = _select_distinct_epochs(clock)
        self._validator_public_keys = context.validator_public_keys
        self._beacon_node = beacon_node
        self._store = duty_store

    async def _process_tick(self, tick: Tick) -> None:
        logger.info("checking for new duties in epoch %s", tick.epoch)
        duties = await self._beacon_node.fetch_duties(
            self._validator_public_keys, tick.epoch
        )
        logger.info("found duties %s in epoch %s", duties, tick.epoch)
        # TODO manage duties correctly, accounting for re-orgs, etc.
        await self._store.add_duties_at_tick(duties, tick)

    async def run(self) -> None:
        async for tick in self._epoch_clock:
            await self._process_tick(tick)
