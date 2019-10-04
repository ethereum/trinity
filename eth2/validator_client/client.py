import logging
from typing import AsyncIterable, Collection

from async_service.base import Service
from eth_typing import BLSPubkey
import trio
from trio.abc import ReceiveChannel, SendChannel

from eth2._utils.humanize import humanize_bytes
from eth2.validator_client.abc import BeaconNodeAPI, KeyStoreAPI, SignatoryDatabaseAPI
from eth2.validator_client.beacon_node import MockBeaconNode as BeaconNode
from eth2.validator_client.clock import Clock
from eth2.validator_client.config import Config
from eth2.validator_client.duty import Duty
from eth2.validator_client.duty_scheduler import (
    resolve_duty,
    schedule_and_dispatch_duties_at_tick,
)
from eth2.validator_client.duty_store import DutyStore
from eth2.validator_client.signatory import sign_and_broadcast_operation_if_valid
from eth2.validator_client.signatory_db import InMemorySignatoryDB
from eth2.validator_client.tick import Tick
from eth2.validator_client.typing import PrivateKeyProvider, ResolvedDuty

# NOTE: ``MAX_INDIVIDUAL_DUTIES_PER_SLOT`` is the maximum number of duties
# per slot we expect a single validator to have to be able to perform.
MAX_INDIVIDUAL_DUTIES_PER_SLOT = 2


def _estimate_max_duties_per_slot(number_of_validators: int) -> int:
    """
    Estimate the maximum number of duties a validator may have to perform.

    This value is used to select an appropriate size of buffer for the communication
    of duties between the scheduler and the signatory.
    """
    # TODO: cap this number so it is not much higher than the expected average
    return MAX_INDIVIDUAL_DUTIES_PER_SLOT * number_of_validators


class Client(Service):
    logger = logging.getLogger("eth2.validator_client.client")

    def __init__(
        self,
        key_store: KeyStoreAPI,
        clock: AsyncIterable[Tick],
        beacon_node: BeaconNodeAPI,
    ) -> None:
        self._key_store = key_store
        self._clock = clock
        self._beacon_node = beacon_node

        self._duty_store = DutyStore()
        self._signature_db = InMemorySignatoryDB()

    @classmethod
    def from_config(cls, config: Config) -> "Client":
        clock = Clock.from_config(config)
        beacon_node = BeaconNode.from_config(config)
        return cls(config.key_store, clock, beacon_node)

    async def _run_client(self) -> None:
        # NOTE: all duties dispatched from the scheduler are expected to be
        # completed in one slot. thus, the maximum expected size of the
        # trio channels connecting the components downstream of the scheduler
        # can be bounded by the maximum number of expected duties per slot,
        # subject to memory constraints on the underlying hardware.
        max_duties_per_slot = _estimate_max_duties_per_slot(
            len(self._key_store.public_keys)
        )

        duty_dispatcher, duties_to_resolve = trio.open_memory_channel[Duty](
            max_duties_per_slot
        )

        resolved_duties, resolved_duty_provider = trio.open_memory_channel[
            ResolvedDuty
        ](max_duties_per_slot)

        self.manager.run_daemon_task(
            self.duty_scheduler,
            self._clock,
            self._duty_store,
            self._beacon_node,
            self._key_store.public_keys,
            duty_dispatcher,
        )
        self.manager.run_daemon_task(
            self.duty_resolver, self._beacon_node, duties_to_resolve, resolved_duties
        )
        self.manager.run_daemon_task(
            self.signatory,
            resolved_duty_provider,
            self._signature_db,
            self._beacon_node,
            self._key_store.private_key_for,
        )

        await self.manager.wait_finished()

    async def _verify_client_state_at_boot(self) -> None:
        """
        Verify any state the client has against the state observable
        from the beacon node as we are booting up.

        There can be simple discrepancies (e.g. differing genesis times) that
        suggest the client is connected to a bad beacon node.
        """
        # TODO: sanity check the validator key pairs we find are valid
        # perhaps by fetching their indices in the state
        pass

    async def run(self) -> None:
        self.logger.debug("booting client from the provided config...")

        with self._key_store:
            self.logger.info(
                "found %d validator key pair(s) for public key(s) %s",
                len(self._key_store.public_keys),
                tuple(map(humanize_bytes, self._key_store.public_keys)),
            )
            async with self._beacon_node:
                await self._verify_client_state_at_boot()
                await self._run_client()

    async def duty_scheduler(
        self,
        clock: AsyncIterable[Tick],
        duty_store: DutyStore,
        beacon_node: BeaconNodeAPI,
        validator_public_keys: Collection[BLSPubkey],
        duty_dispatcher: SendChannel[Duty],
    ) -> None:
        """
        ``duty_scheduler`` manages the set of duties for the validator public keys in the key store.

        This task involves polling the beacon node periodically to get the latest set of assignments
        based on the beacon chain state. Duties are dispatched to consumers of the ``duty_channel``
        at the appropriate point in time.
        """
        async with duty_dispatcher:
            async for tick in clock:
                self.manager.run_task(
                    schedule_and_dispatch_duties_at_tick,
                    tick,
                    beacon_node,
                    validator_public_keys,
                    duty_store,
                    duty_dispatcher,
                )

    async def duty_resolver(
        self,
        beacon_node: BeaconNodeAPI,
        duties_to_resolve: ReceiveChannel[Duty],
        resolved_duties: SendChannel[ResolvedDuty],
    ) -> None:
        async with duties_to_resolve:
            async for duty in duties_to_resolve:
                self.manager.run_task(resolve_duty, beacon_node, duty, resolved_duties)

    async def signatory(
        self,
        duty_provider: ReceiveChannel[ResolvedDuty],
        signature_store: SignatoryDatabaseAPI,
        beacon_node: BeaconNodeAPI,
        private_key_provider: PrivateKeyProvider,
    ) -> None:
        async with duty_provider:
            async for duty, operation in duty_provider:
                self.manager.run_task(
                    sign_and_broadcast_operation_if_valid,
                    duty,
                    operation,
                    signature_store,
                    beacon_node,
                    private_key_provider,
                )
