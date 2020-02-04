import logging
import random
from types import TracebackType
from typing import Callable, Collection, Dict, Optional, Set, Tuple, Type

from asks import Session
from eth_typing import BLSPubkey, BLSSignature
from eth_utils import ValidationError
import trio

from eth2._utils.humanize import humanize_bytes
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.typing import CommitteeIndex, Epoch, Slot
from eth2.validator_client.abc import BeaconNodeAPI
from eth2.validator_client.config import Config
from eth2.validator_client.duty import (
    AttestationDuty,
    BlockProposalDuty,
    Duty,
    DutyType,
)
from eth2.validator_client.tick import Tick


async def _get_duties_from_beacon_node(
    session: Session,
    url: str,
    public_keys: Collection[BLSPubkey],
    epoch: Epoch,
    slots_per_epoch: int,
) -> Tuple[Duty, ...]:
    # TODO
    # resp = await session.get(url, params={"validator_pubkeys": public_keys, "epoch": epoch})
    # ... parse resp into duties
    return ()


async def _publish_duty_with_signature(
    session: Session, url: str, duty: Duty, signature: BLSSignature
) -> None:
    # TODO
    # resp = await session.post(url, params)
    # ... ensure resp is OK
    return


async def _get_syncing_status(session: Session, url: str) -> bool:
    # TODO
    return False


async def _get_genesis_time(session: Session, url: str) -> int:
    # TODO
    return 0


class BeaconNode(BeaconNodeAPI):
    logger = logging.getLogger("eth2.validator_client.beacon_node")

    def __init__(
        self, genesis_time: int, beacon_node_endpoint: str, slots_per_epoch: Slot
    ) -> None:
        self._genesis_time = genesis_time
        self._beacon_node_endpoint = beacon_node_endpoint
        self._slots_per_epoch = slots_per_epoch
        self._session = Session()
        self._connection_lock = trio.Lock()
        self._is_connected = False

    @classmethod
    def from_config(cls, config: Config) -> "BeaconNode":
        return cls(
            config.genesis_time, config.beacon_node_endpoint, config.slots_per_epoch
        )

    async def __aenter__(self) -> BeaconNodeAPI:
        if self._is_connected:
            return self
        async with self._connection_lock:
            await self._connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        pass

    async def _connect(self) -> None:
        """
        Verify the syncing status and genesis time of the provided
        beacon node, ensuring it is up-to-date and reachable.
        """
        await self._wait_for_synced_beacon_node()
        await self._validate_genesis_time()
        self._is_connected = True

    async def _get_syncing_status(self) -> bool:
        # TODO
        url = ""  # self._syncing_endpoint
        return await _get_syncing_status(self._session, url)

    async def _wait_for_synced_beacon_node(self) -> None:
        is_syncing = await self._get_syncing_status()
        if is_syncing:
            # TODO
            # some loop +  time-out until synced!
            pass

    async def _validate_genesis_time(self) -> None:
        """
        Ensure the connected beacon node has the same genesis time
        as was provided during instantiation of ``self``.
        """
        genesis_url = ""  # self._genesis_endpoint
        genesis_time = await _get_genesis_time(self._session, genesis_url)
        if genesis_time != self._genesis_time:
            raise ValidationError(
                f"Genesis time of validator client {self._genesis_time} did not match genesis time"
                f" of beacon node {genesis_time} at endpoint {self._beacon_node_endpoint}"
            )

    async def fetch_duties(
        self,
        current_tick: Tick,
        public_keys: Collection[BLSPubkey],
        target_epoch: Epoch,
    ) -> Collection[Duty]:
        url = ""  # self._fetch_duties_endpoint
        return await _get_duties_from_beacon_node(
            self._session, url, public_keys, target_epoch, self._slots_per_epoch
        )

    async def fetch_attestation(
        self, public_key: BLSPubkey, slot: Slot, committee_index: CommitteeIndex
    ) -> Attestation:
        # TODO make API call
        return Attestation.create(data=AttestationData.create(slot=slot))

    async def fetch_block_proposal(
        self, public_key: BLSPubkey, slot: Slot
    ) -> BeaconBlock:
        # TODO make API call
        return BeaconBlock.create(slot=slot)

    async def publish(self, duty: Duty, signature: BLSSignature) -> None:
        url = ""  # self._publish_endpoint
        await _publish_duty_with_signature(self._session, url, duty, signature)


DutyFetcher = Callable[
    [Tick, Collection[BLSPubkey], Epoch, Slot, int], Tuple[Duty, ...]
]


def _fetch_some_random_duties(
    current_tick: Tick,
    public_keys: Collection[BLSPubkey],
    target_epoch: Epoch,
    slots_per_epoch: Slot,
    seconds_per_slot: int,
) -> Tuple[Duty, ...]:
    if not public_keys:
        return ()
    if target_epoch < 0:
        return ()
    some_slot = Slot(
        random.randint(0, slots_per_epoch) + target_epoch * slots_per_epoch
    )
    execution_time = seconds_per_slot * (some_slot - current_tick.slot) + current_tick.t
    is_attestation = bool(random.getrandbits(1))
    # NOTE: use of ``tuple`` here is satisfy ``mypy``.
    some_validator = random.choice(tuple(public_keys))
    if is_attestation:
        committee_index = random.randint(0, 64)
        some_tick_count = 1
        attestation_duty = AttestationDuty(
            some_validator,
            Tick(execution_time, some_slot, target_epoch, some_tick_count),
            current_tick,
            CommitteeIndex(committee_index),
        )
        return (attestation_duty,)
    else:
        some_tick_count = 0
        block_proposal_duty = BlockProposalDuty(
            some_validator,
            Tick(execution_time, some_slot, target_epoch, some_tick_count),
            current_tick,
        )
        return (block_proposal_duty,)


class MockBeaconNode(BeaconNodeAPI):
    """
    An in-memory beacon node satisfying the ``BeaconNodeAPI``.

    Accepts custom logic to fetch duties with the ``duty_fetcher`` argument.
    Attempts to filter duties that would violate a slashing condition.
    Records signatures submitted for broadcast.
    """

    logger = logging.getLogger("eth2.validator_client.mock_beacon_node")

    def __init__(
        self,
        slots_per_epoch: Slot,
        seconds_per_slot: int,
        duty_fetcher: DutyFetcher = _fetch_some_random_duties,
    ) -> None:
        self._slots_per_epoch = slots_per_epoch
        self._seconds_per_slot = seconds_per_slot
        self._duty_fetcher = duty_fetcher
        # NOTE: emulate only one block proposal per validator per slot
        # and only one attestation per validator per epoch
        self._block_duty_tracker: Set[Slot] = set()
        self._attestation_duty_tracker: Dict[BLSPubkey, Set[Epoch]] = {}
        self.given_duties: Set[Duty] = set()
        self.published_signatures: Dict[Duty, BLSSignature] = {}

    @classmethod
    def from_config(cls, config: Config) -> "MockBeaconNode":
        return cls(config.slots_per_epoch, config.seconds_per_slot)

    async def __aenter__(self) -> BeaconNodeAPI:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        pass

    def _duty_filter(self, duty: Duty) -> bool:
        some_validator = duty.validator_public_key
        if duty.duty_type == DutyType.Attestation:
            attestation_epochs = self._attestation_duty_tracker.get(
                some_validator, set()
            )
            target_epoch = duty.tick_for_execution.epoch
            if target_epoch in attestation_epochs:
                return False
            attestation_epochs.add(target_epoch)
            self._attestation_duty_tracker[some_validator] = attestation_epochs
            return True
        else:
            some_slot = duty.tick_for_execution.slot
            if some_slot in self._block_duty_tracker:
                return False
            self._block_duty_tracker.add(some_slot)
            return True

    async def fetch_duties(
        self,
        current_tick: Tick,
        public_keys: Collection[BLSPubkey],
        target_epoch: Epoch,
    ) -> Collection[Duty]:
        some_duties = self._duty_fetcher(
            current_tick,
            public_keys,
            target_epoch,
            self._slots_per_epoch,
            self._seconds_per_slot,
        )
        valid_duties = tuple(filter(self._duty_filter, some_duties))
        self.logger.debug("%s: got duties %s", current_tick, valid_duties)
        for duty in valid_duties:
            # sanity check
            assert duty not in self.given_duties
            self.given_duties.add(duty)
        return valid_duties

    async def fetch_attestation(
        self, public_key: BLSPubkey, slot: Slot, committee_index: CommitteeIndex
    ) -> Attestation:
        return Attestation.create(
            data=AttestationData.create(slot=slot, index=committee_index)
        )

    async def fetch_block_proposal(
        self, public_key: BLSPubkey, slot: Slot
    ) -> BeaconBlock:
        return BeaconBlock.create(slot=slot)

    async def publish(self, duty: Duty, signature: BLSSignature) -> None:
        self.logger.debug(
            "publishing %s with signature %s to beacon node",
            duty,
            humanize_bytes(signature),
        )
        self.published_signatures[duty] = signature
