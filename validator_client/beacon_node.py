from abc import ABC, abstractmethod
import logging
import random
from typing import Collection

from asks import Session
from eth_typing import BLSPubkey, BLSSignature
from eth_utils import ValidationError, humanize_hash

from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.typing import CommitteeIndex, Epoch, Slot
from validator_client.context import Context
from validator_client.duty import AttestationDuty, BlockProposalDuty, Duty

logger = logging.getLogger("validator_client.beacon_node")
logger.setLevel(logging.DEBUG)


async def _get_duties_from_beacon_node(
    session: Session,
    url: str,
    public_keys: Collection[BLSPubkey],
    epoch: Epoch,
    slots_per_epoch: int,
) -> Collection[Duty]:
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


class BaseBeaconNode(ABC):
    @abstractmethod
    async def fetch_duties(
        self, public_keys: Collection[BLSPubkey], epoch: Epoch
    ) -> Collection[Duty]:
        ...

    @abstractmethod
    async def fetch_attestation(
        self, public_key: BLSPubkey, slot: Slot, committee_index: CommitteeIndex
    ) -> Attestation:
        ...

    @abstractmethod
    async def fetch_block_proposal(
        self, public_key: BLSPubkey, slot: Slot
    ) -> BeaconBlock:
        ...

    @abstractmethod
    async def publish(self, duty: Duty, signature: BLSSignature) -> None:
        ...


class BeaconNode(BaseBeaconNode):
    async def __init__(self, context: Context) -> None:
        self._genesis_time = context.genesis_time
        self._beacon_node_endpoint = context.beacon_node_endpoint
        self._session = Session()

        await self._connect_to_beacon_node()

    async def _connect_to_beacon_node(self) -> None:
        """
        Verify the syncing status and genesis time of the provided
        beacon node, ensuring it is up-to-date and reachable.
        """
        await self._wait_for_synced_beacon_node()
        await self._verify_matching_genesis_time()

    async def _get_syncing_status(self) -> bool:
        # TODO
        url = ...
        return await _get_syncing_status(self._session, url)

    async def _wait_for_synced_beacon_node(self) -> None:
        is_syncing = await self._get_syncing_status()
        if is_syncing:
            # TODO
            # some loop +  time-out until synced!
            pass

    async def _verify_matching_genesis_time(self) -> None:
        genesis_time = await self._get_genesis_time()
        if genesis_time != self._genesis_time:
            raise ValidationError(
                f"Genesis time of validator client {self._genesis_time} did not match genesis time "
                "of beacon node {genesis_time} at endpoint {self._beacon_node_endpoint}"
            )

    async def fetch_duties(
        self, public_keys: Collection[BLSPubkey], epoch: Epoch
    ) -> Collection[Duty]:
        url = self.fetch_duties_endpoint
        return await _get_duties_from_beacon_node(
            self._session, url, public_keys, epoch
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
        await _publish_duty_with_signature(self._session, duty, signature)


class MockBeaconNode(BeaconNode):
    """
    NOTE: For demo purposes, will likely be removed soon...
    """

    def __init__(self, context: Context) -> None:
        self._slots_per_epoch = context.slots_per_epoch

    async def fetch_duties(
        self, public_keys: Collection[BLSPubkey], epoch: Epoch
    ) -> Collection[Duty]:
        if not public_keys:
            return ()

        some_slot = (
            random.randint(0, self._slots_per_epoch) + epoch * self._slots_per_epoch
        )
        is_attestation = bool(random.getrandbits(1))
        some_validator = random.choice(public_keys)
        if is_attestation:
            committee_index = random.randint(0, 64)
            duties = (AttestationDuty(some_validator, some_slot, committee_index),)
        else:
            duties = (BlockProposalDuty(some_validator, some_slot),)
        logger.debug("got duties %s in epoch %s", duties, epoch)
        return duties

    async def fetch_attestation(
        self, public_key: BLSPubkey, slot: Slot, committee_index: CommitteeIndex
    ) -> Attestation:
        return Attestation.create(data=AttestationData.create(slot=slot))

    async def fetch_block_proposal(
        self, public_key: BLSPubkey, slot: Slot
    ) -> BeaconBlock:
        return BeaconBlock.create(slot=slot)

    async def publish(self, duty: Duty, signature: BLSSignature) -> None:
        logger.debug(
            "publishing %s with signature %s to beacon node",
            duty,
            humanize_hash(signature),
        )
