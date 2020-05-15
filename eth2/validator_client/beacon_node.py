import logging
import random
from types import TracebackType
from typing import Any, Callable, Collection, Dict, Optional, Set, Tuple, Type

from asks import Session
from eth_typing import BLSPubkey, BLSSignature
from eth_utils import ValidationError, decode_hex, encode_hex
from eth_utils.toolz import mapcat
import ssz
from ssz.tools.dump import to_formatted_dict
from ssz.tools.parse import from_formatted_dict
import trio

from eth2._utils.humanize import humanize_bytes
from eth2.api.http.validator import Paths as BeaconNodePath
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock, BeaconBlockBody, SignedBeaconBlock
from eth2.beacon.typing import (
    CommitteeIndex,
    Epoch,
    Operation,
    Root,
    SignedOperation,
    Slot,
)
from eth2.clock import TICKS_PER_SLOT, Tick
from eth2.validator_client.abc import BeaconNodeAPI
from eth2.validator_client.config import Config
from eth2.validator_client.duty import (
    AttestationDuty,
    BlockProposalDuty,
    Duty,
    DutyType,
)

SYNCING_POLL_INTERVAL = 10  # seconds
CONNECTION_RETRY_INTERVAL = 5  # seconds


async def _get_node_version(session: Session, url: str) -> str:
    return (await session.get(url)).json()


async def _get_syncing_status(session: Session, url: str) -> bool:
    status_response = await session.get(url)
    status = status_response.json()
    return status["is_syncing"]


async def _get_genesis_time(session: Session, url: str) -> int:
    return (await session.get(url)).json()


async def _get_duties_from_beacon_node(
    session: Session, url: str, public_keys: Collection[BLSPubkey], epoch: Epoch
) -> Tuple[Dict[str, Any]]:
    return (
        await session.get(
            url,
            params={
                "validator_pubkeys": ",".join(
                    encode_hex(public_key) for public_key in public_keys
                ),
                "epoch": epoch,
            },
        )
    ).json()


def _parse_bls_pubkey(encoded_public_key: str) -> BLSPubkey:
    return BLSPubkey(decode_hex(encoded_public_key))


def _parse_attestation_duty(
    duty_data: Dict[str, Any],
    validator_public_key: BLSPubkey,
    current_tick: Tick,
    target_epoch: Epoch,
    genesis_time: int,
    seconds_per_slot: int,
    ticks_per_slot: int,
) -> AttestationDuty:
    target_tick = Tick.computing_t_from(
        Slot(duty_data["attestation_slot"]),
        target_epoch,
        AttestationDuty.tick_count,
        genesis_time,
        seconds_per_slot,
        ticks_per_slot,
    )
    return AttestationDuty(
        validator_public_key=validator_public_key,
        tick_for_execution=target_tick,
        discovered_at_tick=current_tick,
        committee_index=CommitteeIndex(duty_data["committee_index"]),
    )


def _parse_block_proposal_duty(
    duty_data: Dict[str, Any],
    validator_public_key: BLSPubkey,
    current_tick: Tick,
    target_epoch: Epoch,
    genesis_time: int,
    seconds_per_slot: int,
    ticks_per_slot: int,
) -> BlockProposalDuty:
    target_tick = Tick.computing_t_from(
        Slot(duty_data["block_proposal_slot"]),
        target_epoch,
        BlockProposalDuty.tick_count,
        genesis_time,
        seconds_per_slot,
        ticks_per_slot,
    )
    return BlockProposalDuty(
        validator_public_key=validator_public_key,
        tick_for_execution=target_tick,
        discovered_at_tick=current_tick,
    )


def _parse_duties(
    duty_data: Dict[str, Any],
    current_tick: Tick,
    target_epoch: Epoch,
    genesis_time: int,
    seconds_per_slot: int,
    ticks_per_slot: int,
) -> Tuple[Duty, ...]:
    validator_public_key = _parse_bls_pubkey(duty_data["validator_pubkey"])
    attestation_duty = _parse_attestation_duty(
        duty_data,
        validator_public_key,
        current_tick,
        target_epoch,
        genesis_time,
        seconds_per_slot,
        ticks_per_slot,
    )
    block_proposal_duty = _parse_block_proposal_duty(
        duty_data,
        validator_public_key,
        current_tick,
        target_epoch,
        genesis_time,
        seconds_per_slot,
        ticks_per_slot,
    )
    duties: Tuple[Duty, ...] = tuple()
    if attestation_duty:
        duties += (attestation_duty,)
    if block_proposal_duty:
        duties += (block_proposal_duty,)
    return duties


async def _get_attestation_from_beacon_node(
    session: Session,
    url: str,
    public_key: BLSPubkey,
    slot: Slot,
    committee_index: CommitteeIndex,
) -> Attestation:
    attestation_response = (
        await session.get(
            url,
            params={
                "validator_pubkey": encode_hex(public_key),
                "slot": slot,
                "committee_index": committee_index,
            },
        )
    ).json()
    return from_formatted_dict(attestation_response, Attestation)


async def _get_block_proposal_from_beacon_node(
    session: Session, url: str, slot: Slot, randao_reveal: BLSSignature
) -> BeaconBlock:
    response = await session.get(
        url, params={"slot": slot, "randao_reveal": encode_hex(randao_reveal)}
    )
    block_proposal_response = response.json()
    return from_formatted_dict(block_proposal_response, BeaconBlock)


async def _post_signed_operation_to_beacon_node(
    session: Session, url: str, signed_operation: Operation, sedes: ssz.BaseSedes
) -> None:
    await session.post(url, json=to_formatted_dict(signed_operation, sedes))


def _normalize_url(url: str) -> str:
    return url[:-1] if url.endswith("/") else url


class BeaconNode(BeaconNodeAPI):
    logger = logging.getLogger("eth2.validator_client.beacon_node")

    def __init__(
        self, genesis_time: int, beacon_node_endpoint: str, seconds_per_slot: int
    ) -> None:
        self._genesis_time = genesis_time
        self._beacon_node_endpoint = _normalize_url(beacon_node_endpoint)
        self._seconds_per_slot = seconds_per_slot
        self._ticks_per_slot = TICKS_PER_SLOT
        self._session = Session()
        self._connection_lock = trio.Lock()
        self._is_connected = False
        self.client_version: Optional[str] = None
        # NOTE: this facilitates testing, may remove in the future...
        self._broadcast_operations: Set[Root] = set()

    @classmethod
    def from_config(cls, config: Config) -> "BeaconNode":
        return cls(
            config.genesis_time, config.beacon_node_endpoint, config.slots_per_epoch
        )

    def _url_for(self, path: BeaconNodePath) -> str:
        return self._beacon_node_endpoint + path.value

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
        self._is_connected = not self._is_connected

    async def _connect(self, retry: bool = True) -> None:
        """
        Verify the syncing status and genesis time of the provided
        beacon node, ensuring it is up-to-date and reachable.
        """
        try:
            await self.load_client_version()
            await self._wait_for_synced_beacon_node()
            await self._validate_genesis_time()
            self._is_connected = True
        except OSError as e:
            if retry:
                self.logger.warning(
                    "could not connect to beacon node at %s; retrying connection in %d seconds",
                    self._beacon_node_endpoint,
                    CONNECTION_RETRY_INTERVAL,
                )
                await trio.sleep(CONNECTION_RETRY_INTERVAL)
                await self._connect(retry=False)
            else:
                self.logger.error(e)
                raise

    async def load_client_version(self) -> None:
        """
        Reads the client version of the connected beacon node.
        Can be used for "ping"/"poke" methodology.
        """
        url = self._url_for(BeaconNodePath.node_version)
        self.client_version = await _get_node_version(self._session, url)
        self.logger.info(
            "Connected to a node with version identifier: %s", self.client_version
        )

    async def _get_syncing_status(self) -> bool:
        url = self._url_for(BeaconNodePath.sync_status)
        return await _get_syncing_status(self._session, url)

    async def _wait_for_synced_beacon_node(self) -> None:
        is_syncing = await self._get_syncing_status()
        while is_syncing:
            await trio.sleep(SYNCING_POLL_INTERVAL)
            is_syncing = await self._get_syncing_status()

    async def _validate_genesis_time(self) -> None:
        """
        Ensure the connected beacon node has the same genesis time
        as was provided during instantiation of ``self``.
        """
        genesis_time_url = self._url_for(BeaconNodePath.genesis_time)
        genesis_time = await _get_genesis_time(self._session, genesis_time_url)
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
        url = self._url_for(BeaconNodePath.validator_duties)
        duties_data = await _get_duties_from_beacon_node(
            self._session, url, public_keys, target_epoch
        )
        return tuple(
            mapcat(
                lambda data: _parse_duties(
                    data,
                    current_tick,
                    target_epoch,
                    self._genesis_time,
                    self._seconds_per_slot,
                    self._ticks_per_slot,
                ),
                duties_data,
            )
        )

    async def fetch_attestation(
        self, public_key: BLSPubkey, slot: Slot, committee_index: CommitteeIndex
    ) -> Attestation:
        url = self._url_for(BeaconNodePath.attestation)
        return await _get_attestation_from_beacon_node(
            self._session, url, public_key, slot, committee_index
        )

    async def fetch_block_proposal(
        self, slot: Slot, randao_reveal: BLSSignature
    ) -> BeaconBlock:
        url = self._url_for(BeaconNodePath.block_proposal)
        return await _get_block_proposal_from_beacon_node(
            self._session, url, slot, randao_reveal
        )

    async def publish(self, duty: Duty, signed_operation: SignedOperation) -> None:
        if duty.duty_type == DutyType.Attestation:
            url = self._url_for(BeaconNodePath.attestation)
            sedes = Attestation
        elif duty.duty_type == DutyType.BlockProposal:
            url = self._url_for(BeaconNodePath.block_proposal)
            sedes = SignedBeaconBlock
        else:
            raise NotImplementedError(f"unrecognized duty type in duty {duty}")

        self._broadcast_operations.add(signed_operation.hash_tree_root)
        await _post_signed_operation_to_beacon_node(
            self._session, url, signed_operation, sedes
        )


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
        self, slot: Slot, randao_reveal: BLSSignature
    ) -> BeaconBlock:
        body = BeaconBlockBody.create(randao_reveal=randao_reveal)
        return BeaconBlock.create(slot=slot, body=body)

    async def publish(self, duty: Duty, signed_operation: SignedOperation) -> None:
        self.logger.debug(
            "publishing %s with signature %s to beacon node",
            duty,
            humanize_bytes(signed_operation.signature),
        )
        self.published_signatures[duty] = signed_operation.signature
