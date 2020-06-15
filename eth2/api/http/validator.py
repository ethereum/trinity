"""
This module contains the eth2 HTTP validator API connecting a validator client to a beacon node.
"""
from abc import ABC
from dataclasses import asdict, dataclass, field
from enum import Enum, unique
import logging
from typing import Collection, Iterable, Optional, Set

from eth.exceptions import BlockNotFound
from eth_typing import BLSPubkey, BLSSignature
from eth_utils import decode_hex, encode_hex, to_tuple
from ssz.tools.dump import to_formatted_dict
from ssz.tools.parse import from_formatted_dict

from eth2.beacon.chains.base import BaseBeaconChain
from eth2.beacon.committee_helpers import get_beacon_proposer_index
from eth2.beacon.constants import GENESIS_EPOCH
from eth2.beacon.exceptions import NoCommitteeAssignment
from eth2.beacon.helpers import (
    compute_epoch_at_slot,
    compute_start_slot_at_epoch,
    get_block_root_at_slot,
)
from eth2.beacon.tools.builder.committee_assignment import get_committee_assignment
from eth2.beacon.tools.builder.proposer import create_block_proposal, is_proposer
from eth2.beacon.types.attestations import Attestation, AttestationData
from eth2.beacon.types.blocks import BeaconBlock, SignedBeaconBlock
from eth2.beacon.types.checkpoints import Checkpoint
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Bitfield, CommitteeIndex, Epoch, Root, Slot
from eth2.clock import Clock
from eth2.configs import Eth2Config
from trinity._utils.trio_utils import Request, Response

logger = logging.getLogger("eth2.api.http.validator")

# TODO what is a reasonable number here?
MAX_SEARCH_SLOTS = 500


class ServerError(Exception):
    pass


class InvalidRequest(Exception):
    pass


def _get_target_checkpoint(
    state: BeaconState, head_root: Root, config: Eth2Config
) -> Checkpoint:
    epoch = state.current_epoch(config.SLOTS_PER_EPOCH)
    start_slot = compute_start_slot_at_epoch(epoch, config.SLOTS_PER_EPOCH)
    if start_slot == state.slot:
        root = head_root
    else:
        root = get_block_root_at_slot(
            state, start_slot, config.SLOTS_PER_HISTORICAL_ROOT
        )
    return Checkpoint.create(epoch=epoch, root=root)


@unique
class Paths(Enum):
    node_version = "/node/version"
    genesis_time = "/node/genesis_time"
    sync_status = "/node/syncing"
    validator_duties = "/validator/duties"
    block_proposal = "/validator/block"
    attestation = "/validator/attestation"


@dataclass
class SyncStatus:
    is_syncing: bool
    starting_slot: Slot
    current_slot: Slot
    highest_slot: Slot


class SyncerAPI(ABC):
    async def get_status(self) -> SyncStatus:
        ...


class BlockBroadcasterAPI(ABC):
    async def broadcast_block(self, block: SignedBeaconBlock) -> None:
        ...


@dataclass
class ValidatorDuty:
    validator_pubkey: BLSPubkey
    attestation_slot: Slot
    committee_index: CommitteeIndex
    block_proposal_slot: Slot


@dataclass
class Context:
    client_identifier: str
    genesis_time: int  # Unix timestamp
    eth2_config: Eth2Config
    syncer: SyncerAPI
    chain: BaseBeaconChain
    clock: Clock
    block_broadcaster: BlockBroadcasterAPI
    _broadcast_operations: Set[Root] = field(default_factory=set)

    async def get_sync_status(self) -> SyncStatus:
        return await self.syncer.get_status()

    @to_tuple
    def get_validator_duties(
        self, public_keys: Collection[BLSPubkey], epoch: Epoch
    ) -> Iterable[ValidatorDuty]:
        if epoch < GENESIS_EPOCH:
            return ()

        current_tick = self.clock.compute_current_tick()
        state = self.chain.advance_state_to_slot(current_tick.slot)
        for public_key in public_keys:
            validator_index = state.get_validator_index_for_public_key(public_key)
            try:
                committee_assignment = get_committee_assignment(
                    state, self.eth2_config, epoch, validator_index
                )
            except NoCommitteeAssignment:
                continue

            if is_proposer(state, validator_index, self.eth2_config):
                # TODO (ralexstokes) clean this up!
                if state.slot != 0:
                    block_proposal_slot = state.slot
                else:
                    block_proposal_slot = Slot((1 << 64) - 1)
            else:
                # NOTE: temporary sentinel value for "no slot"
                # The API has since been updated w/ much better ergonomics
                block_proposal_slot = Slot((1 << 64) - 1)
            yield ValidatorDuty(
                public_key,
                committee_assignment.slot,
                committee_assignment.committee_index,
                block_proposal_slot,
            )

    def _search_linearly_for_parent(
        self, target_slot: Slot
    ) -> Optional[SignedBeaconBlock]:
        """
        Linear search for a canonical block in the chain starting at ``target_slot``
        and going backwards until finding a block. This search happens when
        there are skipped slots in the chain.

        NOTE: The expected number of skipped slots during normal protocol operation is very low.
        """
        for _slot in range(target_slot - 1, target_slot - 1 - MAX_SEARCH_SLOTS, -1):
            try:
                return self.chain.get_canonical_block_by_slot(Slot(_slot))
            except BlockNotFound:
                continue
        return None

    def get_block_proposal(
        self, slot: Slot, randao_reveal: BLSSignature
    ) -> BeaconBlock:
        if slot < 1:
            raise InvalidRequest()

        target_slot = Slot(max(slot - 1, 0))
        try:
            parent = self.chain.get_canonical_block_by_slot(target_slot)
        except BlockNotFound:
            # as an optimization, try checking the head
            parent = self.chain.get_canonical_head()
            if parent.slot > target_slot:
                # NOTE: if parent.slot == target_slot, we are not under this block.
                # NOTE: head has greater slot than the target, while it is odd
                # a client may want a block here, let's try to satisfy the request.
                # TODO: should we allow this behavior?
                # TODO: consider a more sophisticated search strategy if we can detect ``slot``
                # is far from the canonical head, e.g. binary search across slots
                parent = self._search_linearly_for_parent(target_slot)
                if not parent:
                    raise ServerError()
            else:
                # the head is a satisfactory parent, continue!
                pass

        parent_block_root = parent.message.hash_tree_root
        parent_state = self.chain.get_state_by_root(parent.state_root)
        parent_state = self.chain.advance_state_to_slot(target_slot, parent_state)
        state_machine = self.chain.get_state_machine(at_slot=slot)

        state_at_slot = state_machine.state_transition.apply_state_transition(
            parent_state, future_slot=slot
        )
        proposer_index = get_beacon_proposer_index(state_at_slot, self.eth2_config)

        # TODO: query for latest eth1 data...
        eth1_data = parent_state.eth1_data

        return create_block_proposal(
            slot,
            proposer_index,
            parent_block_root,
            randao_reveal,
            eth1_data,
            parent_state,
            state_machine,
        )

    async def broadcast_block(self, block: SignedBeaconBlock) -> bool:
        logger.debug("broadcasting block with root %s", block.hash_tree_root.hex())
        await self.block_broadcaster.broadcast_block(block)
        self._broadcast_operations.add(block.hash_tree_root)
        return True

    def get_attestation(
        self, public_key: BLSPubkey, slot: Slot, committee_index: CommitteeIndex
    ) -> Attestation:
        current_tick = self.clock.compute_current_tick()
        state = self.chain.advance_state_to_slot(current_tick.slot)
        try:
            block = self.chain.get_canonical_block_by_slot(slot)
        except BlockNotFound:
            # try to find earlier block, assuming skipped slots
            block = self.chain.get_canonical_head()
            # sanity check the assumption in this leg of the conditional
            assert block.slot < slot

        target_checkpoint = _get_target_checkpoint(
            state, block.hash_tree_root, self.eth2_config
        )
        data = AttestationData.create(
            slot=slot,
            index=committee_index,
            beacon_block_root=block.hash_tree_root,
            source=state.current_justified_checkpoint,
            target=target_checkpoint,
        )

        validator_index = state.get_validator_index_for_public_key(public_key)
        epoch = compute_epoch_at_slot(slot, self.eth2_config.SLOTS_PER_EPOCH)
        committee_assignment = get_committee_assignment(
            state, self.eth2_config, epoch, validator_index
        )
        committee = committee_assignment.committee
        committee_validator_index = committee.index(validator_index)
        aggregation_bits = Bitfield(
            tuple(i == committee_validator_index for i in range(len(committee)))
        )
        return Attestation.create(aggregation_bits=aggregation_bits, data=data)

    async def broadcast_attestation(self, attestation: Attestation) -> bool:
        logger.debug(
            "broadcasting attestation with root %s", attestation.hash_tree_root.hex()
        )
        # TODO the actual brodcast
        self._broadcast_operations.add(attestation.hash_tree_root)
        return True


async def _get_node_version(context: Context, _request: Request) -> Response:
    return context.client_identifier


async def _get_genesis_time(context: Context, _request: Request) -> Response:
    return context.genesis_time


async def _get_sync_status(context: Context, _request: Request) -> Response:
    status = await context.get_sync_status()
    status_data = asdict(status)
    del status_data["is_syncing"]
    return {"is_syncing": status.is_syncing, "sync_status": status_data}


def _marshal_duty(duty: ValidatorDuty) -> Response:
    duty_data = asdict(duty)
    duty_data["validator_pubkey"] = encode_hex(duty.validator_pubkey)
    return duty_data


async def _get_validator_duties(context: Context, request: Request) -> Response:
    if not isinstance(request, dict):
        return ()

    if "validator_pubkeys" not in request:
        return ()
    public_keys = tuple(map(decode_hex, request["validator_pubkeys"].split(",")))
    epoch = Epoch(int(request["epoch"]))
    duties = context.get_validator_duties(public_keys, epoch)
    return tuple(map(_marshal_duty, duties))


async def _get_block_proposal(context: Context, request: Request) -> Response:
    if not isinstance(request, dict):
        return {}

    slot = Slot(int(request["slot"]))
    randao_reveal = BLSSignature(
        decode_hex(request["randao_reveal"]).ljust(96, b"\x00")
    )
    try:
        block = context.get_block_proposal(slot, randao_reveal)
        return to_formatted_dict(block)
    except Exception as e:
        # TODO error handling...
        return {"error": str(e)}


async def _post_block_proposal(context: Context, request: Request) -> Response:
    block = from_formatted_dict(request, SignedBeaconBlock)
    return await context.broadcast_block(block)


async def _get_attestation(context: Context, request: Request) -> Response:
    if not isinstance(request, dict):
        return {}

    public_key = BLSPubkey(decode_hex(request["validator_pubkey"]))
    slot = Slot(int(request["slot"]))
    committee_index = CommitteeIndex(int(request["committee_index"]))
    attestation = context.get_attestation(public_key, slot, committee_index)
    return to_formatted_dict(attestation)


async def _post_attestation(context: Context, request: Request) -> Response:
    attestation = from_formatted_dict(request, Attestation)
    return await context.broadcast_attestation(attestation)


GET = "GET"
POST = "POST"


ServerHandlers = {
    Paths.node_version.value: {GET: _get_node_version},
    Paths.genesis_time.value: {GET: _get_genesis_time},
    Paths.sync_status.value: {GET: _get_sync_status},
    Paths.validator_duties.value: {GET: _get_validator_duties},
    Paths.block_proposal.value: {GET: _get_block_proposal, POST: _post_block_proposal},
    Paths.attestation.value: {GET: _get_attestation, POST: _post_attestation},
}
