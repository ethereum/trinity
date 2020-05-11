from abc import ABC
from dataclasses import dataclass, field
from typing import Collection, Iterator, Set

from eth_typing import BLSPubkey, BLSSignature
from eth_utils import to_tuple

from eth.exceptions import BlockNotFound

from eth2.beacon.exceptions import NoCommitteeAssignment
from eth2.beacon.chains.base import BaseBeaconChain
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


@dataclass
class SyncStatus:
    is_syncing: bool
    starting_slot: Slot
    current_slot: Slot
    highest_slot: Slot


class SyncerAPI(ABC):
    async def get_status(self) -> SyncStatus:
        ...


@dataclass
class ValidatorDuty:
    validator_pubkey: BLSPubkey
    attestation_slot: Slot
    committee_index: CommitteeIndex
    block_proposal_slot: Slot


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


@dataclass
class Context:
    client_identifier: str
    genesis_time: int  # Unix timestamp
    eth2_config: Eth2Config
    syncer: SyncerAPI
    chain: BaseBeaconChain
    clock: Clock
    _broadcast_operations: Set[Root] = field(default_factory=set)

    async def get_sync_status(self) -> SyncStatus:
        return await self.syncer.get_status()

    @to_tuple
    def get_validator_duties(
        self, public_keys: Collection[BLSPubkey], epoch: Epoch
    ) -> Iterator[ValidatorDuty]:
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
                block_proposal_slot = state.slot
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

    def get_block_proposal(
        self, slot: Slot, randao_reveal: BLSSignature
    ) -> BeaconBlock:
        target_slot = Slot(max(slot - 1, 0))
        head_state = self.chain.get_head_state()
        parent_state = self.chain.advance_state_to_slot(target_slot, head_state)
        parent_block_root = parent_state.latest_block_header.hash_tree_root
        state_machine = self.chain.get_state_machine(at_slot=target_slot)
        return create_block_proposal(
            slot, parent_block_root, randao_reveal, parent_state, state_machine
        )

    async def broadcast_block(self, signed_block: SignedBeaconBlock):
        # self.logger.info(
        #     "broadcasting block with root %s", humanize_hash(block.hash_tree_root)
        #   )
        # TODO the actual brodcast
        self._broadcast_operations.add(signed_block.hash_tree_root)

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
        # self.logger.info(
        #     "broadcasting attestation with root %s",
        #     humanize_hash(attestation.hash_tree_root),
        # )
        # TODO the actual brodcast
        self._broadcast_operations.add(attestation.hash_tree_root)
        return True
