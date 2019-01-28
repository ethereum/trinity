from typing import (
    Sequence,
)

from eth_typing import (
    Hash32,
)
from eth_utils import (
    encode_hex,
)

import rlp
from rlp.sedes import (
    CountableList,
)

from eth2.beacon.sedes import (
    uint24,
    uint64,
    hash32,
)
from eth2.beacon._utils.hash import (
    hash_eth2,
)
from eth2.beacon.typing import (
    Gwei,
    ShardNumber,
    SlotNumber,
    EpochNumber,
    Timestamp,
    ValidatorIndex,
)

from .eth1_data import Eth1Data
from .eth1_data_vote import Eth1DataVote
from .custody_challenges import CustodyChallenge
from .crosslink_records import CrosslinkRecord
from .fork_data import ForkData
from .pending_attestation_records import PendingAttestationRecord
from .shard_reassignment_records import ShardReassignmentRecord
from .validator_records import ValidatorRecord


class BeaconState(rlp.Serializable):
    """
    Note: using RLP until we have standardized serialization format.
    """
    fields = [
        # Misc
        ('slot', uint64),
        ('genesis_time', uint64),
        ('fork_data', ForkData),  # For versioning hard forks

        # Validator registry
        ('validator_registry', CountableList(ValidatorRecord)),
        ('validator_balances', CountableList(uint64)),
        ('validator_registry_latest_change_slot', uint64),
        ('validator_registry_exit_count', uint64),
        ('validator_registry_delta_chain_tip', hash32),  # For light clients to easily track delta

        # Randomness and committees
        ('latest_randao_mixes', CountableList(hash32)),
        ('latest_vdf_outputs', CountableList(hash32)),

        # TODO Remove `persistent_committee_reassignments`
        ('persistent_committees', CountableList(CountableList(uint24))),
        ('persistent_committee_reassignments', CountableList(ShardReassignmentRecord)),
        ('previous_epoch_start_shard', uint64),
        ('current_epoch_start_shard', uint64),
        ('previous_epoch_calculation_slot', uint64),
        ('current_epoch_calculation_slot', uint64),
        ('previous_epoch_randao_mix', hash32),
        ('current_epoch_randao_mix', hash32),

        # Custody challenges
        ('custody_challenges', CountableList(CustodyChallenge)),

        # Finality
        ('previous_justified_epoch', uint64),
        ('justified_epoch', uint64),

        # Note: justification_bitfield is meant to be defined as an integer type,
        # so its bit operation in Python and is easier to specify and implement.
        ('justification_bitfield', uint64),
        ('finalized_epoch', uint64),

        # Recent state
        ('latest_crosslinks', CountableList(CrosslinkRecord)),
        ('latest_block_roots', CountableList(hash32)),  # Needed to process attestations, older to newer  # noqa: E501
        ('latest_penalized_exit_balances', CountableList(uint64)),  # Balances penalized at every withdrawal period  # noqa: E501
        ('latest_attestations', CountableList(PendingAttestationRecord)),
        ('batched_block_roots', CountableList(Hash32)),  # allow for a log-sized Merkle proof from any block to any historical block root"  # noqa: E501

        # PoW receipt root
        ('latest_eth1_data', Eth1Data),
        ('eth1_data_votes', CountableList(Eth1DataVote)),
    ]

    def __init__(
            self,
            slot: SlotNumber,
            genesis_time: Timestamp,
            fork_data: ForkData,
            validator_registry_latest_change_slot: SlotNumber,
            validator_registry_exit_count: int,
            validator_registry_delta_chain_tip: Hash32,
            previous_epoch_start_shard: ShardNumber,
            current_epoch_start_shard: ShardNumber,
            previous_epoch_calculation_slot: SlotNumber,
            current_epoch_calculation_slot: SlotNumber,
            previous_epoch_randao_mix: Hash32,
            current_epoch_randao_mix: Hash32,
            previous_justified_epoch: EpochNumber,
            justified_epoch: EpochNumber,
            justification_bitfield: int,
            finalized_epoch: EpochNumber,
            latest_eth1_data: Eth1Data,
            validator_registry: Sequence[ValidatorRecord]=(),
            validator_balances: Sequence[Gwei]=(),
            latest_randao_mixes: Sequence[Hash32]=(),
            latest_vdf_outputs: Sequence[Hash32]=(),
            persistent_committees: Sequence[Sequence[ValidatorIndex]]=(),
            persistent_committee_reassignments: Sequence[ShardReassignmentRecord]=(),
            custody_challenges: Sequence[CustodyChallenge]=(),
            latest_crosslinks: Sequence[CrosslinkRecord]=(),
            latest_block_roots: Sequence[Hash32]=(),
            latest_penalized_exit_balances: Sequence[Gwei]=(),
            batched_block_roots: Sequence[Hash32]=(),
            latest_attestations: Sequence[PendingAttestationRecord]=(),
            eth1_data_votes: Sequence[Eth1DataVote]=()
    ) -> None:
        if len(validator_registry) != len(validator_balances):
            raise ValueError(
                "The length of validator_registry and validator_balances should be the same."
            )
        super().__init__(
            # Misc
            slot=slot,
            genesis_time=genesis_time,
            fork_data=fork_data,
            # Validator registry
            validator_registry=validator_registry,
            validator_balances=validator_balances,
            validator_registry_latest_change_slot=validator_registry_latest_change_slot,
            validator_registry_exit_count=validator_registry_exit_count,
            validator_registry_delta_chain_tip=validator_registry_delta_chain_tip,
            # Randomness and committees
            latest_randao_mixes=latest_randao_mixes,
            latest_vdf_outputs=latest_vdf_outputs,
            persistent_committees=persistent_committees,
            persistent_committee_reassignments=persistent_committee_reassignments,
            previous_epoch_start_shard=previous_epoch_start_shard,
            current_epoch_start_shard=current_epoch_start_shard,
            previous_epoch_calculation_slot=previous_epoch_calculation_slot,
            current_epoch_calculation_slot=current_epoch_calculation_slot,
            previous_epoch_randao_mix=previous_epoch_randao_mix,
            current_epoch_randao_mix=current_epoch_randao_mix,
            # Proof of Custody
            custody_challenges=custody_challenges,
            # Finality
            previous_justified_epoch=previous_justified_epoch,
            justified_epoch=justified_epoch,
            justification_bitfield=justification_bitfield,
            finalized_epoch=finalized_epoch,
            # Recent state
            latest_crosslinks=latest_crosslinks,
            latest_block_roots=latest_block_roots,
            latest_penalized_exit_balances=latest_penalized_exit_balances,
            latest_attestations=latest_attestations,
            batched_block_roots=batched_block_roots,
            # PoW receipt root
            latest_eth1_data=latest_eth1_data,
            eth1_data_votes=eth1_data_votes,
        )

    def __repr__(self) -> str:
        return 'BeaconState #{0}>'.format(
            encode_hex(self.root)[2:10],
        )

    _hash = None

    @property
    def hash(self) -> Hash32:
        if self._hash is None:
            self._hash = hash_eth2(rlp.encode(self))
        return self._hash

    @property
    def root(self) -> Hash32:
        # Alias of `hash`.
        # Using flat hash, might change to SSZ tree hash.
        return self.hash

    @property
    def num_validators(self) -> int:
        return len(self.validator_registry)

    @property
    def num_crosslinks(self) -> int:
        return len(self.latest_crosslinks)

    def update_validator_registry(self,
                                  validator_index: ValidatorIndex,
                                  validator: ValidatorRecord) -> 'BeaconState':
        """
        Replace ``self.validator_registry[validator_index]`` with ``validator``.
        """
        if validator_index >= self.num_validators or validator_index < 0:
            raise IndexError("Incorrect validator index")

        validator_registry = list(self.validator_registry)
        validator_registry[validator_index] = validator

        updated_state = self.copy(
            validator_registry=tuple(validator_registry),
        )
        return updated_state

    def update_validator_balance(self,
                                 validator_index: ValidatorIndex,
                                 balance: Gwei) -> 'BeaconState':
        """
        Update the balance of validator of the given ``validator_index``.
        """
        if validator_index >= self.num_validators or validator_index < 0:
            raise IndexError("Incorrect validator index")

        validator_balances = list(self.validator_balances)
        validator_balances[validator_index] = balance

        updated_state = self.copy(
            validator_balances=tuple(validator_balances),
        )
        return updated_state

    def update_validator(self,
                         validator_index: ValidatorIndex,
                         validator: ValidatorRecord,
                         balance: Gwei) -> 'BeaconState':
        """
        Update the ``ValidatorRecord`` and balance of validator of the given ``validator_index``.
        """
        state = self.update_validator_registry(validator_index, validator)
        state = state.update_validator_balance(validator_index, balance)
        return state
