from typing import Any, Callable, Sequence

from eth.constants import ZERO_HASH32
from eth_typing import Hash32
from eth_utils import humanize_hash
import ssz
from ssz.sedes import Bitvector, List, Vector, bytes32, uint64

from eth2._utils.tuple import update_tuple_item, update_tuple_item_with_fn
from eth2.beacon.constants import JUSTIFICATION_BITS_LENGTH, ZERO_SIGNING_ROOT
from eth2.beacon.helpers import compute_epoch_at_slot
from eth2.beacon.typing import (
    Bitfield,
    Epoch,
    Gwei,
    SigningRoot,
    Slot,
    Timestamp,
    ValidatorIndex,
)
from eth2.configs import Eth2Config

from .block_headers import BeaconBlockHeader, default_beacon_block_header
from .checkpoints import Checkpoint, default_checkpoint
from .defaults import (
    default_slot,
    default_timestamp,
    default_tuple,
    default_tuple_of_size,
)
from .eth1_data import Eth1Data, default_eth1_data
from .forks import Fork, default_fork
from .pending_attestations import PendingAttestation
from .validators import Validator

default_justification_bits = Bitfield((False,) * JUSTIFICATION_BITS_LENGTH)


class BeaconState(ssz.Serializable):

    fields = [
        # Versioning
        ("genesis_time", uint64),
        ("slot", uint64),
        ("fork", Fork),
        # History
        ("latest_block_header", BeaconBlockHeader),
        (
            "block_roots",
            Vector(bytes32, 1),
        ),  # Needed to process attestations, older to newer  # noqa: E501
        ("state_roots", Vector(bytes32, 1)),
        (
            "historical_roots",
            List(bytes32, 1),
        ),  # allow for a log-sized Merkle proof from any block to any historical block root  # noqa: E501
        # Ethereum 1.0 chain
        ("eth1_data", Eth1Data),
        ("eth1_data_votes", List(Eth1Data, 1)),
        ("eth1_deposit_index", uint64),
        # Validator registry
        ("validators", List(Validator, 1)),
        ("balances", List(uint64, 1)),
        # Shuffling
        ("randao_mixes", Vector(bytes32, 1)),
        # Slashings
        (
            "slashings",
            Vector(uint64, 1),
        ),  # Balances slashed at every withdrawal period  # noqa: E501
        # Attestations
        ("previous_epoch_attestations", List(PendingAttestation, 1)),
        ("current_epoch_attestations", List(PendingAttestation, 1)),
        # Justification
        ("justification_bits", Bitvector(JUSTIFICATION_BITS_LENGTH)),
        ("previous_justified_checkpoint", Checkpoint),
        ("current_justified_checkpoint", Checkpoint),
        # Finality
        ("finalized_checkpoint", Checkpoint),
    ]

    def __init__(
        self,
        *,
        genesis_time: Timestamp = default_timestamp,
        slot: Slot = default_slot,
        fork: Fork = default_fork,
        latest_block_header: BeaconBlockHeader = default_beacon_block_header,
        block_roots: Sequence[SigningRoot] = default_tuple,
        state_roots: Sequence[Hash32] = default_tuple,
        historical_roots: Sequence[Hash32] = default_tuple,
        eth1_data: Eth1Data = default_eth1_data,
        eth1_data_votes: Sequence[Eth1Data] = default_tuple,
        eth1_deposit_index: int = 0,
        validators: Sequence[Validator] = default_tuple,
        balances: Sequence[Gwei] = default_tuple,
        randao_mixes: Sequence[Hash32] = default_tuple,
        slashings: Sequence[Gwei] = default_tuple,
        previous_epoch_attestations: Sequence[PendingAttestation] = default_tuple,
        current_epoch_attestations: Sequence[PendingAttestation] = default_tuple,
        justification_bits: Bitfield = default_justification_bits,
        previous_justified_checkpoint: Checkpoint = default_checkpoint,
        current_justified_checkpoint: Checkpoint = default_checkpoint,
        finalized_checkpoint: Checkpoint = default_checkpoint,
        config: Eth2Config = None,
        validator_and_balance_length_check: bool = True,
    ) -> None:
        # We usually want to check that the lengths of each list are the same
        # In some cases, e.g. SSZ fuzzing, they are not and we still want to instantiate an object.
        if validator_and_balance_length_check:
            if len(validators) != len(balances):
                raise ValueError(
                    f"The length of validators ({len(validators)}) and balances ({len(balances)}) "
                    "lists should be the same."
                )

        if config:
            # try to provide sane defaults
            if block_roots == default_tuple:
                block_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_SIGNING_ROOT
                )
            if state_roots == default_tuple:
                state_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_HASH32
                )
            if randao_mixes == default_tuple:
                randao_mixes = default_tuple_of_size(
                    config.EPOCHS_PER_HISTORICAL_VECTOR, ZERO_HASH32
                )
            if slashings == default_tuple:
                slashings = default_tuple_of_size(
                    config.EPOCHS_PER_SLASHINGS_VECTOR, Gwei(0)
                )

        super().__init__(
            genesis_time=genesis_time,
            slot=slot,
            fork=fork,
            latest_block_header=latest_block_header,
            block_roots=block_roots,
            state_roots=state_roots,
            historical_roots=historical_roots,
            eth1_data=eth1_data,
            eth1_data_votes=eth1_data_votes,
            eth1_deposit_index=eth1_deposit_index,
            validators=validators,
            balances=balances,
            randao_mixes=randao_mixes,
            slashings=slashings,
            previous_epoch_attestations=previous_epoch_attestations,
            current_epoch_attestations=current_epoch_attestations,
            justification_bits=justification_bits,
            previous_justified_checkpoint=previous_justified_checkpoint,
            current_justified_checkpoint=current_justified_checkpoint,
            finalized_checkpoint=finalized_checkpoint,
        )

    def __str__(self) -> str:
        return (
            f"[hash_tree_root]={humanize_hash(self.hash_tree_root)}, slot={self.slot}"
        )

    @property
    def validator_count(self) -> int:
        return len(self.validators)

    def update_validator(
        self,
        validator_index: ValidatorIndex,
        validator: Validator,
        balance: Gwei = None,
    ) -> "BeaconState":
        """
        Replace ``self.validators[validator_index]`` with ``validator``.

        Callers can optionally provide a ``balance`` which will replace
        ``self.balances[validator_index] with ``balance``.
        """
        if (
            validator_index >= len(self.validators)
            or validator_index >= len(self.balances)
            or validator_index < 0
        ):
            raise IndexError("Incorrect validator index")

        state = self.update_validator_with_fn(validator_index, lambda *_: validator)
        if balance:
            return state._update_validator_balance(validator_index, balance)
        else:
            return state

    def update_validator_with_fn(
        self,
        validator_index: ValidatorIndex,
        fn: Callable[[Validator, Any], Validator],
        *args: Any,
    ) -> "BeaconState":
        """
        Replace ``self.validators[validator_index]`` with
        the result of calling ``fn`` on the existing ``validator``.
        Any auxillary args passed in ``args`` are provided to ``fn`` along with the
        ``validator``.
        """
        if validator_index >= len(self.validators) or validator_index < 0:
            raise IndexError("Incorrect validator index")

        return self.copy(
            validators=update_tuple_item_with_fn(
                self.validators, validator_index, fn, *args
            )
        )

    def _update_validator_balance(
        self, validator_index: ValidatorIndex, balance: Gwei
    ) -> "BeaconState":
        """
        Update the balance of validator of the given ``validator_index``.
        """
        if validator_index >= len(self.balances) or validator_index < 0:
            raise IndexError("Incorrect validator index")

        return self.copy(
            balances=update_tuple_item(self.balances, validator_index, balance)
        )

    def current_epoch(self, slots_per_epoch: int) -> Epoch:
        return compute_epoch_at_slot(self.slot, slots_per_epoch)

    def previous_epoch(self, slots_per_epoch: int, genesis_epoch: Epoch) -> Epoch:
        current_epoch = self.current_epoch(slots_per_epoch)
        if current_epoch == genesis_epoch:
            return genesis_epoch
        else:
            return Epoch(current_epoch - 1)

    def next_epoch(self, slots_per_epoch: int) -> Epoch:
        return Epoch(self.current_epoch(slots_per_epoch) + 1)
