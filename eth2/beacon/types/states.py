import copy
from dataclasses import asdict, astuple, dataclass
from dataclasses import fields as dataclasses_fields

from eth.constants import ZERO_HASH32
from eth_typing import Hash32
from eth_utils import encode_hex
import ssz
from ssz.sedes import Bitvector, List, Vector, bytes32, uint64
from ssz.utils import merkleize

from eth2._utils.tuple import update_tuple_item, update_tuple_item_with_fn
from eth2.beacon.constants import JUSTIFICATION_BITS_LENGTH
from eth2.beacon.helpers import compute_epoch_of_slot
from eth2.beacon.typing import (
    Bitfield,
    Epoch,
    Gwei,
    Shard,
    Slot,
    Timestamp,
    ValidatorIndex,
)
from eth2.configs import Eth2Config

from .block_headers import BeaconBlockHeader, default_beacon_block_header
from .checkpoints import Checkpoint, default_checkpoint
from .crosslinks import Crosslink, default_crosslink
from .defaults import (
    default_shard,
    default_slot,
    default_timestamp,
    default_tuple,
    default_tuple_of_size,
)
from .eth1_data import Eth1Data, default_eth1_data
from .forks import Fork, default_fork
from .pending_attestations import PendingAttestation
from .validators import Validator

from typing import Any, Callable, Dict, Sequence, Tuple  # noqa: F401; noqa: F401


default_justification_bits = Bitfield((False,) * JUSTIFICATION_BITS_LENGTH)


class SSZBeaconState(ssz.Serializable):

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
        ("start_shard", uint64),
        ("randao_mixes", Vector(bytes32, 1)),
        ("active_index_roots", Vector(bytes32, 1)),
        ("compact_committees_roots", Vector(bytes32, 1)),
        # Slashings
        (
            "slashings",
            Vector(uint64, 1),
        ),  # Balances slashed at every withdrawal period  # noqa: E501
        # Attestations
        ("previous_epoch_attestations", List(PendingAttestation, 1)),
        ("current_epoch_attestations", List(PendingAttestation, 1)),
        # Crosslinks
        ("previous_crosslinks", Vector(Crosslink, 1)),
        ("current_crosslinks", Vector(Crosslink, 1)),
        # Justification
        ("justification_bits", Bitvector(JUSTIFICATION_BITS_LENGTH)),
        ("previous_justified_checkpoint", Checkpoint),
        ("current_justified_checkpoint", Checkpoint),
        # Finality
        ("finalized_checkpoint", Checkpoint),
    ]

    def __init__(
        self,
        # *,
        genesis_time: Timestamp = default_timestamp,
        slot: Slot = default_slot,
        fork: Fork = default_fork,
        latest_block_header: BeaconBlockHeader = default_beacon_block_header,
        block_roots: Sequence[Hash32] = default_tuple,
        state_roots: Sequence[Hash32] = default_tuple,
        historical_roots: Sequence[Hash32] = default_tuple,
        eth1_data: Eth1Data = default_eth1_data,
        eth1_data_votes: Sequence[Eth1Data] = default_tuple,
        eth1_deposit_index: int = 0,
        validators: Sequence[Validator] = default_tuple,
        balances: Sequence[Gwei] = default_tuple,
        start_shard: Shard = default_shard,
        randao_mixes: Sequence[Hash32] = default_tuple,
        active_index_roots: Sequence[Hash32] = default_tuple,
        compact_committees_roots: Sequence[Hash32] = default_tuple,
        slashings: Sequence[Gwei] = default_tuple,
        previous_epoch_attestations: Sequence[PendingAttestation] = default_tuple,
        current_epoch_attestations: Sequence[PendingAttestation] = default_tuple,
        previous_crosslinks: Sequence[Crosslink] = default_tuple,
        current_crosslinks: Sequence[Crosslink] = default_tuple,
        justification_bits: Bitfield = default_justification_bits,
        previous_justified_checkpoint: Checkpoint = default_checkpoint,
        current_justified_checkpoint: Checkpoint = default_checkpoint,
        finalized_checkpoint: Checkpoint = default_checkpoint,
        config: Eth2Config = None,
    ) -> None:
        if len(validators) != len(balances):
            raise ValueError(
                "The length of validators and balances lists should be the same."
            )

        if config:
            # try to provide sane defaults
            if block_roots == default_tuple:
                block_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_HASH32
                )
            if state_roots == default_tuple:
                state_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_HASH32
                )
            if randao_mixes == default_tuple:
                randao_mixes = default_tuple_of_size(
                    config.EPOCHS_PER_HISTORICAL_VECTOR, ZERO_HASH32
                )
            if active_index_roots == default_tuple:
                active_index_roots = default_tuple_of_size(
                    config.EPOCHS_PER_HISTORICAL_VECTOR, ZERO_HASH32
                )
            if compact_committees_roots == default_tuple:
                compact_committees_roots = default_tuple_of_size(
                    config.EPOCHS_PER_HISTORICAL_VECTOR, ZERO_HASH32
                )
            if slashings == default_tuple:
                slashings = default_tuple_of_size(
                    config.EPOCHS_PER_SLASHINGS_VECTOR, Gwei(0)
                )
            if previous_crosslinks == default_tuple:
                previous_crosslinks = default_tuple_of_size(
                    config.SHARD_COUNT, default_crosslink
                )
            if current_crosslinks == default_tuple:
                current_crosslinks = default_tuple_of_size(
                    config.SHARD_COUNT, default_crosslink
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
            start_shard=start_shard,
            randao_mixes=randao_mixes,
            active_index_roots=active_index_roots,
            compact_committees_roots=compact_committees_roots,
            slashings=slashings,
            previous_epoch_attestations=previous_epoch_attestations,
            current_epoch_attestations=current_epoch_attestations,
            previous_crosslinks=previous_crosslinks,
            current_crosslinks=current_crosslinks,
            justification_bits=justification_bits,
            previous_justified_checkpoint=previous_justified_checkpoint,
            current_justified_checkpoint=current_justified_checkpoint,
            finalized_checkpoint=finalized_checkpoint,
        )

    def __repr__(self) -> str:
        return f"<BeaconState #{self.slot} {encode_hex(self.hash_tree_root)[2:10]}>"


@dataclass
class BeaconState:
    genesis_time: Timestamp = default_timestamp
    slot: Slot = default_slot
    fork: Fork = default_fork
    latest_block_header: BeaconBlockHeader = default_beacon_block_header
    block_roots: Tuple[Hash32, ...] = default_tuple
    state_roots: Tuple[Hash32, ...] = default_tuple
    historical_roots: Tuple[Hash32, ...] = default_tuple
    eth1_data: Eth1Data = default_eth1_data
    eth1_data_votes: Tuple[Eth1Data, ...] = default_tuple
    eth1_deposit_index: int = 0
    validators: Tuple[Validator, ...] = default_tuple
    balances: Tuple[Gwei, ...] = default_tuple
    start_shard: Shard = default_shard
    randao_mixes: Tuple[Hash32, ...] = default_tuple
    active_index_roots: Tuple[Hash32, ...] = default_tuple
    compact_committees_roots: Tuple[Hash32, ...] = default_tuple
    slashings: Tuple[Gwei, ...] = default_tuple
    previous_epoch_attestations: Tuple[PendingAttestation, ...] = default_tuple
    current_epoch_attestations: Tuple[PendingAttestation, ...] = default_tuple
    previous_crosslinks: Tuple[Crosslink, ...] = default_tuple
    current_crosslinks: Tuple[Crosslink, ...] = default_tuple
    justification_bits: Bitfield = default_justification_bits
    previous_justified_checkpoint: Checkpoint = default_checkpoint
    current_justified_checkpoint: Checkpoint = default_checkpoint
    finalized_checkpoint: Checkpoint = default_checkpoint

    config = None  # type: Eth2Config

    def __init__(
        self,
        *,
        genesis_time: Timestamp = default_timestamp,
        slot: Slot = default_slot,
        fork: Fork = default_fork,
        latest_block_header: BeaconBlockHeader = default_beacon_block_header,
        block_roots: Sequence[Hash32] = default_tuple,
        state_roots: Sequence[Hash32] = default_tuple,
        historical_roots: Sequence[Hash32] = default_tuple,
        eth1_data: Eth1Data = default_eth1_data,
        eth1_data_votes: Sequence[Eth1Data] = default_tuple,
        eth1_deposit_index: int = 0,
        validators: Sequence[Validator] = default_tuple,
        balances: Sequence[Gwei] = default_tuple,
        start_shard: Shard = default_shard,
        randao_mixes: Sequence[Hash32] = default_tuple,
        active_index_roots: Sequence[Hash32] = default_tuple,
        compact_committees_roots: Sequence[Hash32] = default_tuple,
        slashings: Sequence[Gwei] = default_tuple,
        previous_epoch_attestations: Sequence[PendingAttestation] = default_tuple,
        current_epoch_attestations: Sequence[PendingAttestation] = default_tuple,
        previous_crosslinks: Sequence[Crosslink] = default_tuple,
        current_crosslinks: Sequence[Crosslink] = default_tuple,
        justification_bits: Bitfield = default_justification_bits,
        previous_justified_checkpoint: Checkpoint = default_checkpoint,
        current_justified_checkpoint: Checkpoint = default_checkpoint,
        finalized_checkpoint: Checkpoint = default_checkpoint,
        config: Eth2Config = None,
    ) -> None:
        if len(validators) != len(balances):
            raise ValueError(
                "The length of validators and balances lists should be the same."
            )

        if config:
            # try to provide sane defaults
            if block_roots == default_tuple:
                block_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_HASH32
                )
            if state_roots == default_tuple:
                state_roots = default_tuple_of_size(
                    config.SLOTS_PER_HISTORICAL_ROOT, ZERO_HASH32
                )
            if randao_mixes == default_tuple:
                randao_mixes = default_tuple_of_size(
                    config.EPOCHS_PER_HISTORICAL_VECTOR, ZERO_HASH32
                )
            if active_index_roots == default_tuple:
                active_index_roots = default_tuple_of_size(
                    config.EPOCHS_PER_HISTORICAL_VECTOR, ZERO_HASH32
                )
            if compact_committees_roots == default_tuple:
                compact_committees_roots = default_tuple_of_size(
                    config.EPOCHS_PER_HISTORICAL_VECTOR, ZERO_HASH32
                )
            if slashings == default_tuple:
                slashings = default_tuple_of_size(
                    config.EPOCHS_PER_SLASHINGS_VECTOR, Gwei(0)
                )
            if previous_crosslinks == default_tuple:
                previous_crosslinks = default_tuple_of_size(
                    config.SHARD_COUNT, default_crosslink
                )
            if current_crosslinks == default_tuple:
                current_crosslinks = default_tuple_of_size(
                    config.SHARD_COUNT, default_crosslink
                )

        self.genesis_time = genesis_time
        self.slot = slot
        self.fork = fork
        self.latest_block_header = latest_block_header
        self.block_roots = tuple(block_roots)
        self.state_roots = tuple(state_roots)
        self.historical_roots = tuple(historical_roots)
        self.eth1_data = eth1_data
        self.eth1_data_votes = tuple(eth1_data_votes)
        self.eth1_deposit_index = eth1_deposit_index
        self.validators = tuple(validators)
        self.balances = tuple(balances)
        self.start_shard = start_shard
        self.randao_mixes = tuple(randao_mixes)
        self.active_index_roots = tuple(active_index_roots)
        self.compact_committees_roots = tuple(compact_committees_roots)
        self.slashings = tuple(slashings)
        self.previous_epoch_attestations = tuple(previous_epoch_attestations)
        self.current_epoch_attestations = tuple(current_epoch_attestations)
        self.previous_crosslinks = tuple(previous_crosslinks)
        self.current_crosslinks = tuple(current_crosslinks)
        self.justification_bits = Bitfield(justification_bits)
        self.previous_justified_checkpoint = previous_justified_checkpoint
        self.current_justified_checkpoint = current_justified_checkpoint
        self.finalized_checkpoint = finalized_checkpoint

        self.config = config

        # Clean cache
        self.field_hash_tree_root_cache = {}

    def __repr__(self) -> str:
        return f"<BeaconState #{self.slot} {encode_hex(self.hash_tree_root)[2:10]}>"

    def copy(self, *args: Any, **kwargs: Any) -> "BeaconState":
        copied_state = copy.deepcopy(self)
        for field_name, value in kwargs.items():
            setattr(copied_state, field_name, value)

        if len(copied_state.validators) != len(copied_state.balances):
            raise ValueError(
                "The length of validators and balances lists should be the same."
            )

        return copied_state

    ssz_class = SSZBeaconState

    @classmethod
    def from_ssz_object(cls, ssz_object: SSZBeaconState) -> "BeaconState":
        return cls(
            **(
                {
                    name: getattr(ssz_object, name)
                    for name in ssz_object._meta.field_names
                }
            )
        )

    @property
    def ssz_object(self) -> SSZBeaconState:
        return self.ssz_class(**asdict(self))

    field_hash_tree_root_cache = {}  # type: Dict[Any, Hash32]

    @property
    def hash_tree_root(self) -> Hash32:
        merkle_leaves: Tuple[Hash32, ...] = ()
        for element, sedes, field_name in zip(
            tuple(getattr(self, f.name) for f in dataclasses_fields(self)),
            self.ssz_class._meta.container_sedes.field_sedes,
            self.ssz_class._meta.field_names,
        ):
            key = field_name + str(element)
            if key in self.field_hash_tree_root_cache:
                field_hash_tree_root = self.field_hash_tree_root_cache[key]
            else:
                field_hash_tree_root = sedes.get_hash_tree_root(element)
                self.field_hash_tree_root_cache[key] = field_hash_tree_root
            merkle_leaves += (field_hash_tree_root,)

        return merkleize(merkle_leaves)

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
        return compute_epoch_of_slot(self.slot, slots_per_epoch)

    def previous_epoch(self, slots_per_epoch: int, genesis_epoch: Epoch) -> Epoch:
        current_epoch = self.current_epoch(slots_per_epoch)
        if current_epoch == genesis_epoch:
            return genesis_epoch
        else:
            return Epoch(current_epoch - 1)

    def next_epoch(self, slots_per_epoch: int) -> Epoch:
        return Epoch(self.current_epoch(slots_per_epoch) + 1)
