from typing import TYPE_CHECKING, Callable, Sequence, Set, Tuple

from eth_typing import Hash32
from eth_utils import ValidationError
import ssz

from eth2._utils.hash import hash_eth2
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.types.fork_data import ForkData
from eth2.beacon.types.forks import Fork
from eth2.beacon.types.signing_root import SigningData
from eth2.beacon.types.validators import Validator
from eth2.beacon.typing import (
    Domain,
    DomainType,
    Epoch,
    ForkDigest,
    Gwei,
    Root,
    Slot,
    ValidatorIndex,
    Version,
    default_root,
    default_version,
)
from eth2.configs import Eth2Config

if TYPE_CHECKING:
    from eth2.beacon.types.states import BeaconState  # noqa: F401


def compute_epoch_at_slot(slot: Slot, slots_per_epoch: int) -> Epoch:
    return Epoch(slot // slots_per_epoch)


def compute_start_slot_at_epoch(epoch: Epoch, slots_per_epoch: int) -> Slot:
    return Slot(epoch * slots_per_epoch)


def get_active_validator_indices(
    validators: Sequence[Validator], epoch: Epoch
) -> Tuple[ValidatorIndex, ...]:
    """
    Get indices of active validators from ``validators``.
    """
    return tuple(
        ValidatorIndex(index)
        for index, validator in enumerate(validators)
        if validator.is_active(epoch)
    )


def _get_historical_root(
    historical_roots: Sequence[Root],
    state_slot: Slot,
    slot: Slot,
    slots_per_historical_root: int,
) -> Root:
    """
    Return the historical root at a recent ``slot``.
    """
    if slot >= state_slot:
        raise ValidationError(
            f"slot ({slot}) should be less than state.slot ({state_slot})"
        )

    if state_slot > slot + slots_per_historical_root:
        raise ValidationError(
            f"state.slot ({state_slot}) should be less than or equal to "
            f"(slot + slots_per_historical_root) ({slot + slots_per_historical_root}), "
            f"where slot={slot}, slots_per_historical_root={slots_per_historical_root}"
        )

    return historical_roots[slot % slots_per_historical_root]


def get_state_root_at_slot(
    state: "BeaconState", slot: Slot, slots_per_historical_root: int
) -> Hash32:
    """
    Return state root at recent ``slot``.
    """
    if slot == state.slot:
        return state.hash_tree_root
    else:
        return _get_historical_root(
            state.state_roots, state.slot, slot, slots_per_historical_root
        )


def get_block_root_at_slot(
    state: "BeaconState", slot: Slot, slots_per_historical_root: int
) -> Root:
    """
    Return the block root at a recent ``slot``.
    """
    return _get_historical_root(
        state.block_roots, state.slot, slot, slots_per_historical_root
    )


def get_block_root(
    state: "BeaconState",
    epoch: Epoch,
    slots_per_epoch: int,
    slots_per_historical_root: int,
) -> Root:
    return get_block_root_at_slot(
        state,
        compute_start_slot_at_epoch(epoch, slots_per_epoch),
        slots_per_historical_root,
    )


def get_randao_mix(
    state: "BeaconState", epoch: Epoch, epochs_per_historical_vector: int
) -> Hash32:
    """
    Return the randao mix at a recent ``epoch``.
    """
    return state.randao_mixes[epoch % epochs_per_historical_vector]


def _epoch_for_seed(epoch: Epoch) -> Hash32:
    return Hash32(epoch.to_bytes(8, byteorder="little"))


RandaoProvider = Callable[["BeaconState", Epoch, int], Hash32]
ActiveIndexRootProvider = Callable[["BeaconState", Epoch, int], Hash32]


def _get_seed(
    state: "BeaconState",
    epoch: Epoch,
    domain_type: DomainType,
    randao_provider: RandaoProvider,
    epoch_provider: Callable[[Epoch], Hash32],
    config: Eth2Config,
) -> Hash32:
    randao_mix = randao_provider(
        state,
        Epoch(
            epoch + config.EPOCHS_PER_HISTORICAL_VECTOR - config.MIN_SEED_LOOKAHEAD - 1
        ),
        config.EPOCHS_PER_HISTORICAL_VECTOR,
    )
    epoch_as_bytes = epoch_provider(epoch)

    return hash_eth2(domain_type + epoch_as_bytes + randao_mix)


def get_seed(
    state: "BeaconState", epoch: Epoch, domain_type: DomainType, config: Eth2Config
) -> Hash32:
    """
    Generate a seed for the given ``epoch``.
    """
    return _get_seed(state, epoch, domain_type, get_randao_mix, _epoch_for_seed, config)


def get_total_balance(
    state: "BeaconState", validator_indices: Set[ValidatorIndex], config: Eth2Config
) -> Gwei:
    """
    Return the combined effective balance of the ``indices``.
    ``EFFECTIVE_BALANCE_INCREMENT`` Gwei minimum to avoid divisions by zero.
    Math safe up to ~10B ETH, afterwhich this overflows uint64.
    """
    return Gwei(
        max(
            sum(
                state.validators[index].effective_balance for index in validator_indices
            ),
            config.EFFECTIVE_BALANCE_INCREMENT,
        )
    )


def _get_fork_version(fork: Fork, epoch: Epoch) -> Version:
    """
    Return the current ``fork_version`` from the given ``fork`` and ``epoch``.
    """
    if epoch < fork.epoch:
        return fork.previous_version
    else:
        return fork.current_version


def signature_domain_to_domain_type(s: SignatureDomain) -> DomainType:
    return DomainType(s.to_bytes(4, byteorder="little"))


def compute_domain(
    signature_domain: SignatureDomain,
    fork_version: Version = default_version,
    genesis_validators_root: Root = default_root,
) -> Domain:
    """
    NOTE: we deviate from the spec here by taking the enum ``SignatureDomain`` and
    converting before creating the domain.
    """
    domain_type = signature_domain_to_domain_type(signature_domain)
    fork_data_root = compute_fork_data_root(fork_version, genesis_validators_root)
    return Domain(Hash32(domain_type + fork_data_root[:28]))


def get_domain(
    state: "BeaconState",
    signature_domain: SignatureDomain,
    slots_per_epoch: int,
    message_epoch: Epoch = None,
) -> Domain:
    """
    Return the domain number of the current fork and ``domain_type``.
    """
    epoch = (
        state.current_epoch(slots_per_epoch) if message_epoch is None else message_epoch
    )
    fork_version = _get_fork_version(state.fork, epoch)
    return compute_domain(signature_domain, fork_version, state.genesis_validators_root)


def compute_fork_data_root(
    current_version: Version, genesis_validators_root: Root
) -> Root:
    return ForkData.create(current_version, genesis_validators_root).hash_tree_root


def compute_fork_digest(
    current_version: Version, genesis_validators_root: Root
) -> ForkDigest:
    return ForkDigest(
        compute_fork_data_root(current_version, genesis_validators_root)[:4]
    )


def compute_signing_root(object: ssz.Serializable, domain: Domain) -> Root:
    """
    Return the signing root of an object by calculating the root of the object-domain tree.
    """
    domain_wrapped_object = SigningData.create(
        object_root=object.hash_tree_root, domain=domain
    )
    return domain_wrapped_object.hash_tree_root
