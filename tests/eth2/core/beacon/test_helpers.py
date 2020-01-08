from eth.constants import ZERO_HASH32
from eth_utils import ValidationError, to_tuple
import pytest

from eth2._utils.hash import hash_eth2
from eth2.beacon.constants import FAR_FUTURE_EPOCH, GWEI_PER_ETH
from eth2.beacon.helpers import (
    _get_fork_version,
    _get_seed,
    compute_start_slot_at_epoch,
    get_active_validator_indices,
    get_block_root_at_slot,
    get_domain,
    get_state_root_at_slot,
    get_total_balance,
    signature_domain_to_domain_type,
)
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.types.forks import Fork
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.validators import Validator


@to_tuple
def get_pseudo_chain(length, genesis_block):
    """
    Get a pseudo chain, only slot and parent_root are valid.
    """
    block = genesis_block
    yield genesis_block
    for slot in range(1, length * 3):
        block = genesis_block.mset(
            "slot",
            slot,
            "parent_root",
            block.signing_root,
            "state_root",
            slot.to_bytes(32, "big"),
        )
        yield block


def generate_mock_latest_historical_roots(
    genesis_block, current_slot, slots_per_epoch, slots_per_historical_root
):
    if current_slot < slots_per_historical_root:
        filling = current_slot % slots_per_historical_root
        padding = slots_per_historical_root - current_slot
    else:
        filling = slots_per_historical_root
        padding = 0
    chain_length = (current_slot // slots_per_epoch + 1) * slots_per_epoch
    blocks = get_pseudo_chain(chain_length, genesis_block)
    block_roots = [
        block.signing_root for block in blocks[current_slot - filling : current_slot]
    ] + [ZERO_HASH32 for _ in range(padding)]

    state_roots = [
        block.state_root for block in blocks[current_slot - filling : current_slot]
    ] + [ZERO_HASH32 for _ in range(padding)]
    return blocks, block_roots, state_roots


#
# Get historical roots
#
@pytest.mark.parametrize(
    ("slots_per_historical_root,current_slot,target_slot,success"),
    [
        (32, 10, 0, True),
        (32, 10, 9, True),
        (32, 10, 10, False),
        (32, 128, 0, False),
        (32, 128, 127, True),
        (32, 128, 128, False),
        (128, 128, 0, True),
        (128, 128, 127, True),
        (128, 128, 128, False),
    ],
)
def test_get_block_root_at_slot(
    sample_beacon_state_params,
    current_slot,
    target_slot,
    success,
    slots_per_epoch,
    slots_per_historical_root,
    sample_block,
):
    blocks, block_roots, _ = generate_mock_latest_historical_roots(
        sample_block, current_slot, slots_per_epoch, slots_per_historical_root
    )
    state = BeaconState.create(**sample_beacon_state_params).mset(
        "slot", current_slot, "block_roots", block_roots
    )

    if success:
        block_root = get_block_root_at_slot(
            state, target_slot, slots_per_historical_root
        )
        assert block_root == blocks[target_slot].signing_root
    else:
        with pytest.raises(ValidationError):
            get_block_root_at_slot(state, target_slot, slots_per_historical_root)


@pytest.mark.parametrize(
    ("slots_per_historical_root,current_slot,target_slot,success"),
    [
        (32, 10, 0, True),
        (32, 10, 9, True),
        (32, 10, 10, True),
        (32, 128, 0, False),
        (32, 128, 127, True),
        (32, 128, 128, True),
        (128, 128, 0, True),
        (128, 128, 127, True),
        (128, 128, 128, True),
    ],
)
def test_get_state_root_at_slot(
    sample_beacon_state_params,
    current_slot,
    target_slot,
    success,
    slots_per_epoch,
    slots_per_historical_root,
    sample_block,
):
    blocks, _, state_roots = generate_mock_latest_historical_roots(
        sample_block, current_slot, slots_per_epoch, slots_per_historical_root
    )
    state = BeaconState.create(**sample_beacon_state_params).mset(
        "slot", current_slot, "state_roots", state_roots
    )
    blocks[current_slot].state_root = state.hash_tree_root

    if success:
        state_root = get_state_root_at_slot(
            state, target_slot, slots_per_historical_root
        )
        assert state_root == blocks[target_slot].state_root
    else:
        with pytest.raises(ValidationError):
            get_state_root_at_slot(state, target_slot, slots_per_historical_root)


def test_get_active_validator_indices(sample_validator_record_params):
    current_epoch = 1
    # 3 validators are ACTIVE
    validators = [
        Validator.create(**sample_validator_record_params).mset(
            "activation_epoch", 0, "exit_epoch", FAR_FUTURE_EPOCH
        )
        for i in range(3)
    ]
    active_validator_indices = get_active_validator_indices(validators, current_epoch)
    assert len(active_validator_indices) == 3

    validators[0] = validators[0].set(
        "activation_epoch", current_epoch + 1  # activation_epoch > current_epoch
    )
    active_validator_indices = get_active_validator_indices(validators, current_epoch)
    assert len(active_validator_indices) == 2

    validators[1] = validators[1].set(
        "exit_epoch", current_epoch  # current_epoch == exit_epoch
    )
    active_validator_indices = get_active_validator_indices(validators, current_epoch)
    assert len(active_validator_indices) == 1


@pytest.mark.parametrize(
    ("balances," "validator_indices," "expected"),
    [
        (tuple(), tuple(), 1),
        ((32 * GWEI_PER_ETH, 32 * GWEI_PER_ETH), (0, 1), 64 * GWEI_PER_ETH),
        ((32 * GWEI_PER_ETH, 32 * GWEI_PER_ETH), (1,), 32 * GWEI_PER_ETH),
    ],
)
def test_get_total_balance(genesis_state, balances, validator_indices, expected):
    state = genesis_state
    for i, index in enumerate(validator_indices):
        state = state.transform(["balances", index], balances[i])
    total_balance = get_total_balance(state, validator_indices)
    assert total_balance == expected


@pytest.mark.parametrize(
    ("previous_version," "current_version," "epoch," "current_epoch," "expected"),
    [
        (b"\x00" * 4, b"\x00" * 4, 0, 0, b"\x00" * 4),
        (b"\x00" * 4, b"\x00" * 4, 0, 1, b"\x00" * 4),
        (b"\x00" * 4, b"\x11" * 4, 20, 10, b"\x00" * 4),
        (b"\x00" * 4, b"\x11" * 4, 20, 20, b"\x11" * 4),
        (b"\x00" * 4, b"\x11" * 4, 10, 20, b"\x11" * 4),
    ],
)
def test_get_fork_version(
    previous_version, current_version, epoch, current_epoch, expected
):
    fork = Fork.create(
        previous_version=previous_version, current_version=current_version, epoch=epoch
    )
    assert expected == _get_fork_version(fork, current_epoch)


@pytest.mark.parametrize(
    (
        "previous_version,"
        "current_version,"
        "epoch,"
        "current_epoch,"
        "signature_domain,"
        "expected"
    ),
    [
        (b"\x11" * 4, b"\x22" * 4, 4, 4, 1, b"\x01\x00\x00\x00" + b"\x22" * 4),
        (b"\x11" * 4, b"\x22" * 4, 4, 4 - 1, 1, b"\x01\x00\x00\x00" + b"\x11" * 4),
    ],
)
def test_get_domain(
    previous_version,
    current_version,
    epoch,
    current_epoch,
    signature_domain,
    genesis_state,
    slots_per_epoch,
    expected,
):
    state = genesis_state
    fork = Fork.create(
        previous_version=previous_version, current_version=current_version, epoch=epoch
    )
    assert expected == get_domain(
        state=state.set("fork", fork),
        signature_domain=signature_domain,
        slots_per_epoch=slots_per_epoch,
        message_epoch=current_epoch,
    )


def test_get_seed(
    genesis_state,
    committee_config,
    slots_per_epoch,
    min_seed_lookahead,
    max_seed_lookahead,
    epochs_per_historical_vector,
):
    def mock_get_randao_mix(state, epoch, epochs_per_historical_vector):
        return hash_eth2(
            state.hash_tree_root
            + epoch.to_bytes(32, byteorder="little")
            + epochs_per_historical_vector.to_bytes(32, byteorder="little")
        )

    state = genesis_state
    epoch = 1
    state = state.set(
        "slot", compute_start_slot_at_epoch(epoch, committee_config.SLOTS_PER_EPOCH)
    )

    epoch_as_bytes = epoch.to_bytes(32, "little")
    domain_type = signature_domain_to_domain_type(
        SignatureDomain.DOMAIN_BEACON_PROPOSER
    )
    seed = _get_seed(
        state=state,
        epoch=epoch,
        domain_type=domain_type,
        randao_provider=mock_get_randao_mix,
        epoch_provider=lambda *_: epoch_as_bytes,
        committee_config=committee_config,
    )
    assert seed == hash_eth2(
        domain_type
        + epoch_as_bytes
        + mock_get_randao_mix(
            state=state,
            epoch=(epoch + epochs_per_historical_vector - min_seed_lookahead - 1),
            epochs_per_historical_vector=epochs_per_historical_vector,
        )
    )
