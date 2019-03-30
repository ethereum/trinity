import pytest

from eth.constants import (
    ZERO_HASH32,
)
from eth_utils import (
    to_tuple,
)

from py_ecc import bls
from eth2.configs import (
    Eth2Config,
    CommitteeConfig,
)
from eth2.beacon.constants import (
    EMPTY_SIGNATURE,
    FAR_FUTURE_EPOCH,
)
from eth2.beacon.helpers import (
    slot_to_epoch,
)
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.crosslink_records import CrosslinkRecord
from eth2.beacon.types.deposit_data import DepositData
from eth2.beacon.types.deposit_input import DepositInput
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.types.proposal import Proposal
from eth2.beacon.types.slashable_attestations import SlashableAttestation
from eth2.beacon.types.states import BeaconState

from eth2.beacon.on_genesis import (
    get_genesis_block,
)
from eth2.beacon.types.blocks import (
    BeaconBlockBody,
)
from eth2.beacon.types.forks import (
    Fork,
)
from eth2.beacon.state_machines.forks.serenity import (
    SerenityStateMachine,
)
from eth2.beacon.state_machines.forks.serenity.blocks import (
    SerenityBeaconBlock,
)
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG

from tests.eth2.beacon.helpers import (
    mock_validator_record,
)

DEFAULT_SHUFFLING_SEED = b'\00' * 32
DEFAULT_RANDAO = b'\45' * 32
DEFAULT_NUM_VALIDATORS = 40


@pytest.fixture(scope="session")
def privkeys():
    """
    Rationales:
    1. Making the privkeys be small integers to make multiplying easier for tests.
    2. Using ``2**i`` instead of ``i``:
        If using ``i``, the combinations of privkeys would not lead to unique pubkeys.
    """
    return [2 ** i for i in range(100)]


@pytest.fixture(scope="session")
def keymap(privkeys):
    keymap = {}
    for i, k in enumerate(privkeys):
        keymap[bls.privtopub(k)] = k
        if i % 50 == 0:
            print("Generated %d keys" % i)
    return keymap


@pytest.fixture(scope="session")
def pubkeys(keymap):
    return list(keymap)


@pytest.fixture
def sample_proposer_slashing_params(sample_proposal_params):
    proposal_data = Proposal(**sample_proposal_params)
    return {
        'proposer_index': 1,
        'proposal_1': proposal_data,
        'proposal_2': proposal_data,
    }


@pytest.fixture
def sample_attestation_params(sample_attestation_data_params):
    return {
        'aggregation_bitfield': b'\12' * 16,
        'data': AttestationData(**sample_attestation_data_params),
        'custody_bitfield': b'\34' * 16,
        'aggregate_signature': [0, 0],
    }


@pytest.fixture
def sample_attestation_data_params(sample_crosslink_record_params):
    return {
        'slot': 10,
        'shard': 12,
        'beacon_block_root': b'\x11' * 32,
        'epoch_boundary_root': b'\x22' * 32,
        'crosslink_data_root': b'\x33' * 32,
        'latest_crosslink': CrosslinkRecord(**sample_crosslink_record_params),
        'justified_epoch': 0,
        'justified_block_root': b'\x55' * 32,
    }


@pytest.fixture
def sample_attestation_data_and_custody_bit_params(sample_attestation_data_params):
    return {
        'data': AttestationData(**sample_attestation_data_params),
        'custody_bit': False,
    }


@pytest.fixture
def sample_beacon_block_body_params():
    return {
        'proposer_slashings': (),
        'attester_slashings': (),
        'attestations': (),
        'deposits': (),
        'voluntary_exits': (),
        'transfers': (),
    }


@pytest.fixture
def sample_beacon_block_params(sample_beacon_block_body_params,
                               sample_eth1_data_params,
                               genesis_slot):
    return {
        'slot': genesis_slot + 10,
        'parent_root': ZERO_HASH32,
        'state_root': b'\x55' * 32,
        'randao_reveal': EMPTY_SIGNATURE,
        'eth1_data': Eth1Data(**sample_eth1_data_params),
        'signature': EMPTY_SIGNATURE,
        'body': BeaconBlockBody(**sample_beacon_block_body_params)
    }


@pytest.fixture
def sample_beacon_state_params(genesis_slot,
                               genesis_epoch,
                               sample_fork_params,
                               sample_eth1_data_params):
    return {
        'slot': genesis_slot + 100,
        'genesis_time': 0,
        'fork': Fork(**sample_fork_params),
        'validator_registry': (),
        'validator_balances': (),
        'validator_registry_update_epoch': 0,
        'latest_randao_mixes': (),
        'previous_shuffling_start_shard': 1,
        'current_shuffling_start_shard': 2,
        'previous_shuffling_epoch': genesis_epoch,
        'current_shuffling_epoch': genesis_epoch,
        'previous_shuffling_seed': b'\x77' * 32,
        'current_shuffling_seed': b'\x88' * 32,
        'previous_epoch_attestations': (),
        'current_epoch_attestations': (),
        'previous_justified_epoch': 0,
        'justified_epoch': 0,
        'justification_bitfield': 0,
        'finalized_epoch': 0,
        'latest_crosslinks': (),
        'latest_block_roots': (),
        'latest_state_roots': (),
        'latest_active_index_roots': (),
        'latest_slashed_balances': (),
        'historical_roots': (),
        'latest_eth1_data': Eth1Data(**sample_eth1_data_params),
        'eth1_data_votes': (),
        'deposit_index': 0,
    }


@pytest.fixture
def sample_eth1_data_params():
    return {
        'deposit_root': b'\x43' * 32,
        'block_hash': b'\x46' * 32,
    }


@pytest.fixture
def sample_eth1_data_vote_params(sample_eth1_data_params):
    return {
        'eth1_data': Eth1Data(**sample_eth1_data_params),
        'vote_count': 10,
    }


@pytest.fixture
def sample_crosslink_record_params():
    return {
        'epoch': 0,
        'crosslink_data_root': b'\x43' * 32,
    }


@pytest.fixture
def sample_deposit_input_params():
    return {
        'pubkey': 123,
        'withdrawal_credentials': b'\11' * 32,
        'proof_of_possession': (0, 0),
    }


@pytest.fixture
def sample_deposit_data_params(sample_deposit_input_params):
    return {
        'deposit_input': DepositInput(**sample_deposit_input_params),
        'amount': 56,
        'timestamp': 1501851927,
    }


@pytest.fixture
def sample_deposit_params(sample_deposit_data_params):
    return {
        'proof': (),
        'index': 5,
        'deposit_data': DepositData(**sample_deposit_data_params)
    }


@pytest.fixture
def sample_voluntary_exit_params():
    return {
        'epoch': 123,
        'validator_index': 15,
        'signature': EMPTY_SIGNATURE,
    }


@pytest.fixture
def sample_fork_params():
    return {
        'previous_version': (0).to_bytes(4, 'little'),
        'current_version': (0).to_bytes(4, 'little'),
        'epoch': 2**32,
    }


@pytest.fixture
def sample_pending_attestation_record_params(sample_attestation_data_params):
    return {
        'data': AttestationData(**sample_attestation_data_params),
        'aggregation_bitfield': b'\12' * 16,
        'custody_bitfield': b'\34' * 16,
        'slot_included': 0,
    }


@pytest.fixture
def sample_proposal_params():
    return {
        'slot': 10,
        'shard': 12,
        'block_root': b'\x43' * 32,
        'signature': b'\x56' * 96,
    }


@pytest.fixture
def sample_recent_proposer_record_params():
    return {
        'index': 10,
        'randao_commitment': b'\x43' * 32,
        'balance_delta': 3
    }


@pytest.fixture
def sample_slashable_attestation_params(sample_attestation_data_params):
    return {
        'validator_indices': (10, 11, 12, 15, 28),
        'data': AttestationData(**sample_attestation_data_params),
        'custody_bitfield': b'\00' * 4,
        'aggregate_signature': EMPTY_SIGNATURE,
    }


@pytest.fixture
def sample_transfer_params():
    return {
        'sender': 10,
        'recipient': 12,
        'amount': 10 * 10**9,
        'fee': 5 * 10**9,
        'slot': 5,
        'pubkey': b'\x67' * 48,
        'signature': b'\x43' * 96,
    }


@pytest.fixture
def sample_attester_slashing_params(sample_slashable_attestation_params):
    slashable_attestation = SlashableAttestation(**sample_slashable_attestation_params)
    return {
        'slashable_attestation_1': slashable_attestation,
        'slashable_attestation_2': slashable_attestation,
    }


@pytest.fixture
def sample_validator_record_params():
    return {
        'pubkey': 123,
        'withdrawal_credentials': b'\x01' * 32,
        'activation_epoch': FAR_FUTURE_EPOCH,
        'exit_epoch': FAR_FUTURE_EPOCH,
        'withdrawable_epoch': FAR_FUTURE_EPOCH,
        'initiated_exit': False,
        'slashed': False,
    }


@pytest.fixture()
def sample_block(sample_beacon_block_params):
    return SerenityBeaconBlock(**sample_beacon_block_params)


@pytest.fixture()
def sample_state(sample_beacon_state_params):
    return BeaconState(**sample_beacon_state_params)


@pytest.fixture
def filled_beacon_state(genesis_epoch,
                        genesis_slot,
                        genesis_start_shard,
                        shard_count,
                        slots_per_historical_root,
                        latest_active_index_roots_length,
                        latest_randao_mixes_length,
                        latest_slashed_exit_length):
    return BeaconState.create_filled_state(
        genesis_epoch=genesis_epoch,
        genesis_start_shard=genesis_start_shard,
        genesis_slot=genesis_slot,
        shard_count=shard_count,
        slots_per_historical_root=slots_per_historical_root,
        latest_active_index_roots_length=latest_active_index_roots_length,
        latest_randao_mixes_length=latest_randao_mixes_length,
        latest_slashed_exit_length=latest_slashed_exit_length,
    )


@pytest.fixture()
def n():
    return 10


@pytest.fixture()
def n_validators_state(filled_beacon_state, max_deposit_amount, n, config):
    validator_count = n
    return filled_beacon_state.copy(
        validator_registry=tuple(
            mock_validator_record(
                pubkey=index.to_bytes(48, "little"),
                config=config,
                is_active=True,
            )
            for index in range(validator_count)
        ),
        validator_balances=(max_deposit_amount,) * validator_count,
    )


#
# Temporary default values
#
@pytest.fixture
def init_shuffling_seed():
    return DEFAULT_SHUFFLING_SEED


@pytest.fixture
def init_randao():
    return DEFAULT_RANDAO


@pytest.fixture
def num_validators():
    return DEFAULT_NUM_VALIDATORS


@pytest.fixture
def init_validator_privkeys(privkeys, num_validators):
    return privkeys[:num_validators]


@pytest.fixture
def init_validator_pubkeys(pubkeys, num_validators):
    return pubkeys[:num_validators]


#
# config
#
@pytest.fixture
def shard_count():
    return SERENITY_CONFIG.SHARD_COUNT


@pytest.fixture
def target_committee_size():
    return SERENITY_CONFIG.TARGET_COMMITTEE_SIZE


@pytest.fixture
def ejection_balance():
    return SERENITY_CONFIG.EJECTION_BALANCE


@pytest.fixture
def max_balance_churn_quotient():
    return SERENITY_CONFIG.MAX_BALANCE_CHURN_QUOTIENT


@pytest.fixture
def beacon_chain_shard_number():
    return SERENITY_CONFIG.BEACON_CHAIN_SHARD_NUMBER


@pytest.fixture
def max_indices_per_slashable_vote():
    return SERENITY_CONFIG.MAX_INDICES_PER_SLASHABLE_VOTE


@pytest.fixture
def max_exit_dequeues_per_epoch():
    return SERENITY_CONFIG.MAX_EXIT_DEQUEUES_PER_EPOCH


@pytest.fixture
def shuffle_round_count():
    return SERENITY_CONFIG.SHUFFLE_ROUND_COUNT


@pytest.fixture
def slots_per_historical_root():
    return SERENITY_CONFIG.SLOTS_PER_HISTORICAL_ROOT


@pytest.fixture
def latest_active_index_roots_length():
    return SERENITY_CONFIG.LATEST_ACTIVE_INDEX_ROOTS_LENGTH


@pytest.fixture
def latest_randao_mixes_length():
    return SERENITY_CONFIG.LATEST_RANDAO_MIXES_LENGTH


@pytest.fixture
def latest_slashed_exit_length():
    return SERENITY_CONFIG.LATEST_SLASHED_EXIT_LENGTH


@pytest.fixture
def deposit_contract_address():
    return SERENITY_CONFIG.DEPOSIT_CONTRACT_ADDRESS


@pytest.fixture
def deposit_contract_tree_depth():
    return SERENITY_CONFIG.DEPOSIT_CONTRACT_TREE_DEPTH


@pytest.fixture
def min_deposit_amount():
    return SERENITY_CONFIG.MIN_DEPOSIT_AMOUNT


@pytest.fixture
def max_deposit_amount():
    return SERENITY_CONFIG.MAX_DEPOSIT_AMOUNT


@pytest.fixture
def genesis_fork_version():
    return SERENITY_CONFIG.GENESIS_FORK_VERSION


@pytest.fixture
def genesis_slot():
    return SERENITY_CONFIG.GENESIS_SLOT


@pytest.fixture
def genesis_epoch(genesis_slot, slots_per_epoch):
    return slot_to_epoch(genesis_slot, slots_per_epoch)


@pytest.fixture
def genesis_start_shard():
    return SERENITY_CONFIG.GENESIS_START_SHARD


@pytest.fixture
def bls_withdrawal_prefix_byte():
    return SERENITY_CONFIG.BLS_WITHDRAWAL_PREFIX_BYTE


@pytest.fixture
def seconds_per_slot():
    return SERENITY_CONFIG.SECONDS_PER_SLOT


@pytest.fixture
def min_attestation_inclusion_delay():
    return SERENITY_CONFIG.MIN_ATTESTATION_INCLUSION_DELAY


@pytest.fixture
def slots_per_epoch():
    return SERENITY_CONFIG.SLOTS_PER_EPOCH


@pytest.fixture
def min_seed_lookahead():
    return SERENITY_CONFIG.MIN_SEED_LOOKAHEAD


@pytest.fixture
def activation_exit_delay():
    return SERENITY_CONFIG.ACTIVATION_EXIT_DELAY


@pytest.fixture
def epochs_per_eth1_voting_period():
    return SERENITY_CONFIG.EPOCHS_PER_ETH1_VOTING_PERIOD


@pytest.fixture
def min_validator_withdrawability_delay():
    return SERENITY_CONFIG.MIN_VALIDATOR_WITHDRAWABILITY_DELAY


@pytest.fixture
def persistent_committee_period():
    return SERENITY_CONFIG.PERSISTENT_COMMITTEE_PERIOD


@pytest.fixture
def base_reward_quotient():
    return SERENITY_CONFIG.BASE_REWARD_QUOTIENT


@pytest.fixture
def whistleblower_reward_quotient():
    return SERENITY_CONFIG.WHISTLEBLOWER_REWARD_QUOTIENT


@pytest.fixture
def attestation_inclusion_reward_quotient():
    return SERENITY_CONFIG.ATTESTATION_INCLUSION_REWARD_QUOTIENT


@pytest.fixture
def inactivity_penalty_quotient():
    return SERENITY_CONFIG.INACTIVITY_PENALTY_QUOTIENT


@pytest.fixture
def min_penalty_quotient():
    return SERENITY_CONFIG.MIN_PENALTY_QUOTIENT


@pytest.fixture
def max_proposer_slashings():
    return SERENITY_CONFIG.MAX_PROPOSER_SLASHINGS


@pytest.fixture
def max_attester_slashings():
    return SERENITY_CONFIG.MAX_ATTESTER_SLASHINGS


@pytest.fixture
def max_attestations():
    return SERENITY_CONFIG.MAX_ATTESTATIONS


@pytest.fixture
def max_deposits():
    return SERENITY_CONFIG.MAX_DEPOSITS


@pytest.fixture
def max_voluntary_exits():
    return SERENITY_CONFIG.MAX_VOLUNTARY_EXITS


@pytest.fixture
def max_transfers():
    return SERENITY_CONFIG.MAX_TRANSFERS


#
# genesis
#
@pytest.fixture
def genesis_state(filled_beacon_state,
                  activated_genesis_validators,
                  genesis_balances,
                  slots_per_epoch,
                  target_committee_size,
                  genesis_epoch,
                  shard_count,
                  slots_per_historical_root,
                  latest_slashed_exit_length,
                  latest_randao_mixes_length):
    return filled_beacon_state.copy(
        validator_registry=activated_genesis_validators,
        validator_balances=genesis_balances,
        latest_block_roots=tuple(ZERO_HASH32 for _ in range(slots_per_historical_root)),
        latest_slashed_balances=(0,) * latest_slashed_exit_length,
        latest_crosslinks=tuple(
            CrosslinkRecord(
                epoch=genesis_epoch,
                crosslink_data_root=ZERO_HASH32,
            )
            for _ in range(shard_count)
        ),
        latest_randao_mixes=tuple(
            ZERO_HASH32
            for _ in range(latest_randao_mixes_length)
        ),
    )


@pytest.fixture
def genesis_block(genesis_state, genesis_slot):
    return get_genesis_block(
        genesis_state.root,
        genesis_slot,
        SerenityBeaconBlock,
    )


@pytest.fixture
def genesis_validators(init_validator_pubkeys,
                       init_randao,
                       max_deposit_amount,
                       config):
    """
    Inactive
    """
    return tuple(
        mock_validator_record(
            pubkey=pubkey,
            config=config,
            withdrawal_credentials=ZERO_HASH32,
            is_active=False,
        )
        for pubkey in init_validator_pubkeys
    )


@to_tuple
@pytest.fixture
def activated_genesis_validators(genesis_validators, genesis_epoch):
    """
    Active
    """
    for validator in genesis_validators:
        yield validator.copy(activation_epoch=genesis_epoch)


@pytest.fixture
def genesis_balances(init_validator_pubkeys, max_deposit_amount):
    return tuple(
        max_deposit_amount
        for _ in init_validator_pubkeys
    )


#
# StateMachine
#

@pytest.fixture
def config(
        shard_count,
        target_committee_size,
        ejection_balance,
        max_balance_churn_quotient,
        beacon_chain_shard_number,
        max_indices_per_slashable_vote,
        max_exit_dequeues_per_epoch,
        shuffle_round_count,
        slots_per_historical_root,
        latest_active_index_roots_length,
        latest_randao_mixes_length,
        latest_slashed_exit_length,
        deposit_contract_address,
        deposit_contract_tree_depth,
        min_deposit_amount,
        max_deposit_amount,
        genesis_fork_version,
        genesis_slot,
        genesis_epoch,
        genesis_start_shard,
        bls_withdrawal_prefix_byte,
        seconds_per_slot,
        min_attestation_inclusion_delay,
        slots_per_epoch,
        min_seed_lookahead,
        activation_exit_delay,
        epochs_per_eth1_voting_period,
        min_validator_withdrawability_delay,
        persistent_committee_period,
        base_reward_quotient,
        whistleblower_reward_quotient,
        attestation_inclusion_reward_quotient,
        inactivity_penalty_quotient,
        min_penalty_quotient,
        max_proposer_slashings,
        max_attester_slashings,
        max_attestations,
        max_deposits,
        max_voluntary_exits,
        max_transfers
):
    return Eth2Config(
        SHARD_COUNT=shard_count,
        TARGET_COMMITTEE_SIZE=target_committee_size,
        EJECTION_BALANCE=ejection_balance,
        MAX_BALANCE_CHURN_QUOTIENT=max_balance_churn_quotient,
        BEACON_CHAIN_SHARD_NUMBER=beacon_chain_shard_number,
        MAX_INDICES_PER_SLASHABLE_VOTE=max_indices_per_slashable_vote,
        MAX_EXIT_DEQUEUES_PER_EPOCH=max_exit_dequeues_per_epoch,
        SHUFFLE_ROUND_COUNT=shuffle_round_count,
        SLOTS_PER_HISTORICAL_ROOT=slots_per_historical_root,
        LATEST_ACTIVE_INDEX_ROOTS_LENGTH=latest_active_index_roots_length,
        LATEST_RANDAO_MIXES_LENGTH=latest_randao_mixes_length,
        LATEST_SLASHED_EXIT_LENGTH=latest_slashed_exit_length,
        DEPOSIT_CONTRACT_ADDRESS=deposit_contract_address,
        DEPOSIT_CONTRACT_TREE_DEPTH=deposit_contract_tree_depth,
        MIN_DEPOSIT_AMOUNT=min_deposit_amount,
        MAX_DEPOSIT_AMOUNT=max_deposit_amount,
        GENESIS_FORK_VERSION=genesis_fork_version,
        GENESIS_SLOT=genesis_slot,
        GENESIS_EPOCH=genesis_epoch,
        GENESIS_START_SHARD=genesis_start_shard,
        BLS_WITHDRAWAL_PREFIX_BYTE=bls_withdrawal_prefix_byte,
        SECONDS_PER_SLOT=seconds_per_slot,
        MIN_ATTESTATION_INCLUSION_DELAY=min_attestation_inclusion_delay,
        SLOTS_PER_EPOCH=slots_per_epoch,
        MIN_SEED_LOOKAHEAD=min_seed_lookahead,
        ACTIVATION_EXIT_DELAY=activation_exit_delay,
        EPOCHS_PER_ETH1_VOTING_PERIOD=epochs_per_eth1_voting_period,
        MIN_VALIDATOR_WITHDRAWABILITY_DELAY=min_validator_withdrawability_delay,
        PERSISTENT_COMMITTEE_PERIOD=persistent_committee_period,
        BASE_REWARD_QUOTIENT=base_reward_quotient,
        WHISTLEBLOWER_REWARD_QUOTIENT=whistleblower_reward_quotient,
        ATTESTATION_INCLUSION_REWARD_QUOTIENT=attestation_inclusion_reward_quotient,
        INACTIVITY_PENALTY_QUOTIENT=inactivity_penalty_quotient,
        MIN_PENALTY_QUOTIENT=min_penalty_quotient,
        MAX_PROPOSER_SLASHINGS=max_proposer_slashings,
        MAX_ATTESTER_SLASHINGS=max_attester_slashings,
        MAX_ATTESTATIONS=max_attestations,
        MAX_DEPOSITS=max_deposits,
        MAX_VOLUNTARY_EXITS=max_voluntary_exits,
        MAX_TRANSFERS=max_transfers,
    )


#
# State machine
#
@pytest.fixture
def fixture_sm_class(config):
    return SerenityStateMachine.configure(
        __name__='SerenityStateMachineForTesting',
        config=config,
    )


#
# CommitteeConfig
#
@pytest.fixture
def committee_config(config):
    return CommitteeConfig(config)
