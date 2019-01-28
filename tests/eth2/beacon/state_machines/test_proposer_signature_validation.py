import pytest

from eth_utils import (
    ValidationError,
)

from eth2._utils import bls

from eth2.beacon.constants import (
    GWEI_PER_ETH,
)
from eth2.beacon.enums import (
    SignatureDomain,
)
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.types.proposal_signed_data import (
    ProposalSignedData,
)
from eth2.beacon.types.states import BeaconState

from eth2.beacon.state_machines.forks.serenity.validation import (
    validate_proposer_signature,
)

from tests.eth2.beacon.helpers import mock_validator_record


@pytest.mark.parametrize(
    'epoch_length, shard_count,'
    'proposer_privkey, proposer_pubkey, is_valid_signature',
    (
        (5, 2, 0, bls.privtopub(0), True, ),
        (5, 2, 0, bls.privtopub(0)[1:] + b'\x01', False),
        (5, 2, 0, b'\x01\x23', False),
        (5, 2, 123, bls.privtopub(123), True),
        (5, 2, 123, bls.privtopub(123)[1:] + b'\x01', False),
        (5, 2, 123, b'\x01\x23', False),
    )
)
def test_validate_proposer_signature(
        proposer_privkey,
        proposer_pubkey,
        is_valid_signature,
        sample_beacon_block_params,
        sample_beacon_state_params,
        beacon_chain_shard_number,
        epoch_length,
        max_deposit,
        target_committee_size,
        shard_count):

    state = BeaconState(**sample_beacon_state_params).copy(
        validator_registry=tuple(
            mock_validator_record(proposer_pubkey)
            for _ in range(10)
        ),
        validator_balances=(max_deposit * GWEI_PER_ETH,) * 10,
    )

    default_block = BeaconBlock(**sample_beacon_block_params)
    empty_signature_block_root = default_block.block_without_signature_root

    proposal_root = ProposalSignedData(
        state.slot,
        beacon_chain_shard_number,
        empty_signature_block_root,
    ).root

    proposed_block = BeaconBlock(**sample_beacon_block_params).copy(
        signature=bls.sign(
            message=proposal_root,
            privkey=proposer_privkey,
            domain=SignatureDomain.DOMAIN_PROPOSAL,
        ),
    )

    if is_valid_signature:
        validate_proposer_signature(
            state,
            proposed_block,
            beacon_chain_shard_number,
            epoch_length,
            target_committee_size,
            shard_count,
        )
    else:
        with pytest.raises(ValidationError):
            validate_proposer_signature(
                state,
                proposed_block,
                beacon_chain_shard_number,
                epoch_length,
                target_committee_size,
                shard_count
            )
