from eth2.beacon.enums import (
    SignatureDomain,
)
from eth2.beacon.state_machines.forks.serenity.block_validation import (
    validate_proposer_slashing,
)
from eth2.beacon.tools.builder.validator import (
    sign_transaction,
)
from eth2.beacon.types.proposal_signed_data import ProposalSignedData
from eth2.beacon.types.proposer_slashings import ProposerSlashing


def test_validate_proposer_slashing_valid(epoch_length,
                                          genesis_state,
                                          beacon_chain_shard_number,
                                          keymap):
    state = genesis_state
    proposer_index = 0

    block_root_1 = b'\x11' * 32
    proposal_data_1 = ProposalSignedData(
        state.slot,
        beacon_chain_shard_number,
        block_root_1,
    )
    proposal_signature_1 = sign_transaction(
        message=proposal_data_1.root,
        privkey=keymap[state.validator_registry[proposer_index].pubkey],
        fork=state.fork,
        slot=proposal_data_1.slot,
        signature_domain=SignatureDomain.DOMAIN_PROPOSAL,
        epoch_length=epoch_length,
    )

    block_root_2 = b'\x22' * 32
    proposal_data_2 = ProposalSignedData(
        state.slot,
        beacon_chain_shard_number,
        block_root_2,
    )
    proposal_signature_2 = sign_transaction(
        message=proposal_data_2.root,
        privkey=keymap[state.validator_registry[proposer_index].pubkey],
        fork=state.fork,
        slot=proposal_data_2.slot,
        signature_domain=SignatureDomain.DOMAIN_PROPOSAL,
        epoch_length=epoch_length,
    )

    proposer_slashing = ProposerSlashing(
        proposer_index=proposer_index,
        proposal_data_1=proposal_data_1,
        proposal_data_2=proposal_data_2,
        proposal_signature_1=proposal_signature_1,
        proposal_signature_2=proposal_signature_2,
    )

    validate_proposer_slashing(state, proposer_slashing, epoch_length)
