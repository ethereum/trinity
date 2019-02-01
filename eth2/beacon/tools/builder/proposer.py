from typing import (
    Dict,
    Sequence,
    Type,
)

from eth.constants import (
    ZERO_HASH32,
)

from eth2._utils import bls
from eth2.beacon.enums import (
    SignatureDomain,
)
from eth2.beacon.exceptions import (
    ProposerIndexError,
)
from eth2.beacon.helpers import (
    get_beacon_proposer_index,
    get_domain,
)

from eth2.beacon.state_machines.base import (
    BaseBeaconStateMachine,
)
from eth2.beacon.state_machines.configs import BeaconConfig

from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
    BeaconBlockBody,
)
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.types.proposal_signed_data import ProposalSignedData
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import (
    BLSPubkey,
    FromBlockParams,
    SlotNumber,
)


def create_block_on_state(
        state: BeaconState,
        config: BeaconConfig,
        state_machine: BaseBeaconStateMachine,
        block_class: BaseBeaconBlock,
        parent_block: BaseBeaconBlock,
        slot: SlotNumber,
        validator_index: int,
        privkey: int,
        attestations: Sequence[Attestation]) -> BaseBeaconBlock:
    """
    Create a beacon block with the given parameters.
    """
    # Check proposer
    beacon_proposer_index = get_beacon_proposer_index(
        state.copy(
            slot=slot,
        ),
        slot,
        config.EPOCH_LENGTH,
        config.TARGET_COMMITTEE_SIZE,
        config.SHARD_COUNT,
    )

    if validator_index != beacon_proposer_index:
        raise ProposerIndexError

    # Prepare block: slot and parent_root
    block = block_class.from_parent(
        parent_block=parent_block,
        block_params=FromBlockParams(slot=slot),
    )

    # TODO: Add more operations
    randao_reveal = ZERO_HASH32
    eth1_data = Eth1Data.create_empty_data()
    body = BeaconBlockBody.create_empty_body().copy(
        attestations=attestations,
    )

    block = block.copy(
        randao_reveal=randao_reveal,
        eth1_data=eth1_data,
        body=body,
    )

    # Apply state transition to get state root
    state, block = state_machine.import_block(block, check_proposer_signature=True)

    # Sign
    empty_signature_block_root = block.block_without_signature_root
    proposal_root = ProposalSignedData(
        slot,
        config.BEACON_CHAIN_SHARD_NUMBER,
        empty_signature_block_root,
    ).root
    domain = get_domain(
        state.fork,
        slot,
        SignatureDomain.DOMAIN_PROPOSAL,
    )
    block = block.copy(
        signature=bls.sign(
            message=proposal_root,
            privkey=privkey,
            domain=domain,
        ),
    )

    return block


def create_mock_block(state: BeaconState,
                      config: BeaconConfig,
                      state_machine: BaseBeaconStateMachine,
                      block_class: Type[BaseBeaconBlock],
                      parent_block: BaseBeaconBlock,
                      keymap: Dict[BLSPubkey, int],
                      slot: SlotNumber=None,
                      attestations: Sequence[Attestation]=()) -> BaseBeaconBlock:
    """
    Create a mocking block with the given block parameters and ``keymap``.

    Note that it doesn't return the correct ``state_root``.
    """
    proposer_index = get_beacon_proposer_index(
        state.copy(
            slot=slot,
        ),
        slot,
        config.EPOCH_LENGTH,
        config.TARGET_COMMITTEE_SIZE,
        config.SHARD_COUNT,
    )
    proposer_pubkey = state.validator_registry[proposer_index].pubkey
    proposer_privkey = keymap[proposer_pubkey]

    result_block = create_block_on_state(
        state,
        config,
        state_machine,
        block_class,
        parent_block,
        slot,
        validator_index=proposer_index,
        privkey=proposer_privkey,
        attestations=attestations,
    )

    return result_block
