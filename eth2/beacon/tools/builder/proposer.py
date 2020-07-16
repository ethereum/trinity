from typing import Sequence

from eth_typing import BLSSignature

from eth2._utils.bls import bls
from eth2.beacon.committee_helpers import get_beacon_proposer_index
from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.exceptions import ProposerIndexError
from eth2.beacon.helpers import compute_epoch_at_slot, compute_signing_root, get_domain
from eth2.beacon.signature_domain import SignatureDomain
from eth2.beacon.state_machines.abc import BaseBeaconStateMachine
from eth2.beacon.tools.builder.validator import sign_transaction
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock, BeaconBlockBody, SignedBeaconBlock
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import Root, SerializableUint64, Slot, ValidatorIndex
from eth2.configs import Eth2Config


def is_proposer(
    state: BeaconState, validator_index: ValidatorIndex, config: Eth2Config
) -> bool:
    """
    Return if the validator is proposer of `state.slot`.
    """
    return get_beacon_proposer_index(state, config) == validator_index


def generate_randao_reveal(
    privkey: int, slot: Slot, state: BeaconState, config: Eth2Config
) -> BLSSignature:
    """
    Return the RANDAO reveal for the validator represented by ``privkey``.
    The current implementation requires a validator to provide the BLS signature
    over the SSZ-serialized epoch in which they are proposing a block.
    """
    epoch = compute_epoch_at_slot(slot, config.SLOTS_PER_EPOCH)

    randao_reveal = sign_transaction(
        object=SerializableUint64(epoch),
        privkey=privkey,
        state=state,
        slot=slot,
        signature_domain=SignatureDomain.DOMAIN_RANDAO,
        slots_per_epoch=config.SLOTS_PER_EPOCH,
    )
    return randao_reveal


def validate_proposer_index(
    state: BeaconState, config: Eth2Config, slot: Slot, validator_index: ValidatorIndex
) -> None:
    beacon_proposer_index = get_beacon_proposer_index(state.copy(slot=slot), config)

    if validator_index != beacon_proposer_index:
        raise ProposerIndexError


def create_block_proposal(
    slot: Slot,
    parent_root: Root,
    randao_reveal: BLSSignature,
    eth1_data: Eth1Data,
    attestations: Sequence[Attestation],
    state: BeaconState,
    state_machine: BaseBeaconStateMachine,
) -> BeaconBlock:
    config = state_machine.config
    state_at_slot, _ = state_machine.apply_state_transition(state, future_slot=slot)
    proposer_index = get_beacon_proposer_index(state_at_slot, config)

    block_body = BeaconBlockBody.create(
        randao_reveal=randao_reveal, eth1_data=eth1_data, attestations=attestations
    )
    proposal = BeaconBlock.create(
        slot=slot,
        parent_root=parent_root,
        body=block_body,
        proposer_index=proposer_index,
    )
    block_with_empty_signature = SignedBeaconBlock.create(
        message=proposal, signature=EMPTY_SIGNATURE
    )
    post_state, block_with_state_root = state_machine.apply_state_transition(
        state, block_with_empty_signature, check_proposer_signature=False
    )
    return block_with_state_root.message


def get_block_signature(
    state: BeaconState, block: BeaconBlock, private_key: int, slots_per_epoch: int
) -> BLSSignature:
    epoch = compute_epoch_at_slot(block.slot, slots_per_epoch)
    domain = get_domain(
        state,
        SignatureDomain.DOMAIN_BEACON_PROPOSER,
        slots_per_epoch,
        message_epoch=epoch,
    )
    signing_root = compute_signing_root(block, domain)
    return bls.sign(private_key, signing_root)


def sign_block(
    state: BeaconState, block: BeaconBlock, private_key: int, slots_per_epoch: int
) -> SignedBeaconBlock:
    signature = get_block_signature(state, block, private_key, slots_per_epoch)
    return SignedBeaconBlock.create(message=block, signature=signature)


def create_block(
    slot: Slot,
    parent_root: Root,
    randao_reveal: BLSSignature,
    eth1_data: Eth1Data,
    attestations: Sequence[Attestation],
    state: BeaconState,
    state_machine: BaseBeaconStateMachine,
    private_key: int,
) -> SignedBeaconBlock:
    block = create_block_proposal(
        slot, parent_root, randao_reveal, eth1_data, attestations, state, state_machine
    )
    slots_per_epoch = state_machine.config.SLOTS_PER_EPOCH
    return sign_block(state, block, private_key, slots_per_epoch)
