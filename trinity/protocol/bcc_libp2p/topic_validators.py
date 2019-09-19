import logging
from typing import (
    Callable,
)

import ssz

from eth_utils import (
    ValidationError,
    encode_hex,
)

from eth.exceptions import BlockNotFound

from eth2.beacon.types.attestations import Attestation
from eth2.beacon.attestation_helpers import get_attestation_data_slot
from eth2.beacon.exceptions import SignatureError
from eth2.beacon.chains.base import BaseBeaconChain
from eth2.beacon.helpers import compute_start_slot_of_epoch
from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.state_machines.forks.serenity.block_processing import process_block_header
from eth2.beacon.state_machines.forks.serenity.block_validation import validate_attestation
from eth2.beacon.typing import Slot

from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2

from trinity._utils.shellart import bold_red

logger = logging.getLogger('trinity.components.eth2.beacon.TopicValidator')


# Find the state of the block where `target_block_slot > block.slot`
def get_ancestor_state(chain: BaseBeaconChain,
                       block: BeaconBlock,
                       latest_finalized_slot: Slot,
                       target_block_slot: Slot) -> BeaconState:
    block = chain.get_block_by_root(block.parent_root)
    while target_block_slot <= block.slot:
        block = chain.get_block_by_root(block.parent_root)
        if block.slot < latest_finalized_slot:
            raise ValueError()
    return chain.get_state_by_slot(block.slot)


def validate_block_signature(chain: BaseBeaconChain,
                             head_block: BeaconBlock,
                             head_state: BeaconState,
                             block: BeaconBlock) -> None:
    state_machine = chain.get_state_machine(max(block.slot - 1, 0))
    latest_finalized_slot = compute_start_slot_of_epoch(
        head_state.finalized_checkpoint.epoch,
        state_machine.config.SLOTS_PER_EPOCH,
    )
    if block.slot < latest_finalized_slot:
        raise ValidationError("block is older than latest finalized slot")

    # Find the proper state to verify the proposer's signature of the block
    # head state slot >= target block slot, try head block
    if head_state.slot >= block.slot:
        # head block slot >= target block slot, try ancestor blocks
        if head_block.slot >= block.slot:
            try:
                state = get_ancestor_state(chain, head_block, latest_finalized_slot, block.slot)
            except ValueError:
                raise ValidationError(
                    "This should not be possible as we already check"
                    "that `block.slot < latest_finalized_slot`"
                )
        else:
            state = chain.get_state_by_slot(head_block.slot)
    else:
        state = head_state

    state_transition = state_machine.state_transition
    state = state_transition.apply_state_transition(
        state,
        future_slot=block.slot,
    )

    process_block_header(
        state, block, state_machine.config, True
    )


def get_beacon_block_validator(chain: BaseBeaconChain) -> Callable[..., bool]:
    def beacon_block_validator(msg_forwarder: ID, msg: rpc_pb2.Message) -> bool:
        try:
            block = ssz.decode(msg.data, BeaconBlock)
        except (TypeError, ssz.DeserializationError) as error:
            logger.debug(
                bold_red("Failed to validate block=%s, error=%s"),
                encode_hex(block.signing_root),
                str(error),
            )
            return False

        head_block = chain.get_canonical_head()
        head_state = chain.get_head_state()
        try:
            validate_block_signature(
                chain,
                head_block,
                head_state,
                block,
            )
        except (ValidationError, SignatureError) as error:
            logger.debug(
                bold_red("Failed to validate block=%s, error=%s"),
                encode_hex(block.signing_root),
                str(error),
            )
            return False

        return True

    return beacon_block_validator


def get_beacon_attestation_validator(chain: BaseBeaconChain) -> Callable[..., bool]:
    def beacon_attestation_validator(msg_forwarder: ID, msg: rpc_pb2.Message) -> bool:
        try:
            attestation = ssz.decode(msg.data, sedes=Attestation)
        except (TypeError, ssz.DeserializationError) as error:
            # Not correctly encoded
            logger.debug(
                bold_red("Failed to validate attestation=%s, error=%s"),
                attestation,
                str(error),
            )
            return False

        state_machine = chain.get_state_machine()
        config = state_machine.config
        state = chain.get_head_state()

        # Check that beacon blocks attested to by the attestation are validated
        try:
            chain.get_block_by_root(attestation.data.beacon_block_root)
        except BlockNotFound:
            logger.debug(
                bold_red(
                    "Failed to validate attestation=%s, attested block=%s is not validated yet"
                ),
                attestation,
                encode_hex(attestation.data.beacon_block_root),
            )
            return False

        # Fast forward to state in future slot in order to pass
        # attestation.data.slot validity check
        attestation_data_slot = get_attestation_data_slot(
            state,
            attestation.data,
            config,
        )
        future_state = state_machine.state_transition.apply_state_transition(
            state,
            future_slot=Slot(attestation_data_slot + config.MIN_ATTESTATION_INCLUSION_DELAY),
        )
        try:
            validate_attestation(
                future_state,
                attestation,
                config,
            )
        except ValidationError as error:
            logger.debug(
                bold_red("Failed to validate attestation=%s, error=%s"),
                attestation,
                str(error),
            )
            return False

        return True
    return beacon_attestation_validator
