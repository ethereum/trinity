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

from eth2.beacon.types.aggregate_and_proof import AggregateAndProof
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.chains.base import BaseBeaconChain
from eth2.beacon.types.blocks import BaseSignedBeaconBlock, SignedBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.state_machines.base import BaseBeaconStateMachine
from eth2.beacon.state_machines.forks.serenity.block_validation import (
    validate_proposer_signature,
)
from eth2.beacon.tools.builder.aggregator import (
    validate_aggregate_and_proof,
    validate_attestation_propagation_slot_range,
    validate_attestation_signature,
)
from eth2.beacon.typing import SubnetId
from eth2.configs import CommitteeConfig

from libp2p.peer.id import ID
from libp2p.pubsub.pb import rpc_pb2

from trinity._utils.shellart import bold_red
from trinity.protocol.bcc_libp2p.configs import (
    ATTESTATION_PROPAGATION_SLOT_RANGE,
    ATTESTATION_SUBNET_COUNT,
)
from trinity.protocol.bcc_libp2p.exceptions import InvalidGossipMessage


logger = logging.getLogger('trinity.components.eth2.beacon.TopicValidator')


def get_beacon_block_validator(chain: BaseBeaconChain) -> Callable[..., bool]:
    def beacon_block_validator(msg_forwarder: ID, msg: rpc_pb2.Message) -> bool:
        try:
            block = ssz.decode(msg.data, SignedBeaconBlock)
        except (TypeError, ssz.DeserializationError) as error:
            logger.debug(
                bold_red("Failed to deserialize SignedBeaconBlock from %s, error=%s"),
                encode_hex(msg.data),
                str(error),
            )
            return False

        state_machine = chain.get_state_machine(block.slot - 1)
        state = chain.get_head_state()

        try:
            run_validate_block_proposer_signature(state, state_machine, block)
        except InvalidGossipMessage as error:
            logger.debug("%s", str(error))
            return False
        else:
            return True

    return beacon_block_validator


def get_beacon_attestation_validator(chain: BaseBeaconChain) -> Callable[..., bool]:
    # TODO:  The beacon_attestation topic is only for interop and will be removed prior to mainnet.
    def beacon_attestation_validator(msg_forwarder: ID, msg: rpc_pb2.Message) -> bool:
        try:
            attestation = ssz.decode(msg.data, sedes=Attestation)
        except (TypeError, ssz.DeserializationError) as error:
            # Not correctly encoded
            logger.debug(
                bold_red("Failed to deserialize Attestation from %s, error=%s"),
                encode_hex(msg.data),
                str(error),
            )
            return False

        state = chain.get_head_state()
        state_machine = chain.get_state_machine()

        try:
            validate_voting_beacon_block(chain, attestation)
            validate_attestation_signature(
                state,
                attestation,
                CommitteeConfig(state_machine.config),
            )
        except InvalidGossipMessage as error:
            logger.debug("%s", str(error))
            return False
        else:
            return True

    return beacon_attestation_validator


def get_committee_index_beacon_attestation_validator(
    chain: BaseBeaconChain, subnet_id: SubnetId
) -> Callable[..., bool]:
    def committee_index_beacon_attestation_validator(
        msg_forwarder: ID, msg: rpc_pb2.Message
    ) -> bool:
        try:
            attestation = ssz.decode(msg.data, sedes=Attestation)
        except (TypeError, ssz.DeserializationError) as error:
            # Not correctly encoded
            logger.debug(
                bold_red("Failed to deserialize Attestation from %s, error=%s"),
                encode_hex(msg.data),
                str(error),
            )
            return False

        state_machine = chain.get_state_machine()
        state = chain.get_head_state()

        try:
            validate_subnet_id(attestation, subnet_id)
            validate_is_unaggregated(attestation)
            validate_voting_beacon_block(chain, attestation)
            validate_attestation_propagation_slot_range(
                state,
                attestation,
                ATTESTATION_PROPAGATION_SLOT_RANGE,
            )
            validate_attestation_signature(
                state,
                attestation,
                CommitteeConfig(state_machine.config),
            )
        except InvalidGossipMessage as error:
            logger.debug("%s", str(error))
            return False
        else:
            return True

    return committee_index_beacon_attestation_validator


def get_beacon_aggregate_and_proof_validator(chain: BaseBeaconChain) -> Callable[..., bool]:
    def beacon_aggregate_and_proof_validator(msg_forwarder: ID, msg: rpc_pb2.Message) -> bool:
        try:
            aggregate_and_proof = ssz.decode(msg.data, sedes=AggregateAndProof)
        except (TypeError, ssz.DeserializationError) as error:
            # Not correctly encoded
            logger.debug(
                bold_red("Failed to deserialize AggregateAndProof from %s, error=%s"),
                encode_hex(msg.data),
                str(error),
            )
            return False

        state = chain.get_head_state()
        state_machine = chain.get_state_machine()
        attestation = aggregate_and_proof.aggregate

        try:
            validate_voting_beacon_block(chain, attestation)
            run_validate_aggregate_and_proof(
                state,
                aggregate_and_proof,
                CommitteeConfig(state_machine.config),
            )
        except InvalidGossipMessage as error:
            logger.debug("%s", str(error))
            return False

        return True
    return beacon_aggregate_and_proof_validator


#
# Validation
#


def run_validate_block_proposer_signature(
    state: BeaconState, state_machine: BaseBeaconStateMachine, block: BaseSignedBeaconBlock
) -> None:
    # Fast forward to state in future slot in order to pass
    # block.slot validity check
    try:
        future_state = state_machine.state_transition.apply_state_transition(
            state,
            future_slot=block.slot,
        )
    except ValidationError as error:
        raise InvalidGossipMessage(
            f"Failed to fast forward to state at slot={block.slot}",
            error,
        )

    try:
        validate_proposer_signature(future_state, block, CommitteeConfig(state_machine.config))
    except ValidationError as error:
        raise InvalidGossipMessage(
            f"Failed to validate block={encode_hex(block.message.hash_tree_root)}",
            error,
        )


def validate_subnet_id(attestation: Attestation, subnet_id: SubnetId) -> None:
    if attestation.data.index % ATTESTATION_SUBNET_COUNT != subnet_id:
        raise InvalidGossipMessage(
            f"Wrong attestation subnet_id={attestation.data.index % ATTESTATION_SUBNET_COUNT},"
            f" topic subnet_id={subnet_id}. Attestation: {attestation}"
        )


def validate_is_unaggregated(attestation: Attestation) -> None:
    # Check if the attestation is unaggregated
    if len([bit for bit in attestation.aggregation_bits if bit is True]) != 1:
        raise InvalidGossipMessage(
            f"The attestation is aggregated. Attestation: {attestation}"
        )
        return False


def validate_voting_beacon_block(chain: BaseBeaconChain, attestation: Attestation) -> None:
    # Check that beacon blocks attested to by the attestation are validated
    try:
        chain.get_block_by_root(attestation.data.beacon_block_root)
    except BlockNotFound:
        raise InvalidGossipMessage(
            f"Failed to validate attestation={attestation},"
            f" attested block={encode_hex(attestation.data.beacon_block_root)}"
            " has not been not validated yet"
        )


def run_validate_aggregate_and_proof(
    state: BeaconState,
    aggregate_and_proof: AggregateAndProof,
    config: CommitteeConfig
) -> None:
    try:
        validate_aggregate_and_proof(
            state,
            aggregate_and_proof,
            ATTESTATION_PROPAGATION_SLOT_RANGE,
            config,
        )
    except ValidationError as error:
        raise InvalidGossipMessage(
            f"Failed to validate aggregate_and_proof={aggregate_and_proof}",
            error,
        )
