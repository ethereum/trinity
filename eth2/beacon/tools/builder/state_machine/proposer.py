from typing import (
    Sequence,
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
    FromBlockParams,
    SlotNumber,
)


def create_block_on_state_machine(
        sm: BaseBeaconStateMachine,
        parent_block: BaseBeaconBlock,
        slot: SlotNumber,
        validator_index: int,
        privkey: int,
        attestations: Sequence[Attestation]) -> BaseBeaconBlock:
    create_block_on_state(
        sm.state,
        sm.config,
        sm.get_block_class(),
        parent_block,
        slot,
        validator_index,
        privkey,
        attestations,
    )


def create_block_on_state(
        state: BeaconState,
        config: BeaconConfig,
        block_class: BaseBeaconBlock,
        parent_block: BaseBeaconBlock,
        slot: SlotNumber,
        validator_index: int,
        privkey: int,
        attestations: Sequence[Attestation]):

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

    # TODO: STUB, Need to add more operations
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

    # Sign
    empty_signature_block_root = block.block_without_signature_root
    proposal_root = ProposalSignedData(
        slot,
        config.BEACON_CHAIN_SHARD_NUMBER,
        empty_signature_block_root,
    ).root
    domain = get_domain(
        state.fork_data,
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
