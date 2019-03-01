import ssz

from eth2.beacon.types.blocks import (
    BeaconBlockBody,
)
from eth2.beacon.types.eth1_data import Eth1Data

# Body
from eth2.beacon.types.proposer_slashings import ProposerSlashing
from eth2.beacon.types.attester_slashings import AttesterSlashing
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.deposits import Deposit
from eth2.beacon.types.voluntary_exits import VoluntaryExit


from eth2.beacon.state_machines.forks.serenity.blocks import SerenityBeaconBlock


def test_serialize(
        config,
        sample_proposer_slashing_params,
        sample_attester_slashing_params,
        sample_attestation_params,
        sample_deposit_params,
        sample_voluntary_exit_params):
    count = 10
    for i in range(count):
        test_block_only_header = SerenityBeaconBlock(
            # Header
            slot=i,
            parent_root=i.to_bytes(32, 'little'),
            state_root=i.to_bytes(32, 'little'),
            randao_reveal=i.to_bytes(96, 'little'),
            eth1_data=Eth1Data.create_empty_data(),
            signature=i.to_bytes(96, 'little'),
            # Body
            body=BeaconBlockBody.create_empty_body()
        )
        test_block = test_block_only_header.copy(
            body=test_block_only_header.body.copy(
                proposer_slashings=(
                    (ProposerSlashing(**sample_proposer_slashing_params),) *
                    config.MAX_PROPOSER_SLASHINGS
                ),
                attester_slashings=(
                    (AttesterSlashing(**sample_attester_slashing_params),) *
                    config.MAX_ATTESTER_SLASHINGS
                ),
                attestations=(Attestation(**sample_attestation_params), ) * config.MAX_ATTESTATIONS,
                deposits=(Deposit(**sample_deposit_params), ) * config.MAX_DEPOSITS,
                voluntary_exits=(
                    (VoluntaryExit(**sample_voluntary_exit_params), ) *
                    config.MAX_VOLUNTARY_EXITS
                ),
            )
        )
        ssz.encode(test_block_only_header)
        ssz.encode(test_block)

    for i in range(count):
        test_attestation = Attestation(**sample_attestation_params)
        ssz.encode(test_attestation)
