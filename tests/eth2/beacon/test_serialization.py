import random
from random import randint
import time

import ssz

from eth2._utils.bitfield import (
    get_empty_bitfield,
    set_voted,
)
from eth2.beacon._utils.hash import hash_eth2

from eth2.beacon.types.blocks import (
    BeaconBlockBody,
)
from eth2.beacon.types.eth1_data import Eth1Data

# Body
from eth2.beacon.types.proposer_slashings import ProposerSlashing
from eth2.beacon.types.proposal_signed_data import ProposalSignedData

from eth2.beacon.types.attester_slashings import AttesterSlashing
from eth2.beacon.types.slashable_attestations import SlashableAttestation

from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.attestation_data import AttestationData

from eth2.beacon.types.deposits import Deposit
from eth2.beacon.types.deposit_data import DepositData
from eth2.beacon.types.deposit_input import DepositInput

from eth2.beacon.types.voluntary_exits import VoluntaryExit
from eth2.beacon.types.transfers import Transfer

from eth2.beacon.state_machines.forks.serenity.blocks import SerenityBeaconBlock


SAMPLE_TARGET_COMMITTEE_SIZE = 128
SAMPLE_EMPTY_BITFIELD = get_empty_bitfield(SAMPLE_TARGET_COMMITTEE_SIZE)
SAMPLE_VOTED_COUNT = int(SAMPLE_TARGET_COMMITTEE_SIZE * 0.8)


def get_random_validator_index():
    return randint(0, 2**22)


def get_random_shard():
    return randint(0, 1024)


def get_random_slot():
    return randint(2**32 - 1, 2**32 + 64)


def get_random_epoch():
    return randint(0, 5)


def get_mock_pubkey(prefix, i):
    i_in_bytes = i.to_bytes(32, 'little')
    return (
        hash_eth2(prefix + b'1' + i_in_bytes) +
        hash_eth2(prefix + b'2' + i_in_bytes) +
        hash_eth2(prefix + b'3' + i_in_bytes)
    )[:48]


def get_mock_signature(prefix, i):
    i_in_bytes = i.to_bytes(32, 'little')
    return (
        hash_eth2(prefix + b'1' + i_in_bytes) +
        hash_eth2(prefix + b'2' + i_in_bytes) +
        hash_eth2(prefix + b'3' + i_in_bytes)
    )


def get_sample_bitfield(prefix, i):
    sample_bitfield = SAMPLE_EMPTY_BITFIELD
    seed = int.from_bytes(prefix, 'little') + i
    random.seed(seed)
    voting_committee_indices = tuple(random.sample(
        range(SAMPLE_TARGET_COMMITTEE_SIZE),
        SAMPLE_VOTED_COUNT,
    ))
    for i in voting_committee_indices:
        sample_bitfield = set_voted(sample_bitfield, i)

    return bytes(sample_bitfield), voting_committee_indices


def get_mock_hash(prefix, i):
    i_in_bytes = i.to_bytes(32, 'little')
    return hash_eth2(prefix + i_in_bytes)


def get_mock_attestation_data(prefix, i):
    return AttestationData(
        slot=get_random_slot(),
        shard=get_random_shard(),
        beacon_block_root=get_mock_hash(prefix + b'beacon_block_root', i),
        epoch_boundary_root=get_mock_hash(prefix + b'epoch_boundary_root', i),
        crosslink_data_root=get_mock_hash(prefix + b'crosslink_data_root', i),
        latest_crosslink_root=get_mock_hash(prefix + b'latest_crosslink_root', i),
        justified_epoch=get_random_epoch(),
        justified_block_root=get_mock_hash(prefix + b'justified_block_root', i),
    )


def test_serialize(config):
    count = 2
    for k in range(count):
        test_block_only_header = SerenityBeaconBlock(
            # Header
            slot=k,
            parent_root=get_mock_hash(b'parent_root', k),
            state_root=get_mock_hash(b'state_root', k),
            randao_reveal=get_mock_signature(b'randao_reveal', k),
            eth1_data=Eth1Data(
                deposit_root=get_mock_hash(b'deposit_root', k),
                block_hash=get_mock_hash(b'block_hash', k),
            ),
            signature=get_mock_signature(b'block_signature', k),
            # Body
            body=BeaconBlockBody.create_empty_body()
        )
        serial_number = k * 10 ** 10
        test_block = test_block_only_header.copy(
            body=test_block_only_header.body.copy(
                proposer_slashings=tuple(
                    ProposerSlashing(
                        proposer_index=get_random_validator_index(),
                        proposal_data_1=ProposalSignedData(
                            slot=get_random_slot(),
                            shard=get_random_shard(),
                            block_root=get_mock_hash(b'block_root_1', i),
                        ),
                        proposal_data_2=ProposalSignedData(
                            slot=get_random_slot(),
                            shard=get_random_shard(),
                            block_root=get_mock_hash(b'block_root_2', i),
                        ),
                        proposal_signature_1=get_mock_signature(b'proposal_signature_1', i),
                        proposal_signature_2=get_mock_signature(b'proposal_signature_2', i),
                    )
                    for i in range(serial_number, serial_number + config.MAX_PROPOSER_SLASHINGS)
                ),
                attester_slashings=tuple(
                    AttesterSlashing(
                        slashable_attestation_1=SlashableAttestation(
                            validator_indices=get_sample_bitfield(
                                b'attester_slashings_data_1_validator_indices',
                                i,
                            )[1],
                            data=get_mock_attestation_data(b'attester_slashings_data_1', i),
                            custody_bitfield=get_sample_bitfield(
                                b'attester_slashings_data_1_custody_bitfield',
                                i,
                            )[0],
                            aggregate_signature=get_mock_signature(
                                b'attester_slashings_data_1_aggregate_signature',
                                i,
                            ),
                        ),
                        slashable_attestation_2=SlashableAttestation(
                            validator_indices=get_sample_bitfield(
                                b'attester_slashings_data_2_validator_indices',
                                i,
                            )[1],
                            data=get_mock_attestation_data(b'attester_slashings_data_2', i),
                            custody_bitfield=get_sample_bitfield(
                                b'attester_slashings_data_2_custody_bitfield',
                                i,
                            )[0],
                            aggregate_signature=get_mock_signature(
                                b'attester_slashings_data_2_aggregate_signature',
                                i,
                            ),
                        ),
                    )
                    for i in range(serial_number, serial_number + config.MAX_ATTESTER_SLASHINGS)
                ),
                attestations=tuple(
                    Attestation(
                        data=get_mock_attestation_data(b'attestation_data', i),
                        aggregation_bitfield=get_sample_bitfield(
                            b'attestation_data_aggregation_bitfield',
                            i,
                        )[0],
                        custody_bitfield=get_sample_bitfield(
                            b'attestation_data_custody_bitfield',
                            i,
                        )[0],
                        aggregate_signature=get_mock_signature(
                            b'attestation_aggregate_signature',
                            i,
                        ),
                    )
                    for i in range(serial_number, serial_number + config.MAX_ATTESTATIONS)
                ),
                deposits=tuple(
                    Deposit(
                        branch=tuple(
                            get_mock_hash(b'deposit_branch' + i.to_bytes(32, 'little'), j)
                            for j in range(10)
                        ),
                        index=i,
                        deposit_data=DepositData(
                            deposit_input=DepositInput(
                                pubkey=get_mock_pubkey(b'deposit_pubkey', i),
                                withdrawal_credentials=get_mock_hash(b'withdrawal_credentials', i),
                                proof_of_possession=get_mock_signature(b'proof_of_possession', i),
                            ),
                            amount=config.MAX_DEPOSIT_AMOUNT,
                            timestamp=int(time.time()) + i,
                        ),
                    )
                    for i in range(serial_number, serial_number + config.MAX_DEPOSITS)
                ),
                voluntary_exits=tuple(
                    VoluntaryExit(
                        epoch=get_random_epoch(),
                        validator_index=get_random_validator_index(),
                        signature=get_mock_signature(b'voluntary_exits', i),
                    )
                    for i in range(serial_number, serial_number + config.MAX_VOLUNTARY_EXITS)
                ),
                transfers=tuple(
                    Transfer(
                        from_validator_index=get_random_validator_index(),
                        to_validator_index=get_random_validator_index(),
                        amount=randint(0, 10**10),
                        fee=randint(0, 10**10),
                        slot=get_random_slot(),
                        pubkey=get_mock_pubkey(b'transfer_pubkey', i),
                        signature=get_mock_signature(b'transfer_signature', i),
                    )
                    for i in range(serial_number, serial_number + config.MAX_TRANSFERS)
                ),
            )
        )
        assert len(test_block.body.attestations) == config.MAX_ATTESTATIONS
        print(len(ssz.encode(test_block_only_header)))
        print(len(ssz.encode(test_block)))

    for i in range(count):
        seed = 1
        test_attestation = Attestation(
            data=get_mock_attestation_data(b'attestation_data', seed),
            aggregation_bitfield=get_sample_bitfield(
                b'attestation_data_aggregation_bitfield',
                seed,
            )[0],
            custody_bitfield=get_sample_bitfield(b'attestation_data_custody_bitfield', seed)[0],
            aggregate_signature=get_mock_signature(b'attestation_aggregate_signature', seed),
        )
        print(len(ssz.encode(test_attestation)))
