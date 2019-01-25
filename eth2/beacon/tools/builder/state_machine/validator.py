import random

from eth2._utils import bls
from eth2.beacon.enums import (
    SignatureDomain,
)
from eth2.beacon.helpers import (
    get_domain,
)
from eth2.beacon.types.attestation_data_and_custody_bits import (
    AttestationDataAndCustodyBit,
)


#
# Signer
#
def sign_proof_of_possession(deposit_input,
                             privkey,
                             fork_data,
                             slot):
    domain = get_domain(
        fork_data,
        slot,
        SignatureDomain.DOMAIN_DEPOSIT,
    )
    return bls.sign(
        message=deposit_input.root,
        privkey=privkey,
        domain=domain,
    )


def sign_attestation(message, privkey, fork_data, slot):
    domain = get_domain(
        fork_data,
        slot,
        SignatureDomain.DOMAIN_ATTESTATION,
    )
    return bls.sign(
        message=message,
        privkey=privkey,
        domain=domain,
    )


#
# Only for test/simulation
#
def prepare_attesstation_signing(attestation_data,
                                 committee,
                                 num_voted_attesters):
    message = AttestationDataAndCustodyBit.create_attestation_message(attestation_data)

    committee_size = len(committee)
    assert num_voted_attesters <= committee_size

    voting_committee_indices = tuple(random.sample(range(committee_size), num_voted_attesters))

    return message, voting_committee_indices
