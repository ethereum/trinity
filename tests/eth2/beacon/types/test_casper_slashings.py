from eth2.beacon.types.attester_slashings import AttesterSlashing


def test_defaults(sample_casper_slashing_params):
    slashing = AttesterSlashing(**sample_casper_slashing_params)

    assert (slashing.slashable_attestation_1
            .custody_bit_0_indices ==
            sample_casper_slashing_params['slashable_attestation_1']
            .custody_bit_0_indices
            )
    assert (slashing.slashable_attestation_2
            .custody_bit_1_indices ==
            sample_casper_slashing_params['slashable_attestation_2']
            .custody_bit_1_indices
            )
