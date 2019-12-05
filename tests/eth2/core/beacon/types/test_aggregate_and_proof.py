from eth2.beacon.types.aggregate_and_proof import AggregateAndProof


def test_defaults(sample_aggregate_and_proof_params):
    aggregate_and_proof = AggregateAndProof.create(**sample_aggregate_and_proof_params)

    assert aggregate_and_proof.aggregator_index == (
        sample_aggregate_and_proof_params["aggregator_index"]
    )
    assert (
        aggregate_and_proof.aggregate == sample_aggregate_and_proof_params["aggregate"]
    )
    assert (
        aggregate_and_proof.selection_proof
        == sample_aggregate_and_proof_params["selection_proof"]
    )
