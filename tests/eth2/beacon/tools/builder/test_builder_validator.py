import pytest
from hypothesis import (
    given,
    settings,
    strategies as st,
)

from eth2._utils import bls
from eth2._utils.bitfield import (
    get_empty_bitfield,
    has_voted,
)
from eth2.beacon.tools.builder.validator import (
    aggregate_votes,
    get_next_epoch_committee_assignments,
    verify_votes,
)


@settings(max_examples=1)
@given(random=st.randoms())
@pytest.mark.parametrize(
    (
        'votes_count'
    ),
    [
        (0),
        (9),
    ],
)
def test_aggregate_votes(votes_count, random, privkeys, pubkeys):
    bit_count = 10
    pre_bitfield = get_empty_bitfield(bit_count)
    pre_sigs = ()
    domain = 0

    random_votes = random.sample(range(bit_count), votes_count)
    message = b'hello'

    # Get votes: (committee_index, sig, public_key)
    votes = [
        (
            committee_index,
            bls.sign(message, privkeys[committee_index], domain),
            pubkeys[committee_index],
        )
        for committee_index in random_votes
    ]

    # Verify
    sigs, committee_indices = verify_votes(message, votes, domain)

    # Aggregate the votes
    bitfield, sigs = aggregate_votes(
        bitfield=pre_bitfield,
        sigs=pre_sigs,
        voting_sigs=sigs,
        voting_committee_indices=committee_indices
    )

    try:
        _, _, pubs = zip(*votes)
    except ValueError:
        pubs = ()

    voted_index = [
        committee_index
        for committee_index in random_votes
        if has_voted(bitfield, committee_index)
    ]
    assert len(voted_index) == len(votes)

    aggregated_pubs = bls.aggregate_pubkeys(pubs)
    assert bls.verify(message, aggregated_pubs, sigs, domain)


@pytest.mark.parametrize(
    (
        'num_validators,'
        'epoch_length,'
        'target_committee_size,'
        'shard_count,'
    ),
    [
        (40, 16, 1, 2),
    ]
)
def test_get_next_epoch_committee_assignments(genesis_state, epoch_length, config, num_validators):
    state = genesis_state
    print('')
    for validator_index in range(num_validators):
        print('#######')
        print(f'validator_index={validator_index}')
        result = get_next_epoch_committee_assignments(state, config, validator_index)

        for epoch in result:
            committee = epoch[0]
            shard = epoch[1]
            slot = epoch[2]
            is_proposer = epoch[3]

            print(
                f"   committee={committee}, shard={shard}, slot={slot}, is_proposer={is_proposer}"
            )
