import pytest

from p2p.discv5.endpoint_tracker import (
    EndpointVoteBallotBox,
)
from p2p.tools.factories.discovery import (
    EndpointFactory,
    EndpointVoteFactory,
)


@pytest.fixture
def box():
    return EndpointVoteBallotBox(quorum=8, majority_fraction=0.65, expiry_time=3)


def test_add_vote(box):
    endpoint_vote = EndpointVoteFactory()
    box.add_vote(endpoint_vote)
    assert box.votes_by_sender[endpoint_vote.node_id] == endpoint_vote
    assert box.num_votes_by_endpoint[endpoint_vote.endpoint] == 1

    box.add_vote(endpoint_vote)
    assert box.votes_by_sender[endpoint_vote.node_id] == endpoint_vote
    assert box.num_votes_by_endpoint[endpoint_vote.endpoint] == 1

    another_endpoint_vote = EndpointVoteFactory()
    assert another_endpoint_vote.endpoint != endpoint_vote.endpoint
    box.add_vote(another_endpoint_vote)
    assert box.votes_by_sender[another_endpoint_vote.node_id] == another_endpoint_vote
    assert box.num_votes_by_endpoint[another_endpoint_vote.endpoint] == 1

    vote_for_same_endpoint = EndpointVoteFactory(endpoint=another_endpoint_vote.endpoint)
    box.add_vote(vote_for_same_endpoint)
    assert box.votes_by_sender[vote_for_same_endpoint.node_id] == vote_for_same_endpoint
    assert box.num_votes_by_endpoint[vote_for_same_endpoint.endpoint] == 2


def test_remove_vote(box):
    endpoint_vote = EndpointVoteFactory()
    with pytest.raises(KeyError):
        box.remove_vote_by_node_id(endpoint_vote.node_id)
    box.add_vote(endpoint_vote)
    box.remove_vote_by_node_id(endpoint_vote.node_id)
    assert endpoint_vote.node_id not in box.votes_by_sender
    assert box.num_votes_by_endpoint[endpoint_vote.endpoint] == 0


def test_remove_expired_votes(box):
    v1 = EndpointVoteFactory(timestamp=0)
    v2 = EndpointVoteFactory(timestamp=2)
    v3 = EndpointVoteFactory(timestamp=4)
    box.add_vote(v1)
    box.add_vote(v2)
    box.add_vote(v3)
    box.remove_expired_votes(6)
    assert v1.node_id not in box.votes_by_sender
    assert v2.node_id not in box.votes_by_sender
    assert v3.node_id in box.votes_by_sender


def test_majority():
    box = EndpointVoteBallotBox(quorum=8, majority_fraction=0.65, expiry_time=3)
    e1 = EndpointFactory()
    e2 = EndpointFactory()

    # vote for e1 without reaching quorum
    for _ in range(7):
        box.add_vote(EndpointVoteFactory(endpoint=e1))
        assert box.result is None

    # vote for e1, exceeding quorum, with no votes against
    for _ in range(3):
        box.add_vote(EndpointVoteFactory(endpoint=e1))
        assert box.result == e1

    # vote for e2, without e1 losing required majority
    for _ in range(5):
        box.add_vote(EndpointVoteFactory(endpoint=e2))
        assert box.result == e1

    # vote for e2, reaching simple majority, but not required fraction
    for _ in range(13):
        box.add_vote(EndpointVoteFactory(endpoint=e2))
        assert box.result is None

    box.add_vote(EndpointVoteFactory(endpoint=e2))
    assert box.result == e2
