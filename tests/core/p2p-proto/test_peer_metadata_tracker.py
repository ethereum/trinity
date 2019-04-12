import pytest

from p2p.tools.factories import NodeFactory

from trinity.plugins.builtin.peer_db.tracker import (
    MemoryEth1PeerTracker,
)


@pytest.fixture
def remote():
    return NodeFactory()


ZERO_HASH = b'\x00' * 32
ZERO_HASH_HEX = ZERO_HASH.hex()

ZERO_ONE_HASH = b'\x01' * 32
ZERO_ONE_HASH_HEX = ZERO_ONE_HASH.hex()


SIMPLE_META = (ZERO_HASH, 'eth', 61, 1)


@pytest.mark.parametrize('is_outbound', (True, False))
def test_track_peer_connection_persists(remote, is_outbound):
    tracker = MemoryEth1PeerTracker()
    assert not tracker._remote_exists(remote.uri())
    tracker.track_peer_connection(remote, is_outbound=is_outbound)
    assert tracker._remote_exists(remote.uri())

    node = tracker._get_remote(remote.uri())
    assert node.uri == remote.uri()
    assert node.is_outbound is is_outbound


@pytest.mark.parametrize('is_outbound', (True, False))
def test_track_peer_connection_does_not_clobber_existing_record(remote, is_outbound):
    tracker = MemoryEth1PeerTracker()
    assert not tracker._remote_exists(remote.uri())
    tracker.track_peer_connection(remote, is_outbound=is_outbound)

    original = tracker._get_remote(remote.uri())
    assert original.is_outbound is is_outbound

    tracker.track_peer_connection(remote, is_outbound=not is_outbound)

    updated = tracker._get_remote(remote.uri())
    assert updated.is_outbound is is_outbound


def test_track_peer_metadata_only_warns_if_remote_missing(remote, caplog):
    tracker = MemoryEth1PeerTracker()

    caplog.clear()
    tracker.update_peer_meta(remote, *SIMPLE_META)
    assert str(remote) in caplog.text


def test_track_peer_metadata(remote, caplog):
    tracker = MemoryEth1PeerTracker()
    tracker.track_peer_connection(remote, True)

    assert tracker._meta_exists(remote.uri()) is False
    tracker.update_peer_meta(remote, *SIMPLE_META)
    assert tracker._meta_exists(remote.uri()) is True

    eth1_meta = tracker._get_meta(remote.uri())
    eth1_meta = tracker._get_meta(remote.uri())
    assert eth1_meta.genesis_hash == ZERO_HASH_HEX
    assert eth1_meta.protocol == 'eth'
    assert eth1_meta.protocol_version == 61
    assert eth1_meta.network_id == 1


def test_track_peer_metadata_updates(remote, caplog):
    tracker = MemoryEth1PeerTracker()
    tracker.track_peer_connection(remote, True)

    assert tracker._meta_exists(remote.uri()) is False
    tracker.update_peer_meta(remote, *SIMPLE_META)
    assert tracker._meta_exists(remote.uri()) is True

    eth1_meta = tracker._get_meta(remote.uri())
    assert eth1_meta.genesis_hash == ZERO_HASH_HEX
    assert eth1_meta.protocol == 'eth'
    assert eth1_meta.protocol_version == 61
    assert eth1_meta.network_id == 1

    tracker.update_peer_meta(remote, ZERO_ONE_HASH, 'les', 60, 2)
    assert tracker._meta_exists(remote.uri()) is True

    updated_meta = tracker._get_meta(remote.uri())
    assert updated_meta.genesis_hash == ZERO_ONE_HASH_HEX
    assert updated_meta.protocol == 'les'
    assert updated_meta.protocol_version == 60
    assert updated_meta.network_id == 2


def do_tracker_peer_query_test(tracker_params, good_remotes, bad_remotes):
    tracker = MemoryEth1PeerTracker(**tracker_params)

    for remote, is_outbound, meta_params in good_remotes:
        tracker.track_peer_connection(remote, is_outbound)
        tracker.update_peer_meta(remote, *meta_params)

    for remote, is_outbound, meta_params in bad_remotes:
        tracker.track_peer_connection(remote, is_outbound)
        tracker.update_peer_meta(remote, *meta_params)

    candidates = tracker._get_peer_candidates(10)
    assert len(candidates) == len(good_remotes)
    for remote, _, _ in good_remotes:
        assert remote in candidates


def test_getting_peer_candidates_no_filter():
    do_tracker_peer_query_test(
        {},
        (
            (NodeFactory(), True, SIMPLE_META),
            (NodeFactory(), True, SIMPLE_META),
            (NodeFactory(), True, SIMPLE_META),
        ),
        (),
    )


def test_getting_peer_candidates_excludes_non_outbound():
    do_tracker_peer_query_test(
        {},
        (
            (NodeFactory(), True, SIMPLE_META),
            (NodeFactory(), True, SIMPLE_META),
        ),
        (
            (NodeFactory(), False, SIMPLE_META),
        ),
    )


def test_getting_peer_candidates_excludes_genesis_hash_mismatch():
    do_tracker_peer_query_test(
        {'genesis_hash': ZERO_HASH},
        (
            (NodeFactory(), True, SIMPLE_META),
            (NodeFactory(), True, SIMPLE_META),
        ),
        (
            (NodeFactory(), True, (ZERO_ONE_HASH, 'eth', 61, 1)),
        ),
    )


def test_getting_peer_candidates_excludes_protocol_mismatch():
    do_tracker_peer_query_test(
        {'protocols': ('eth',)},
        (
            (NodeFactory(), True, SIMPLE_META),
            (NodeFactory(), True, SIMPLE_META),
        ),
        (
            (NodeFactory(), True, (ZERO_HASH, 'les', 61, 1)),
        ),
    )


def test_getting_peer_candidates_matches_multiple_protocols():
    do_tracker_peer_query_test(
        {'protocols': ('eth', 'les')},
        (
            (NodeFactory(), True, (ZERO_HASH, 'eth', 61, 1)),
            (NodeFactory(), True, (ZERO_HASH, 'les', 61, 1)),
        ),
        (
            (NodeFactory(), True, (ZERO_HASH, 'bcc', 61, 1)),
        ),
    )


def test_getting_peer_candidates_excludes_protocol_version_mismatch():
    do_tracker_peer_query_test(
        {'protocol_versions': (60, 61)},
        (
            (NodeFactory(), True, (ZERO_HASH, 'eth', 60, 1)),
            (NodeFactory(), True, (ZERO_HASH, 'eth', 61, 1)),
        ),
        (
            (NodeFactory(), True, (ZERO_HASH, 'eth', 62, 1)),
        ),
    )


def test_getting_peer_candidates_excludes_network_id_mismatch():
    do_tracker_peer_query_test(
        {'network_id': 1},
        (
            (NodeFactory(), True, SIMPLE_META),
            (NodeFactory(), True, SIMPLE_META),
        ),
        (
            (NodeFactory(), True, (ZERO_HASH, 'eth', 62, 2)),
        ),
    )


def test_getting_peer_candidates_complex_query():
    do_tracker_peer_query_test(
        {
            'genesis_hash': ZERO_HASH,
            'protocols': ['eth'],
            'protocol_versions': [61],
            'network_id': 1,
        },
        (
            (NodeFactory(), True, SIMPLE_META),
            (NodeFactory(), True, SIMPLE_META),
        ),
        (
            (NodeFactory(), False, SIMPLE_META),  # inbound peer
            (NodeFactory(), True, (ZERO_HASH, 'les', 61, 1)),  # wrong protocol
            (NodeFactory(), True, (ZERO_HASH, 'eth', 60, 1)),  # wrong protocol version
            (NodeFactory(), True, (ZERO_HASH, 'eth', 61, 2)),  # wrong network_id
        ),
    )
