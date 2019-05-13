from eth_hash.auto import keccak

from eth2._utils.hash import hash_eth2


def test_hash():
    output = hash_eth2(b'helloworld')
    assert len(output) == 32


def test_hash_is_keccak256():
    assert hash_eth2(b'foo') == keccak(b'foo')
