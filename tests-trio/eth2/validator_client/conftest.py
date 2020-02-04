import pytest

from eth2._utils.bls import Eth2BLS, bls


@pytest.fixture
def sample_bls_private_key():
    return 42


@pytest.fixture
def sample_bls_public_key(sample_bls_private_key):
    return bls.privtopub(sample_bls_private_key)


@pytest.fixture
def sample_bls_key_pair(sample_bls_private_key, sample_bls_public_key):
    return (sample_bls_public_key, sample_bls_private_key)


@pytest.fixture
def seconds_per_slot():
    return 12


@pytest.fixture
def slots_per_epoch():
    return 32


@pytest.fixture
def no_op_bls():
    """
    Disables BLS cryptography across the entire process.
    """
    Eth2BLS.use_noop_backend()
