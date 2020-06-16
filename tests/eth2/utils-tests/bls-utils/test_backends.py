from py_ecc.optimized_bls12_381 import curve_order
import pytest

from eth2._utils.bls import bls
from eth2._utils.bls.backends.milagro import MilagroBackend
from eth2._utils.bls.backends.noop import NoOpBackend
from eth2._utils.bls.backends.py_ecc import PyECCBackend

BACKENDS = (NoOpBackend, MilagroBackend, PyECCBackend)


def assert_pubkey(obj):
    assert isinstance(obj, bytes) and len(obj) == 48


def assert_signature(obj):
    assert isinstance(obj, bytes) and len(obj) == 96


@pytest.fixture
def domain():
    return (123).to_bytes(8, "big")


@pytest.mark.parametrize("backend", BACKENDS)
def test_sanity(backend):
    bls.use(backend)
    msg_0 = b"\x32" * 32

    # Test: Verify the basic sign/verify process
    privkey_0 = 5566
    sig_0 = bls.sign(privkey_0, msg_0)
    assert_signature(sig_0)
    pubkey_0 = bls.sk_to_pk(privkey_0)
    assert_pubkey(pubkey_0)
    assert bls.verify(msg_0, sig_0, pubkey_0)

    privkey_1 = 5567
    sig_1 = bls.sign(privkey_1, msg_0)
    pubkey_1 = bls.sk_to_pk(privkey_1)
    assert bls.verify(msg_0, sig_1, pubkey_1)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "privkey",
    [
        (1),
        (5),
        (124),
        (735),
        (127409812145),
        (90768492698215092512159),
        (curve_order - 1),
    ],
)
def test_bls_core_succeed(backend, privkey):
    bls.use(backend)
    msg = str(privkey).encode("utf-8")
    sig = bls.sign(privkey, msg)
    pub = bls.sk_to_pk(privkey)
    assert bls.verify(msg, sig, pub)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("privkey", [(0), (curve_order), (curve_order + 1)])
def test_invalid_private_key(backend, privkey, domain):
    bls.use(backend)
    msg = str(privkey).encode("utf-8")
    with pytest.raises(ValueError):
        bls.sk_to_pk(privkey)
    with pytest.raises(ValueError):
        bls.sign(privkey, msg)
