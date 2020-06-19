import pytest

from eth2._utils.bls import bls
from eth2._utils.bls.backends.milagro import MilagroBackend
from eth2._utils.bls.backends.py_ecc import PyECCBackend
from eth2.beacon.exceptions import SignatureError

BACKENDS = (MilagroBackend, PyECCBackend)


@pytest.mark.parametrize("backend", BACKENDS)
def test_validate_empty(backend):
    bls.use(backend)
    msg = b"\x32" * 32
    sig = b"\x42" * 96
    with pytest.raises(SignatureError):
        bls.validate(msg, sig)


@pytest.mark.parametrize("backend", BACKENDS)
def test_validate(backend):
    bls.use(backend)
    msg = b"\x32" * 32
    privkey = 42
    pubkey = bls.sk_to_pk(privkey)
    sig = bls.sign(privkey, msg)
    bls.validate(msg, sig, pubkey)


@pytest.mark.parametrize("backend", BACKENDS)
def test_validate_multiple(backend):
    bls.use(backend)
    msg = b"\x32" * 32
    privkey_0 = 42
    privkey_1 = 4242
    pubkey_0 = bls.sk_to_pk(privkey_0)
    pubkey_1 = bls.sk_to_pk(privkey_1)
    aggregate_sig = bls.aggregate(bls.sign(privkey_0, msg), bls.sign(privkey_1, msg))
    bls.validate(msg, aggregate_sig, pubkey_0, pubkey_1)


@pytest.mark.parametrize("backend", BACKENDS)
def test_validate_invalid(backend):
    bls.use(backend)
    msg = b"\x32" * 32
    privkey = 42
    pubkey = bls.sk_to_pk(privkey)
    sig = b"\x42" * 96
    with pytest.raises(SignatureError):
        bls.validate(msg, sig, pubkey)


@pytest.mark.parametrize("backend", BACKENDS)
def test_validate_multiple_one_invalid(backend):
    bls.use(backend)
    msg = b"\x32" * 32
    privkey_0 = 42
    privkey_1 = 4242
    privkey_other = 424242
    pubkey_0 = bls.sk_to_pk(privkey_0)
    pubkey_1 = bls.sk_to_pk(privkey_1)
    aggregate_sig = bls.aggregate(
        bls.sign(privkey_0, msg), bls.sign(privkey_other, msg)
    )
    with pytest.raises(SignatureError):
        bls.validate(msg, aggregate_sig, pubkey_0, pubkey_1)


@pytest.mark.parametrize("backend", BACKENDS)
def test_validate_invalid_lengths(backend):
    msg = b"\x32" * 32
    with pytest.raises(SignatureError):
        bls.validate(msg, b"\x00", b"\x00" * 48)
    with pytest.raises(SignatureError):
        bls.validate(msg, b"\x00" * 96, b"\x00")
