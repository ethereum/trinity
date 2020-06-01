from eth_utils import ValidationError
from py_ecc.optimized_bls12_381 import curve_order
import pytest

from eth2._utils.bls import bls
from eth2._utils.bls.backends.milagro import MilagroBackend
from eth2._utils.bls.backends.noop import NoOpBackend
from eth2._utils.bls.backends.py_ecc import PyECCBackend
from eth2.beacon.constants import EMPTY_PUBKEY, EMPTY_SIGNATURE

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
    sig_0 = bls.Sign(privkey_0, msg_0)
    assert_signature(sig_0)
    pubkey_0 = bls.SkToPk(privkey_0)
    assert_pubkey(pubkey_0)
    assert bls.Verify(pubkey_0, msg_0, sig_0)

    privkey_1 = 5567
    sig_1 = bls.Sign(privkey_1, msg_0)
    pubkey_1 = bls.SkToPk(privkey_1)
    assert bls.Verify(pubkey_1, msg_0, sig_1)

    # Test: Verify signatures are correctly aggregated
    aggregated_signature = bls.Aggregate([sig_0, sig_1])
    assert_signature(aggregated_signature)

    # TODO: Add test for fast aggregate


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
    sig = bls.Sign(privkey, msg)
    pub = bls.SkToPk(privkey)
    assert bls.Verify(pub, msg, sig)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("privkey", [(0), (curve_order), (curve_order + 1)])
def test_invalid_private_key(backend, privkey, domain):
    bls.use(backend)
    msg = str(privkey).encode("utf-8")
    with pytest.raises(ValueError):
        bls.SkToPk(privkey)
    with pytest.raises(ValueError):
        bls.Sign(privkey, msg)


@pytest.mark.parametrize("backend", BACKENDS)
def test_empty_aggregation(backend):
    bls.use(backend)
    assert bls.aggregate_pubkeys([]) == EMPTY_PUBKEY
    assert bls.aggregate_signatures([]) == EMPTY_SIGNATURE


@pytest.mark.parametrize("backend", BACKENDS)
def test_verify_empty_signatures(backend, domain):
    # Want EMPTY_SIGNATURE to fail in Trinity
    bls.use(backend)

    def validate():
        bls.validate(b"\x11" * 32, EMPTY_PUBKEY, EMPTY_SIGNATURE, domain)

    def validate_multiple_1():
        bls.validate_multiple(
            pubkeys=(), message_hashes=(), signature=EMPTY_SIGNATURE, domain=domain
        )

    def validate_multiple_2():
        bls.validate_multiple(
            pubkeys=(EMPTY_PUBKEY, EMPTY_PUBKEY),
            message_hashes=(b"\x11" * 32, b"\x12" * 32),
            signature=EMPTY_SIGNATURE,
            domain=domain,
        )

    if backend == NoOpBackend:
        validate()
        validate_multiple_1()
        validate_multiple_2()
    else:
        with pytest.raises(ValidationError):
            validate()
        with pytest.raises(ValidationError):
            validate_multiple_1()
        with pytest.raises(ValidationError):
            validate_multiple_2()


"""
@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "msg, privkeys",
    [
        (
            b"\x12" * 32,
            [1, 5, 124, 735, 127409812145, 90768492698215092512159, curve_order - 1],
        ),
        (b"\x34" * 32, [42, 666, 1274099945, 4389392949595]),
    ],
)
def test_signature_aggregation(backend, msg, privkeys):
    bls.use(backend)
    sigs = [bls.Sign(msg, k, domain=domain) for k in privkeys]
    pubs = [bls.SkToPk(k) for k in privkeys]
    aggsig = bls.aggregate_signatures(sigs)
    aggpub = bls.aggregate_pubkeys(pubs)
    assert bls.Verify(msg, aggpub, aggsig, domain=domain)
"""


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("msg_1, msg_2", [(b"\x12" * 32, b"\x34" * 32)])
@pytest.mark.parametrize(
    "privkeys_1, privkeys_2",
    [
        (tuple(range(1, 11)), tuple(range(1, 11))),
        ((1, 2, 3), (4, 5, 6, 7)),
        ((1, 2, 3), (2, 3, 4, 5)),
        ((1, 2, 3), ()),
        ((), (2, 3, 4, 5)),
    ],
)
def test_multi_aggregation(backend, msg_1, msg_2, privkeys_1, privkeys_2, domain):
    bls.use(backend)

    sigs_1 = [
        bls.Sign(msg_1, k, domain=domain) for k in privkeys_1
    ]  # signatures to msg_1
    pubs_1 = [bls.SkToPk(k) for k in privkeys_1]
    aggpub_1 = bls.aggregate_pubkeys(pubs_1)  # sig_1 to msg_1

    sigs_2 = [
        bls.Sign(msg_2, k, domain=domain) for k in privkeys_2
    ]  # signatures to msg_2
    pubs_2 = [bls.SkToPk(k) for k in privkeys_2]
    aggpub_2 = bls.aggregate_pubkeys(pubs_2)  # sig_2 to msg_2

    message_hashes = [msg_1, msg_2]
    pubs = [aggpub_1, aggpub_2]
    aggsig = bls.aggregate_signatures(sigs_1 + sigs_2)

    assert bls.verify_multiple(
        pubkeys=pubs, message_hashes=message_hashes, signature=aggsig, domain=domain
    )
