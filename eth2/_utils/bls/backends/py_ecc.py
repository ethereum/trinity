from typing import (
    Sequence,
    cast,
)

from eth_typing import (
    BLSPubkey,
    BLSSignature,
    Hash32,
)

from eth2._utils.bls.backends.base import (
    BaseBLSBackend,
)
from py_ecc.bls import (
    aggregate_pubkeys,
    aggregate_signatures,
    privtopub,
    sign,
    verify,
    verify_multiple,
)
from py_ecc.bls.typing import (
    Domain,
)

from eth2.beacon.constants import (
    EMPTY_PUBKEY,
    EMPTY_SIGNATURE,
)


def to_bytes(domain: int) -> Domain:
    return cast(Domain, domain.to_bytes(8, 'little'))


class PyECCBackend(BaseBLSBackend):
    @staticmethod
    def privtopub(k: int) -> BLSPubkey:
        return privtopub(k)

    @staticmethod
    def sign(message_hash: Hash32,
             privkey: int,
             domain: int) -> BLSSignature:
        return sign(message_hash, privkey, to_bytes(domain))

    @staticmethod
    def verify(message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: int) -> bool:
        return verify(message_hash, pubkey, signature, to_bytes(domain))

    @staticmethod
    def aggregate_signatures(signatures: Sequence[BLSSignature]) -> BLSSignature:
        # py_ecc use a different EMPTY_SIGNATURE. Return the Trinity one here:
        if len(signatures) == 0:
            return EMPTY_SIGNATURE
        return aggregate_signatures(signatures)

    @staticmethod
    def aggregate_pubkeys(pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        # py_ecc use a different EMPTY_PUBKEY. Return the Trinity one here:
        if len(pubkeys) == 0:
            return EMPTY_PUBKEY
        return aggregate_pubkeys(pubkeys)

    @staticmethod
    def verify_multiple(pubkeys: Sequence[BLSPubkey],
                        message_hashes: Sequence[Hash32],
                        signature: BLSSignature,
                        domain: int) -> bool:
        return verify_multiple(pubkeys, message_hashes, signature, to_bytes(domain))
