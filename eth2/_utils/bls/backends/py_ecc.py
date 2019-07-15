from typing import (
    Sequence,
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


class PyECCBackend(BaseBLSBackend):
    @staticmethod
    def privtopub(k: int) -> BLSPubkey:
        return privtopub(k)

    @staticmethod
    def sign(message_hash: Hash32,
             privkey: int,
             domain: int) -> BLSSignature:
        domain_bytes = Domain(domain.to_bytes(8, 'little'))
        return sign(message_hash, privkey, domain_bytes)

    @staticmethod
    def verify(message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: int) -> bool:
        domain_bytes = Domain(domain.to_bytes(8, 'little'))
        return verify(message_hash, pubkey, signature, domain_bytes)

    @staticmethod
    def aggregate_signatures(signatures: Sequence[BLSSignature]) -> BLSSignature:
        return aggregate_signatures(signatures)

    @staticmethod
    def aggregate_pubkeys(pubkeys: Sequence[BLSPubkey]) -> BLSPubkey:
        return aggregate_pubkeys(pubkeys)

    @staticmethod
    def verify_multiple(pubkeys: Sequence[BLSPubkey],
                        message_hashes: Sequence[Hash32],
                        signature: BLSSignature,
                        domain: int) -> bool:
        domain_bytes = Domain(domain.to_bytes(8, 'little'))
        return verify_multiple(pubkeys, message_hashes, signature, domain_bytes)
