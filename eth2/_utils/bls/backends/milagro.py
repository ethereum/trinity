from milagro_bls_binding import (
    privtopub,
    sign,
    verify,
    aggregate_signatures,
    aggregate_pubkeys,
    verify_multiple,
)


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

from py_ecc.bls.typing import Domain

from eth2.beacon.constants import (
    EMPTY_PUBKEY,
    EMPTY_SIGNATURE,
)


def to_little(domain: Domain) -> int:
    return int.from_bytes(domain, 'little')


class MilagroBackend(BaseBLSBackend):
    @staticmethod
    def privtopub(k: int) -> BLSPubkey:
        return privtopub(k.to_bytes(48, 'big'))

    @staticmethod
    def sign(message_hash: Hash32,
             privkey: int,
             domain: Domain) -> BLSSignature:
        return sign(message_hash, privkey.to_bytes(48, 'big'), to_little(domain))

    @staticmethod
    def verify(message_hash: Hash32,
               pubkey: BLSPubkey,
               signature: BLSSignature,
               domain: Domain) -> bool:
        if pubkey == EMPTY_PUBKEY:
            raise ValueError(f"Empty public key breaks Milagro binding  pubkey={pubkey}")
        return verify(message_hash, pubkey, signature, to_little(domain))

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
                        domain: Domain) -> bool:
        if signature == EMPTY_SIGNATURE:
            raise ValueError(f"Empty signature breaks Milagro binding  signature={signature}")
        return verify_multiple(pubkeys, message_hashes, signature, to_little(domain))
