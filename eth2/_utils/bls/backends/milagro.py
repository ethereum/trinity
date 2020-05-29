from typing import Iterator, Sequence, Tuple

from eth_typing import BLSPubkey, BLSSignature, Hash32
from eth_utils import to_tuple
from milagro_bls_binding import (
    _AggregatePKs,
    Aggregate,
    FastAggregateVerify,
    SkToPk,
    Sign,
    Verify,
)
from py_ecc.bls.typing import Domain

from eth2._utils.bls.backends.base import BaseBLSBackend
from eth2.beacon.constants import EMPTY_PUBKEY, EMPTY_SIGNATURE


def to_int(domain: Domain) -> int:
    """
    Convert Domain to big endian int since
    sigp/milagro_bls use big endian int on hash to g2.
    """
    return int.from_bytes(domain, "big")


@to_tuple
def filter_non_empty_pair(
    pubkeys: Sequence[BLSPubkey], message_hashes: Sequence[Hash32]
) -> Iterator[Tuple[BLSPubkey, Hash32]]:
    for i, pubkey in enumerate(pubkeys):
        if pubkey != EMPTY_PUBKEY:
            yield pubkey, message_hashes[i]


class MilagroBackend(BaseBLSBackend):
    @staticmethod
    def SkToPk(SK: int) -> BLSPubkey:
        return SkToPk(SK.to_bytes(48, "big"))

    @staticmethod
    def Sign(SK: int, message: Hash32) -> BLSSignature:
        return Sign(SK.to_bytes(48, "big"), message)

    @staticmethod
    def Verify(PK: BLSPubkey, message: Hash32, signature: BLSSignature) -> bool:
        if PK == EMPTY_PUBKEY:
            raise ValueError(
                f"Empty public key breaks Milagro binding  pubkey={PK.hex()}"
            )
        return Verify(PK, message, signature)

    @staticmethod
    def Aggregate(signatures: Sequence[BLSSignature]) -> BLSSignature:
        non_empty_signatures = tuple(
            sig for sig in signatures if sig != EMPTY_SIGNATURE
        )
        if len(non_empty_signatures) == 0:
            return EMPTY_SIGNATURE
        return Aggregate(list(non_empty_signatures))

    @staticmethod
    def FastAggregateVerify(
        PKs: Sequence[BLSPubkey],
        message: Hash32,
        signature: BLSSignature,
    ) -> bool:
        if signature == EMPTY_SIGNATURE:
            raise ValueError(
                f"Empty signature breaks Milagro binding  signature={signature.hex()}"
            )

        return FastAggregateVerify(
            PKs,
            message,
            signature
        )
