from typing import Iterator, Sequence, Tuple

from eth_typing import BLSPubkey, BLSSignature, Hash32
from eth_utils import to_tuple
from milagro_bls_binding import (
    Aggregate,
    AggregateVerify,
    FastAggregateVerify,
    Sign,
    SkToPk,
    Verify,
)

from eth2._utils.bls.backends.base import BaseBLSBackend
from eth2.beacon.constants import EMPTY_PUBKEY, EMPTY_SIGNATURE


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
        return SkToPk(SK.to_bytes(32, "big"))

    @staticmethod
    def Sign(SK: int, message: Hash32) -> BLSSignature:
        return Sign(SK.to_bytes(32, "big"), message)

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
        return Aggregate(list(non_empty_signatures))

    @staticmethod
    def AggregateVerify(
        signature: BLSSignature,
        public_keys: Tuple[BLSPubkey, ...],
        messages: Tuple[Hash32, ...],
    ) -> bool:
        return AggregateVerify(list(public_keys), list(messages), signature)

    @staticmethod
    def FastAggregateVerify(
        PKs: Sequence[BLSPubkey], message: Hash32, signature: BLSSignature
    ) -> bool:
        return FastAggregateVerify(list(PKs), message, signature)
