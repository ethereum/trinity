from typing import Sequence, Tuple

from eth_typing import BLSPubkey, BLSSignature, Hash32

from eth2.beacon.constants import EMPTY_SIGNATURE

from .base import BaseBLSBackend


class NoOpBackend(BaseBLSBackend):
    @staticmethod
    def AggregateVerify(
        pairs: Sequence[Tuple[BLSPubkey, Hash32]], signature: BLSSignature
    ) -> bool:
        return True

    @staticmethod
    def SkToPk(k: int) -> BLSPubkey:
        return BLSPubkey(k.to_bytes(48, "little"))

    @staticmethod
    def Sign(SK: int, message: Hash32) -> BLSSignature:
        return EMPTY_SIGNATURE

    @staticmethod
    def Verify(PK: BLSPubkey, message: Hash32, signature: BLSSignature) -> bool:
        return True

    @staticmethod
    def Aggregate(signatures: Sequence[BLSSignature]) -> BLSSignature:
        return EMPTY_SIGNATURE

    @staticmethod
    def FastAggregateVerify(
        PKs: Sequence[BLSPubkey], message: Hash32, signature: BLSSignature
    ) -> bool:
        return True
