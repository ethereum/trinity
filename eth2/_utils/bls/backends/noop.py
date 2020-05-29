from typing import Sequence

from eth_typing import BLSPubkey, BLSSignature, Hash32
from py_ecc.bls.typing import Domain

from eth2.beacon.constants import EMPTY_PUBKEY, EMPTY_SIGNATURE

from .base import BaseBLSBackend


class NoOpBackend(BaseBLSBackend):
    @staticmethod
    def SkToPk(k: int) -> BLSPubkey:
        return BLSPubkey(k.to_bytes(48, "little"))

    @staticmethod
    def Sign(**kwargs) -> BLSSignature:
        return EMPTY_SIGNATURE

    @staticmethod
    def Verify(**kwargs) -> bool:
        return True

    @staticmethod
    def Aggregate(**kwargs) -> BLSSignature:
        return EMPTY_SIGNATURE

    @staticmethod
    def FastAggregateVerify(**kwargs) -> bool:
        return True
